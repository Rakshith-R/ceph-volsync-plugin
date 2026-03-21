# Worker Layer Deduplication Design

**Date:** 2026-03-19
**Status:** Draft
**Scope:** `internal/worker/`, `cmd/mover/main.go`

## Problem

The RBD and CephFS worker packages duplicate
identical patterns:

1. **Config structs** — `SourceConfig{DestinationAddress}`
   and `DestinationConfig{ServerPort}` are defined
   identically in both `rbd/` and `cephfs/`
2. **Worker structs** — `SourceWorker` and
   `DestinationWorker` have the same
   `logger` + `config` fields
3. **Constructor pattern** — `NewSourceWorker()` and
   `NewDestinationWorker()` follow the same shape
4. **`Run()` entry point** — both source workers
   connect to destination, do version check, then
   diverge; both destination workers create a
   `DataServiceServer` and call `RunDestinationServer`
5. **Worker factory** — `newCephFSWorker()` and
   `newRBDWorker()` in `cmd/mover/main.go` are
   nearly identical switch blocks

## Approach: Embedded Base Worker Structs

Use Go struct embedding to extract shared
source/destination worker scaffolding into
`internal/worker/common/`. Mover-specific logic stays
in `rbd/` and `cephfs/` packages.

## Design

### 1. Shared Config (common/worker.go)

Move duplicated config and interface definitions to
`internal/worker/common/worker.go`:

```go
package common

import (
    "context"

    "github.com/go-logr/logr"
)

// SourceConfig holds shared configuration for all
// source workers.
type SourceConfig struct {
    DestinationAddress string
}

// DestinationConfig holds shared configuration for
// all destination workers.
type DestinationConfig struct {
    ServerPort string
}

// Worker is implemented by all mover workers.
type Worker interface {
    Run(ctx context.Context) error
}
```

No `ceph_preview` build tag — these are pure Go types.

### 2. Base Source Worker (common/base_source.go)

Handles connect + version check + connection cleanup.
Delegates sync logic to a `Syncer` interface:

```go
package common

import (
    "context"
    "fmt"

    "github.com/go-logr/logr"
    "google.golang.org/grpc"
)

// Syncer is implemented by mover-specific source
// workers to perform their sync operation.
type Syncer interface {
    Sync(ctx context.Context,
        conn *grpc.ClientConn) error
}

// BaseSourceWorker provides shared source worker
// scaffolding: gRPC connection setup, version
// handshake, and connection cleanup.
type BaseSourceWorker struct {
    Logger logr.Logger
    Config SourceConfig
}

// Run connects to the destination with default
// MaxCallSendMsgSize, delegates to syncer.Sync(),
// and cleans up the connection.
// The syncer is responsible for calling SignalDone
// at the appropriate point in its flow.
func (w *BaseSourceWorker) Run(
    ctx context.Context, syncer Syncer,
) (err error) {
    if w.Config.DestinationAddress == "" {
        w.Logger.Info(
            "No destination address provided, " +
                "waiting for context cancellation",
        )
        <-ctx.Done()
        return ctx.Err()
    }

    w.Logger.Info(
        "Connecting to destination",
        "address", w.Config.DestinationAddress,
    )

    conn, err := ConnectToDestination(
        ctx, w.Logger,
        w.Config.DestinationAddress,
        grpc.WithDefaultCallOptions(
            grpc.MaxCallSendMsgSize(
                MaxGRPCMessageSize,
            ),
        ),
    )
    if err != nil {
        return err
    }
    defer func() {
        if cerr := conn.Close(); cerr != nil &&
            err == nil {
            err = fmt.Errorf(
                "close gRPC connection: %w", cerr,
            )
        }
    }()

    return syncer.Sync(ctx, conn)
}
```

Key decisions:
- **SignalDone stays in Sync()**: CephFS calls
  `SignalDone` after snapdiff or rsync fallback;
  RBD calls it after commit. The timing differs,
  so each mover handles it.
- **Empty address guard**: CephFS currently has
  this; by moving it to base, RBD also gets it
  for free.
- **Default gRPC options**: `MaxCallSendMsgSize`
  is applied in the base `Run()` for all source
  workers. Previously only RBD set this; now
  CephFS also gets it, which is correct since
  CephFS snapdiff also streams block diffs via
  gRPC.
- **Error-preserving defer**: Uses the RBD pattern
  (named return + error check) which is safer
  than CephFS's `_ = conn.Close()`.

### 3. Base Destination Worker (common/base_dest.go)

Thin wrapper providing shared config and logger:

```go
package common

import (
    "context"

    "github.com/go-logr/logr"
    "google.golang.org/grpc"

    apiv1 "github.com/RamenDR/ceph-volsync-plugin/internal/proto/api/v1"
)

// BaseDestinationWorker provides shared destination
// worker scaffolding.
type BaseDestinationWorker struct {
    Logger logr.Logger
    Config DestinationConfig
}

// Run starts the gRPC destination server with
// default MaxRecvMsgSize applied.
func (w *BaseDestinationWorker) Run(
    ctx context.Context,
    dataServer apiv1.DataServiceServer,
) error {
    w.Logger.Info("Starting destination worker")

    return RunDestinationServer(
        ctx, w.Logger,
        w.Config.ServerPort, dataServer,
        grpc.MaxRecvMsgSize(MaxGRPCMessageSize),
    )
}
```

### 4. Mover-Specific Workers

#### RBD Source (rbd/source.go)

```go
type SourceWorker struct {
    common.BaseSourceWorker
}

func NewSourceWorker(
    logger logr.Logger, cfg common.SourceConfig,
) *SourceWorker {
    return &SourceWorker{
        BaseSourceWorker: common.BaseSourceWorker{
            Logger: logger.WithName(
                "rbd-source-worker",
            ),
            Config: cfg,
        },
    }
}

func (w *SourceWorker) Run(
    ctx context.Context,
) error {
    return w.BaseSourceWorker.Run(ctx, w)
}

func (w *SourceWorker) Sync(
    ctx context.Context, conn *grpc.ClientConn,
) error {
    // existing RBD sync logic:
    // resolveSourceConfig, resolveParentImage,
    // streamBlocks, commitAndSignalDone
}
```

#### RBD Destination (rbd/destination.go)

```go
type DestinationWorker struct {
    common.BaseDestinationWorker
}

func NewDestinationWorker(
    logger logr.Logger, cfg common.DestinationConfig,
) *DestinationWorker {
    return &DestinationWorker{
        BaseDestinationWorker:
            common.BaseDestinationWorker{
                Logger: logger.WithName(
                    "rbd-destination-worker",
                ),
                Config: cfg,
            },
    }
}

func (w *DestinationWorker) Run(
    ctx context.Context,
) error {
    dataServer := &RBDDataServer{
        logger:     w.Logger,
        devicePath: common.DevicePath,
    }
    return w.BaseDestinationWorker.Run(
        ctx, dataServer,
    )
}
```

#### CephFS Source (cephfs/source.go)

Embed `BaseSourceWorker`, implement `Syncer`. No
`DialOpts` needed (empty slice).

```go
type SourceWorker struct {
    common.BaseSourceWorker
}

func NewSourceWorker(
    logger logr.Logger, cfg common.SourceConfig,
) *SourceWorker {
    return &SourceWorker{
        BaseSourceWorker: common.BaseSourceWorker{
            Logger: logger.WithName(
                "cephfs-source-worker",
            ),
            Config: cfg,
        },
    }
}

func (w *SourceWorker) Run(
    ctx context.Context,
) error {
    return w.BaseSourceWorker.Run(ctx, w)
}

func (w *SourceWorker) Sync(
    ctx context.Context, conn *grpc.ClientConn,
) error {
    // existing CephFS sync logic:
    // branch on snapshot handles present:
    //   yes -> runSnapdiffSync(ctx, conn, ...)
    //   no  -> runRsyncFallback(ctx, conn)
    // both paths call SignalDone internally
}
```

#### CephFS Destination (cephfs/destination.go)

Embed `BaseDestinationWorker`, no `ServerOpts`
needed (empty slice).

```go
type DestinationWorker struct {
    common.BaseDestinationWorker
}

func NewDestinationWorker(
    logger logr.Logger, cfg common.DestinationConfig,
) *DestinationWorker {
    return &DestinationWorker{
        BaseDestinationWorker:
            common.BaseDestinationWorker{
                Logger: logger.WithName(
                    "cephfs-destination-worker",
                ),
                Config: cfg,
            },
    }
}

func (w *DestinationWorker) Run(
    ctx context.Context,
) error {
    dataServer := &DataServer{logger: w.Logger}
    return w.BaseDestinationWorker.Run(
        ctx, dataServer,
    )
}
```

### 5. Factory Registry (cmd/mover/main.go)

Replace duplicated `newCephFSWorker()` /
`newRBDWorker()` with a registry:

```go
type workerFactory func(
    logger logr.Logger,
    workerType string,
    srcCfg common.SourceConfig,
    dstCfg common.DestinationConfig,
) (common.Worker, error)

var factories = map[string]workerFactory{
    "cephfs": newCephFSWorker,
    "rbd":    newRBDWorker,
}

func newWorker(
    logger logr.Logger, config Config,
) (common.Worker, error) {
    factory, ok := factories[config.MoverType]
    if !ok {
        return nil, fmt.Errorf(
            "unsupported MOVER_TYPE '%s'",
            config.MoverType,
        )
    }
    return factory(
        logger,
        config.WorkerType,
        common.SourceConfig{
            DestinationAddress:
                config.DestinationAddress,
        },
        common.DestinationConfig{
            ServerPort: config.ServerPort,
        },
    )
}
```

Each factory remains simple but now shares the
config construction:

```go
func newCephFSWorker(
    logger logr.Logger,
    workerType string,
    srcCfg common.SourceConfig,
    dstCfg common.DestinationConfig,
) (common.Worker, error) {
    switch workerType {
    case workerTypeSource:
        return wcephfs.NewSourceWorker(
            logger, srcCfg,
        ), nil
    case workerTypeDestination:
        return wcephfs.NewDestinationWorker(
            logger, dstCfg,
        ), nil
    default:
        return nil, fmt.Errorf(
            "invalid worker type: %s", workerType,
        )
    }
}
```

The `Runner` interface in `main.go` is removed;
`common.Worker` is used instead.

### 6. Build Tag Changes

Remove `ceph_preview` build tag from all files in
`internal/worker/common/`. Only files that import
`go-ceph` CGO libraries need the tag.

Files affected:
- `common/worker.go` (new, no tag)
- `common/base_source.go` (new, no tag)
- `common/base_dest.go` (new, no tag)
- `common/credentials.go` (keep tag — imports ceph)
- `common/client.go` (confirm no tag present)
- `common/destination.go` (confirm no tag present)
- `common/grpc.go` (confirm no tag present)
- `common/constants.go` (confirm no tag present)
- `common/util.go` (confirm no tag present)

## Files Modified

| File | Change |
|------|--------|
| `common/worker.go` | New: shared configs + Worker |
| `common/base_source.go` | New: BaseSourceWorker |
| `common/base_dest.go` | New: BaseDestinationWorker |
| `rbd/source.go` | Embed base, impl Syncer |
| `rbd/destination.go` | Embed base |
| `cephfs/source.go` | Embed base, impl Syncer |
| `cephfs/destination.go` | Embed base |
| `cmd/mover/main.go` | Factory registry, use Worker |
| `common/*.go` | Confirm no ceph_preview tags |

**Mechanical rename**: All existing method bodies in
`rbd/` and `cephfs/` that reference `w.logger` and
`w.config` must change to `w.Logger` and `w.Config`
(exported fields from embedded base structs).

## Risks and Mitigations

- **Embedding confusion**: Method resolution with
  embedding can be subtle. Mitigated by keeping the
  base structs minimal (2-3 methods max) and having
  the outer `Run()` explicitly delegate.
- **Breaking changes**: All changes are internal
  (no public API). The gRPC wire protocol and
  env var contract are unchanged.
- **CephFS empty-address behavior**: Currently only
  CephFS handles missing destination address. Moving
  this to the base changes RBD behavior slightly
  (it would block instead of failing at connect).
  This is arguably correct — the manager always
  provides the address, so this path is defensive.

## Unit Tests

Add unit tests for the new base worker types in
`internal/worker/common/`:

### base_source_test.go

```go
package common_test

import (
    "context"
    "testing"

    "github.com/go-logr/logr"
    "google.golang.org/grpc"

    "github.com/RamenDR/ceph-volsync-plugin/internal/worker/common"
)

// mockSyncer implements Syncer for testing.
type mockSyncer struct {
    called bool
    err    error
}

func (m *mockSyncer) Sync(
    ctx context.Context, conn *grpc.ClientConn,
) error {
    m.called = true
    return m.err
}

func TestBaseSourceWorker_EmptyAddress(t *testing.T) {
    w := common.BaseSourceWorker{
        Logger: logr.Discard(),
        Config: common.SourceConfig{},
    }
    syncer := &mockSyncer{}

    ctx, cancel := context.WithCancel(
        context.Background(),
    )
    cancel() // cancel immediately

    err := w.Run(ctx, syncer)
    if err == nil {
        t.Fatal("expected context error")
    }
    if syncer.called {
        t.Fatal("Sync should not be called " +
            "with empty address")
    }
}
```

### base_dest_test.go

```go
package common_test

import (
    "context"
    "net"
    "testing"

    "github.com/go-logr/logr"

    "github.com/RamenDR/ceph-volsync-plugin/internal/worker/common"
    apiv1 "github.com/RamenDR/ceph-volsync-plugin/internal/proto/api/v1"
)

// mockDataServer implements DataServiceServer.
type mockDataServer struct {
    apiv1.UnimplementedDataServiceServer
}

func TestBaseDestWorker_ListensOnPort(t *testing.T) {
    // Find a free port
    lis, err := net.Listen("tcp", ":0")
    if err != nil {
        t.Fatal(err)
    }
    port := lis.Addr().(*net.TCPAddr).Port
    lis.Close()

    w := common.BaseDestinationWorker{
        Logger: logr.Discard(),
        Config: common.DestinationConfig{
            ServerPort: fmt.Sprintf("%d", port),
        },
    }

    ctx, cancel := context.WithCancel(
        context.Background(),
    )

    errCh := make(chan error, 1)
    go func() {
        errCh <- w.Run(ctx, &mockDataServer{})
    }()

    // Cancel to trigger shutdown
    cancel()

    if err := <-errCh; err != nil &&
        err != context.Canceled {
        t.Fatalf("unexpected error: %v", err)
    }
}
```

### worker_test.go

```go
package common_test

import (
    "context"
    "testing"

    "github.com/RamenDR/ceph-volsync-plugin/internal/worker/common"
)

// Verify Worker interface is satisfied by a type
// that embeds BaseSourceWorker and implements Run.
type testWorker struct{}

func (w *testWorker) Run(
    ctx context.Context,
) error {
    return nil
}

func TestWorkerInterface(t *testing.T) {
    var _ common.Worker = &testWorker{}
}
```

## Success Criteria

- `make test` passes with new unit tests
- `make docker-build-mover` succeeds
- No duplicated `SourceConfig`/`DestinationConfig`
  structs remain in `rbd/` or `cephfs/`
- `cmd/mover/main.go` has no `Runner` interface
  and uses `common.Worker` instead
