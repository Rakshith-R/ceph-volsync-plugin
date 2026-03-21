# Worker Layer Deduplication Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development (recommended)
> or superpowers:executing-plans to implement this plan
> task-by-task. Steps use checkbox (`- [ ]`) syntax
> for tracking.

**Goal:** Eliminate duplicated config structs,
constructors, `Run()` scaffolding, and factory code
between RBD and CephFS worker packages by extracting
shared base types into `internal/worker/common/`.

**Architecture:** Add `Worker` interface,
`SourceConfig`/`DestinationConfig` structs, and
`BaseSourceWorker`/`BaseDestinationWorker` embedded
structs to `common/`. Each mover embeds the base and
implements a `Syncer` interface for its sync logic.
The entry point `cmd/mover/main.go` switches from a
local `Runner` interface to `common.Worker` with a
factory registry.

**Tech Stack:** Go 1.24, gRPC, controller-runtime
logr, go-ceph (CGO in mover-specific code only)

**Spec:**
`docs/superpowers/specs/2026-03-19-worker-dedup-design.md`

**Validation command:** `make test`

---

## File Structure

| File | Action | Responsibility |
|------|--------|----------------|
| `internal/worker/common/worker.go` | Create | `SourceConfig`, `DestinationConfig`, `Worker` interface |
| `internal/worker/common/base_source.go` | Create | `Syncer` interface, `BaseSourceWorker` with `Run()` |
| `internal/worker/common/base_dest.go` | Create | `BaseDestinationWorker` with `Run()` |
| `internal/worker/common/base_source_test.go` | Create | Unit tests for `BaseSourceWorker` |
| `internal/worker/common/base_dest_test.go` | Create | Unit tests for `BaseDestinationWorker` |
| `internal/worker/common/worker_test.go` | Create | Interface satisfaction compile-time check |
| `internal/worker/rbd/source.go` | Modify | Embed `BaseSourceWorker`, implement `Syncer`, rename `w.logger`→`w.Logger` / `w.config`→`w.Config` |
| `internal/worker/rbd/destination.go` | Modify | Embed `BaseDestinationWorker`, rename fields |
| `internal/worker/cephfs/source.go` | Modify | Embed `BaseSourceWorker`, implement `Syncer`, rename fields (47 occurrences) |
| `internal/worker/cephfs/destination.go` | Modify | Embed `BaseDestinationWorker`, rename fields |
| `cmd/mover/main.go` | Modify | Use `common.Worker`, factory registry, remove `Runner` |

---

### Task 1: Create shared config and Worker interface

**Files:**
- Create: `internal/worker/common/worker.go`
- Test: `internal/worker/common/worker_test.go`

- [ ] **Step 1: Create `worker.go` with shared types**

```go
// internal/worker/common/worker.go
package common

import "context"

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

- [ ] **Step 2: Create `worker_test.go`**

```go
// internal/worker/common/worker_test.go
package common_test

import (
	"context"
	"testing"

	"github.com/RamenDR/ceph-volsync-plugin/internal/worker/common"
)

type testWorker struct{}

func (w *testWorker) Run(
	_ context.Context,
) error {
	return nil
}

// Compile-time check: testWorker satisfies Worker.
func TestWorkerInterface(t *testing.T) {
	var _ common.Worker = &testWorker{}
}
```

- [ ] **Step 3: Verify it compiles**

Run: `cd internal/worker/common && go build ./...`
Expected: no errors

- [ ] **Step 4: Run the test**

Run: `cd internal/worker/common && go test -run TestWorkerInterface -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/worker/common/worker.go \
  internal/worker/common/worker_test.go
git commit -sm"refactor: add shared Worker interface and config types"
```

---

### Task 2: Create BaseSourceWorker

**Files:**
- Create: `internal/worker/common/base_source.go`
- Create: `internal/worker/common/base_source_test.go`

- [ ] **Step 1: Write `base_source_test.go`**

```go
// internal/worker/common/base_source_test.go
package common_test

import (
	"context"
	"testing"

	"github.com/go-logr/logr"
	"google.golang.org/grpc"

	"github.com/RamenDR/ceph-volsync-plugin/internal/worker/common"
)

type mockSyncer struct {
	called bool
	err    error
}

func (m *mockSyncer) Sync(
	_ context.Context, _ *grpc.ClientConn,
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
		t.Fatal(
			"Sync should not be called " +
				"with empty address",
		)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd internal/worker/common && go test -run TestBaseSourceWorker_EmptyAddress -v`
Expected: FAIL — `BaseSourceWorker` not defined

- [ ] **Step 3: Create `base_source.go`**

```go
// internal/worker/common/base_source.go
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
				"waiting for context " +
				"cancellation",
		)
		<-ctx.Done()

		return ctx.Err()
	}

	w.Logger.Info(
		"Connecting to destination",
		"address",
		w.Config.DestinationAddress,
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
				"close gRPC connection: %w",
				cerr,
			)
		}
	}()

	return syncer.Sync(ctx, conn)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd internal/worker/common && go test -run TestBaseSourceWorker_EmptyAddress -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/worker/common/base_source.go \
  internal/worker/common/base_source_test.go
git commit -sm"refactor: add BaseSourceWorker with Syncer interface"
```

---

### Task 3: Create BaseDestinationWorker

**Files:**
- Create: `internal/worker/common/base_dest.go`
- Create: `internal/worker/common/base_dest_test.go`

- [ ] **Step 1: Write `base_dest_test.go`**

```go
// internal/worker/common/base_dest_test.go
package common_test

import (
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/go-logr/logr"

	apiv1 "github.com/RamenDR/ceph-volsync-plugin/internal/proto/api/v1"
	"github.com/RamenDR/ceph-volsync-plugin/internal/worker/common"
)

// Note: "fmt" is required for fmt.Sprintf below.

type mockDataServer struct {
	apiv1.UnimplementedDataServiceServer
}

func TestBaseDestWorker_ListensOnPort(
	t *testing.T,
) {
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

	cancel()

	if err := <-errCh; err != nil &&
		err != context.Canceled {
		t.Fatalf("unexpected error: %v", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd internal/worker/common && go test -run TestBaseDestWorker_ListensOnPort -v`
Expected: FAIL — `BaseDestinationWorker` not defined

- [ ] **Step 3: Create `base_dest.go`**

```go
// internal/worker/common/base_dest.go
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

- [ ] **Step 4: Run test to verify it passes**

Run: `cd internal/worker/common && go test -run TestBaseDestWorker_ListensOnPort -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/worker/common/base_dest.go \
  internal/worker/common/base_dest_test.go
git commit -sm"refactor: add BaseDestinationWorker"
```

---

### Task 4: Refactor RBD source worker

**Files:**
- Modify: `internal/worker/rbd/source.go`

This task removes `SourceConfig`, embeds
`common.BaseSourceWorker`, implements `Syncer`,
and renames `w.logger`→`w.Logger` /
`w.config`→`w.Config` (8 occurrences).

- [ ] **Step 1: Delete `SourceConfig` and old `SourceWorker` struct**

In `internal/worker/rbd/source.go`, replace
lines 35-56:

Old:
```go
type SourceConfig struct {
	DestinationAddress string
}

type SourceWorker struct {
	logger logr.Logger
	config SourceConfig
}

func NewSourceWorker(
	logger logr.Logger, cfg SourceConfig,
) *SourceWorker {
	return &SourceWorker{
		logger: logger.WithName("rbd-source-worker"),
		config: cfg,
	}
}
```

New:
```go
// SourceWorker represents an RBD source worker
// instance.
type SourceWorker struct {
	common.BaseSourceWorker
}

// NewSourceWorker creates a new RBD source worker.
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
```

- [ ] **Step 2: Replace `Run()` with delegation + `Sync()`**

Replace the existing `Run()` method (lines 77-172)
with:

```go
// Run starts the RBD source worker.
func (w *SourceWorker) Run(
	ctx context.Context,
) error {
	return w.BaseSourceWorker.Run(ctx, w)
}

// Sync implements common.Syncer. It performs the RBD
// block diff sync operation.
//
//nolint:funlen // sequential orchestration steps
func (w *SourceWorker) Sync(
	ctx context.Context,
	conn *grpc.ClientConn,
) (err error) {
	w.Logger.Info("Starting RBD source sync")

	sc, cc, err := w.resolveSourceConfig()
	if err != nil {
		return err
	}
	defer cc.Destroy()

	if err := w.resolveParentImage(cc, sc); err != nil {
		return err
	}

	parentSpec := ceph.RBDImageSpec(
		sc.parentPoolName, sc.parentNS,
		sc.parentImageName,
	)
	parentImage, err := ceph.NewImage(cc, parentSpec)
	if err != nil {
		return fmt.Errorf(
			"failed to open parent image %s: %w",
			parentSpec, err,
		)
	}

	volSize, err := parentImage.GetSize()
	if err != nil {
		_ = parentImage.Close()
		return fmt.Errorf(
			"failed to get volume size: %w", err,
		)
	}
	_ = parentImage.Close()

	iter, err := ceph.NewRBDBlockDiffIterator(
		sc.mons, sc.user, sc.key,
		sc.parentPoolID, sc.parentNS,
		sc.parentImageName,
		sc.fromSnapID, sc.targetSnapID, volSize,
	)
	if err != nil {
		return fmt.Errorf(
			"failed to create block diff "+
				"iterator: %w",
			err,
		)
	}
	defer func() { _ = iter.Close() }()

	device, err := os.Open(common.DevicePath)
	if err != nil {
		return fmt.Errorf(
			"failed to open %s: %w",
			common.DevicePath, err,
		)
	}
	defer func() { _ = device.Close() }()

	dataClient := apiv1.NewDataServiceClient(conn)
	stream, err := dataClient.Sync(ctx)
	if err != nil {
		return fmt.Errorf(
			"failed to create sync stream: %w",
			err,
		)
	}

	if err := w.streamBlocks(
		iter, device, stream,
	); err != nil {
		return err
	}

	return w.commitAndSignalDone(
		ctx, conn, stream,
	)
}
```

- [ ] **Step 3: Rename `w.logger` to `w.Logger` in remaining methods**

Use replace-all in `internal/worker/rbd/source.go`:
- `w.logger` → `w.Logger` (all occurrences)
- `w.config` → `w.Config` (all occurrences)

These affect: `resolveFullDiffFromVolume`,
`resolveSnapshotDiff`, `commitAndSignalDone`.

- [ ] **Step 4: Update imports**

Keep `"github.com/go-logr/logr"` — the constructor
takes `logr.Logger` as a parameter. Keep
`"google.golang.org/grpc"` — used in `Sync`
signature and `commitAndSignalDone`.

Remove the old `SourceConfig` type reference; the
import of `common` already exists.

- [ ] **Step 5: Verify it compiles**

Run: `cd internal/worker/rbd && go build -tags ceph_preview ./...`
Expected: no errors

- [ ] **Step 6: Commit**

```bash
git add internal/worker/rbd/source.go
git commit -sm"refactor: RBD source embeds BaseSourceWorker"
```

---

### Task 5: Refactor RBD destination worker

**Files:**
- Modify: `internal/worker/rbd/destination.go`

- [ ] **Step 1: Replace config, struct, constructor, and Run()**

In `internal/worker/rbd/destination.go`, replace
lines 32-73:

Old:
```go
type DestinationConfig struct {
	ServerPort string
}

type DestinationWorker struct {
	logger logr.Logger
	config DestinationConfig
}

func NewDestinationWorker(
	logger logr.Logger, config DestinationConfig,
) *DestinationWorker {
	return &DestinationWorker{
		logger: logger.WithName(
			"rbd-destination-worker",
		),
		config: config,
	}
}

func (w *DestinationWorker) Run(
	ctx context.Context,
) error {
	w.logger.Info("Starting RBD destination worker")

	dataServer := &RBDDataServer{
		logger:     w.logger,
		devicePath: common.DevicePath,
	}

	return common.RunDestinationServer(
		ctx, w.logger, w.config.ServerPort,
		dataServer,
		grpc.MaxRecvMsgSize(
			common.MaxGRPCMessageSize,
		),
	)
}
```

New:
```go
// DestinationWorker represents an RBD destination
// worker instance.
type DestinationWorker struct {
	common.BaseDestinationWorker
}

// NewDestinationWorker creates a new RBD destination
// worker.
func NewDestinationWorker(
	logger logr.Logger,
	config common.DestinationConfig,
) *DestinationWorker {
	return &DestinationWorker{
		BaseDestinationWorker:
			common.BaseDestinationWorker{
				Logger: logger.WithName(
					"rbd-destination-worker",
				),
				Config: config,
			},
	}
}

// Run starts the RBD destination worker.
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

- [ ] **Step 2: Update imports**

Keep `"github.com/go-logr/logr"` — the constructor
takes `logr.Logger`. Keep `"google.golang.org/grpc"`
— `RBDDataServer.Sync()` uses
`grpc.ClientStreamingServer` in its signature.
Remove the old `DestinationConfig` type; `common`
import already exists.

- [ ] **Step 3: Verify it compiles**

Run: `cd internal/worker/rbd && go build -tags ceph_preview ./...`
Expected: no errors

- [ ] **Step 4: Commit**

```bash
git add internal/worker/rbd/destination.go
git commit -sm"refactor: RBD destination embeds BaseDestinationWorker"
```

---

### Task 6: Refactor CephFS source worker

**Files:**
- Modify: `internal/worker/cephfs/source.go`

This has 47 occurrences of `w.logger`/`w.config`.

- [ ] **Step 1: Replace config, struct, constructor**

In `internal/worker/cephfs/source.go`, replace
lines 80-101:

Old:
```go
type SourceConfig struct {
	DestinationAddress string
}

type SourceWorker struct {
	logger logr.Logger
	config SourceConfig
}

func NewSourceWorker(
	logger logr.Logger, cfg SourceConfig,
) *SourceWorker {
	return &SourceWorker{
		logger: logger.WithName("source-worker"),
		config: cfg,
	}
}
```

New:
```go
// SourceWorker represents a CephFS source worker
// instance.
type SourceWorker struct {
	common.BaseSourceWorker
}

// NewSourceWorker creates a new CephFS source
// worker.
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
```

- [ ] **Step 2: Replace `Run()` with delegation + `Sync()`**

Replace the existing `Run()` method (lines 103-155)
with:

```go
// Run starts the CephFS source worker.
func (w *SourceWorker) Run(
	ctx context.Context,
) error {
	return w.BaseSourceWorker.Run(ctx, w)
}

// Sync implements common.Syncer. It performs CephFS
// snapdiff sync or falls back to rsync.
func (w *SourceWorker) Sync(
	ctx context.Context,
	conn *grpc.ClientConn,
) error {
	baseSnapshotHandle := os.Getenv(
		worker.EnvBaseSnapshotHandle,
	)
	targetSnapshotHandle := os.Getenv(
		worker.EnvTargetSnapshotHandle,
	)
	volumeHandle := os.Getenv(
		worker.EnvVolumeHandle,
	)

	if baseSnapshotHandle == "" ||
		targetSnapshotHandle == "" ||
		volumeHandle == "" {
		w.Logger.Info(
			"Snapshot handles not set, " +
				"using rsync on /data",
		)
		return w.runRsyncFallback(ctx, conn)
	}

	return w.runSnapdiffSync(
		ctx, conn,
		baseSnapshotHandle,
		targetSnapshotHandle,
		volumeHandle,
	)
}
```

- [ ] **Step 3: Rename all `w.logger` → `w.Logger` and `w.config` → `w.Config`**

Use replace-all across `internal/worker/cephfs/source.go`:
- `w.logger` → `w.Logger` (all occurrences)
- `w.config` → `w.Config` (all occurrences)

There are ~47 occurrences total. This affects every
method in the file: `runRsyncFallback`,
`runSnapdiffSync`, `runStatelessSync`,
`walkAndStreamDirectories`,
`processDirectorySnapdiff`, `processFile`,
`streamBlockDiff`, `rsync`,
`createFileListAndSync`, `syncForDeletion`,
`rsyncConvergence`.

- [ ] **Step 4: Update imports**

Keep `"github.com/go-logr/logr"` — the constructor
takes `logr.Logger` and `syncState` struct has a
`logr.Logger` field. Keep `"google.golang.org/grpc"`
— used in `Sync` signature and
`runSnapdiffSync`/`runRsyncFallback`.

- [ ] **Step 5: Verify it compiles**

Run: `cd internal/worker/cephfs && go build -tags ceph_preview ./...`
Expected: no errors

- [ ] **Step 6: Commit**

```bash
git add internal/worker/cephfs/source.go
git commit -sm"refactor: CephFS source embeds BaseSourceWorker"
```

---

### Task 7: Refactor CephFS destination worker

**Files:**
- Modify: `internal/worker/cephfs/destination.go`

- [ ] **Step 1: Replace config, struct, constructor, and Run()**

In `internal/worker/cephfs/destination.go`, replace
lines 35-70:

Old:
```go
type DestinationConfig struct {
	ServerPort string
}

type DestinationWorker struct {
	logger logr.Logger
	config DestinationConfig
}

func NewDestinationWorker(
	logger logr.Logger, config DestinationConfig,
) *DestinationWorker {
	return &DestinationWorker{
		logger: logger.WithName(
			"destination-worker",
		),
		config: config,
	}
}

func (w *DestinationWorker) Run(
	ctx context.Context,
) error {
	w.logger.Info("Starting destination worker")

	dataServer := &DataServer{logger: w.logger}

	return common.RunDestinationServer(
		ctx, w.logger, w.config.ServerPort,
		dataServer,
	)
}
```

New:
```go
// DestinationWorker represents a CephFS destination
// worker instance.
type DestinationWorker struct {
	common.BaseDestinationWorker
}

// NewDestinationWorker creates a new CephFS
// destination worker.
func NewDestinationWorker(
	logger logr.Logger,
	config common.DestinationConfig,
) *DestinationWorker {
	return &DestinationWorker{
		BaseDestinationWorker:
			common.BaseDestinationWorker{
				Logger: logger.WithName(
					"cephfs-destination-worker",
				),
				Config: config,
			},
	}
}

// Run starts the CephFS destination worker.
func (w *DestinationWorker) Run(
	ctx context.Context,
) error {
	dataServer := &DataServer{
		logger: w.Logger,
	}

	return w.BaseDestinationWorker.Run(
		ctx, dataServer,
	)
}
```

- [ ] **Step 2: Update imports**

Keep `"github.com/go-logr/logr"` — the constructor
takes `logr.Logger` and `DataServer` struct has a
`logr.Logger` field. Remove
`"google.golang.org/grpc"` — no longer directly used
(was only for `common.RunDestinationServer` which is
now called by the embedded base).

**Note:** This task adds `MaxRecvMsgSize(8MB)` to
CephFS destination via the base worker, which it did
not have before. This is a safe behavioral change —
it raises the limit from the 4MB gRPC default, which
is consistent with the source side already sending
up to 3MB payloads.

- [ ] **Step 3: Verify it compiles**

Run: `cd internal/worker/cephfs && go build -tags ceph_preview ./...`
Expected: no errors

- [ ] **Step 4: Commit**

```bash
git add internal/worker/cephfs/destination.go
git commit -sm"refactor: CephFS destination embeds BaseDestinationWorker"
```

---

### Task 8: Refactor cmd/mover/main.go factory

**Files:**
- Modify: `cmd/mover/main.go`

- [ ] **Step 1: Replace `Runner` interface and `newWorker` with factory registry**

In `cmd/mover/main.go`:

1. Replace the `Runner` interface and `newWorker`
   function (lines 181-201) with:

```go
// workerFactory creates a Worker for a given
// worker type and config.
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

2. Replace `newCephFSWorker` (lines 203-227):

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
			"invalid worker type: %s",
			workerType,
		)
	}
}
```

3. Replace `newRBDWorker` (lines 229-253):

```go
func newRBDWorker(
	logger logr.Logger,
	workerType string,
	srcCfg common.SourceConfig,
	dstCfg common.DestinationConfig,
) (common.Worker, error) {
	switch workerType {
	case workerTypeSource:
		return wrbd.NewSourceWorker(
			logger, srcCfg,
		), nil
	case workerTypeDestination:
		return wrbd.NewDestinationWorker(
			logger, dstCfg,
		), nil
	default:
		return nil, fmt.Errorf(
			"invalid worker type: %s",
			workerType,
		)
	}
}
```

- [ ] **Step 2: Update imports**

Add `"github.com/RamenDR/ceph-volsync-plugin/internal/worker/common"`
to imports. Remove the `"github.com/RamenDR/ceph-volsync-plugin/internal/worker"`
import if it is no longer used (check if `worker.EnvWorkerType` etc.
are still referenced — they are, so keep it).

- [ ] **Step 3: Verify it compiles**

Run: `cd cmd/mover && go build -tags ceph_preview ./...`
Expected: no errors

- [ ] **Step 4: Commit**

```bash
git add cmd/mover/main.go
git commit -sm"refactor: use common.Worker factory registry in mover entry point"
```

---

### Task 9: Run full validation

**Files:** none (verification only)

- [ ] **Step 1: Run unit tests**

Run: `make test`
Expected: all tests PASS

- [ ] **Step 2: Build mover image**

Run: `make docker-build-mover`
Expected: build succeeds

- [ ] **Step 3: Verify no duplicate configs remain**

Run grep to confirm no `SourceConfig` or
`DestinationConfig` in `rbd/` or `cephfs/`:

```bash
grep -r "type SourceConfig struct" \
  internal/worker/rbd/ internal/worker/cephfs/
grep -r "type DestinationConfig struct" \
  internal/worker/rbd/ internal/worker/cephfs/
```
Expected: no output (zero matches)

- [ ] **Step 4: Verify no Runner interface in main.go**

```bash
grep "Runner" cmd/mover/main.go
```
Expected: no output

- [ ] **Step 5: Commit any fixes if needed**

If validation revealed issues, fix and commit.
Otherwise, this task is a verification-only pass.
