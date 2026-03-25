# internal/proto

Protocol Buffer definitions and generated gRPC stubs
for data transfer between source and destination worker
pods.

## Directory Structure

```
internal/proto/
├── api/v1/
│   └── sync.proto       SyncService
└── version/v1/
    └── version.proto    VersionService
```

Generated `.pb.go` and `_grpc.pb.go` files live
alongside their `.proto` sources and are committed
to the repo.

## Services

### SyncService (`api/v1/sync.proto`)

Single consolidated service for all data-path RPCs.

**Bidi-streaming RPCs:**

```protobuf
rpc Write(stream WriteRequest)
    returns (stream WriteResponse);
rpc CompareHashes(stream HashRequest)
    returns (stream HashResponse);
rpc Commit(stream CommitRequest)
    returns (stream CommitResponse);
rpc Delete(stream DeleteRequest)
    returns (stream DeleteResponse);
```

**Unary RPCs:**

```protobuf
rpc Done(DoneRequest) returns (DoneResponse);
```

### VersionService (`version/v1/version.proto`)

Version handshake between source and destination.

```protobuf
rpc GetVersion(GetVersionRequest)
    returns (GetVersionResponse);
```

## Go Package Paths

| Proto package | Go import path |
|---------------|----------------|
| `api.v1` | `.../internal/proto/api/v1` |
| `version.v1` | `.../internal/proto/version/v1` |

Conventional import aliases:

```go
import (
    apiv1 "github.com/RamenDR/ceph-volsync-plugin/internal/proto/api/v1"
    versionv1 "github.com/RamenDR/ceph-volsync-plugin/internal/proto/version/v1"
)
```

## Code Generation

Generation runs inside a container built from
`build/Containerfile.protoc`. No host installation
required.

```sh
make proto-image      # build the protoc image
make proto-generate   # regenerate all stubs
make proto-verify     # verify stubs match committed
```

`proto-verify` is a prerequisite of `make test` and
fails if regeneration produces any diff.

## Tooling Versions

Pinned in `build/build.env`:

| Tool | Variable | Version |
|------|----------|---------|
| protoc | `PROTOC_VERSION` | 29.3 |
| protoc-gen-go | `PROTOC_GEN_GO_VERSION` | v1.36.11 |
| protoc-gen-go-grpc | `PROTOC_GEN_GO_GRPC_VERSION` | v1.6.1 |

Override on the CLI:

```sh
make proto-generate PROTOC_VERSION=30.0
```

## Versioning

- **Non-breaking**: adding optional fields, new RPCs,
  or new messages. Safe to deploy independently.
- **Breaking**: removing or renaming fields/RPCs,
  changing field types or numbers. Requires
  coordinated rollout and a VersionService version
  bump so old pods are rejected before data transfer.
