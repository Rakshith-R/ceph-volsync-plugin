# internal/proto

Protocol Buffer definitions and generated gRPC stubs
for the ceph-volsync-plugin mover worker.

## Directory Structure

```
internal/proto/
├── api/v1/
│   ├── data.proto        DataService  (Sync, Delete)
│   ├── done.proto        DoneService  (Done)
│   └── metadata.proto    MetadataService (Get, Update)
└── version/v1/
    └── version.proto     VersionService  (GetVersion)
```

Generated `.pb.go` and `_grpc.pb.go` files live alongside
their source `.proto` files and are committed to the repo.

## Services

### DataService (`api/v1/data.proto`)

Client-streaming sync and batch deletion.

```protobuf
service DataService {
  rpc Sync(stream SyncRequest) returns (SyncResponse);
  rpc Delete(DeleteRequest) returns (DeleteResponse);
}
```

`SyncRequest` is a `oneof` of `WriteRequest` (batched
`ChangedBlock` data) or `CommitRequest` (flush and
close the current file). The stream is reusable: after a
`CommitRequest` the source can send a new `WriteRequest`
for the next file without reopening the stream.

### DoneService (`api/v1/done.proto`)

Signals graceful shutdown.

```protobuf
service DoneService {
  rpc Done(DoneRequest) returns (DoneResponse);
}
```

On success the destination exits with code 0.

### MetadataService (`api/v1/metadata.proto`)

Checkpoint key-value state persistence.

```protobuf
service MetadataService {
  rpc Get(GetMetadataRequest) returns (Metadata);
  rpc Update(UpdateMetadataRequest) returns (Metadata);
}
```

### VersionService (`version/v1/version.proto`)

Version handshake between source and destination workers.

```protobuf
service VersionService {
  rpc GetVersion(GetVersionRequest) returns (GetVersionResponse);
}
```

The source calls `GetVersion` immediately after connecting.
If the version string does not match, the connection is
rejected before any data is transferred.

## Go Package Paths

| Proto package | Go import path |
|---------------|----------------|
| `api.v1` | `.../internal/proto/api/v1` |
| `version.v1` | `.../internal/proto/version/v1` |

Conventional import aliases used across the codebase:

```go
import (
    apiv1    "github.com/RamenDR/ceph-volsync-plugin/internal/proto/api/v1"
    versionv1 "github.com/RamenDR/ceph-volsync-plugin/internal/proto/version/v1"
)
```

## Generating Code

No host installation required — generation runs inside a
container built from `build/Containerfile.protoc`.

**Build the generation image (one-time):**

```
make proto-image
```

**Regenerate all stubs:**

```
make proto-generate
```

This deletes existing `*.pb.go` files and regenerates them
from the `.proto` sources. Commit the result.

**Verify generated files match committed state (CI):**

```
make proto-verify
```

Fails if regeneration produces any diff — use this in CI
to catch stale generated code.

## Tooling Versions

Pinned in `build/build.env`:

| Tool | Variable | Version |
|------|----------|---------|
| protoc | `PROTOC_VERSION` | 29.3 |
| protoc-gen-go | `PROTOC_GEN_GO_VERSION` | v1.36.11 |
| protoc-gen-go-grpc | `PROTOC_GEN_GO_GRPC_VERSION` | v1.6.1 |

Override on the CLI:

```
make proto-generate PROTOC_VERSION=30.0
```

## Versioning

- **Non-breaking**: adding optional fields, adding RPCs,
  adding new messages. Safe to deploy without coordinating
  source and destination pod versions.
- **Breaking**: removing or renaming fields/RPCs, changing
  field types, changing field numbers. Requires coordinated
  rollout and a `VersionService` version bump so old pods
  are rejected before data transfer begins.
