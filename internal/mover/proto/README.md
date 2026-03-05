# Protocol Buffer Definitions

This directory contains the Protocol Buffer (protobuf) definitions for the Ceph VolSync Plugin mover services.

## Directory Structure

```
internal/mover/proto/
├── api/v1/           # Core API services for data operations
│   ├── data.proto    # Block-level write operations
│   ├── done.proto    # Completion signaling service
│   └── metadata.proto # State persistence service
└── version/v1/       # Version information service
    └── version.proto  # Version query service
```

## Services

### API Services (`api/v1`)

These services handle the core data movement and state management operations.

#### DataService
**File**: [`data.proto`](api/v1/data.proto)

Provides block-level write operations for efficient data transfer.

**RPCs**:
- `Write(WriteRequest) → WriteResponse`: Writes one or more changed blocks

**Use Cases**:
- Incremental data synchronization
- Block-level data replication
- Efficient delta transfers

#### DoneService
**File**: [`done.proto`](api/v1/done.proto)

Signals task completion and requests graceful shutdown.

**RPCs**:
- `Done(DoneRequest) → DoneResponse`: Indicates all work is complete, triggers graceful exit

**Use Cases**:
- Coordinating completion between source and destination
- Triggering cleanup operations
- Ensuring graceful shutdown

#### MetadataService
**File**: [`metadata.proto`](api/v1/metadata.proto)

Manages checkpoint and persistent state for resumable operations.

**RPCs**:
- `Get(GetMetadataRequest) → Metadata`: Retrieves current metadata state
- `Update(UpdateMetadataRequest) → Metadata`: Atomically updates metadata

**Use Cases**:
- Checkpoint management for resumable transfers
- Storing transfer progress
- Maintaining synchronization state

### Version Service (`version/v1`)

#### VersionService
**File**: [`version.proto`](version/v1/version.proto)

Provides version information for compatibility checking.

**RPCs**:
- `GetVersion(GetVersionRequest) → GetVersionResponse`: Returns semantic version

**Use Cases**:
- Protocol version negotiation
- Compatibility verification
- Debugging and diagnostics

## Regenerating Go Code

After modifying any `.proto` files, regenerate the Go code:

```bash
make buf-generate
```

This command:
1. Validates proto files using `buf lint`
2. Generates Go code (`.pb.go` files)
3. Generates gRPC service code (`_grpc.pb.go` files)

## Package Organization

The proto packages follow this naming convention:
- **Proto package**: `api.v1` or `version.v1`
- **Go package**: `github.com/RamenDR/ceph-volsync-plugin/internal/mover/proto/api/v1`

## Versioning Strategy

### Current Version: v1

All services are currently at version `v1`. When making changes:

**Breaking Changes** (require new version):
- Removing fields or RPCs
- Changing field types
- Renaming services or methods

**Non-Breaking Changes** (can stay in v1):
- Adding new optional fields
- Adding new RPCs
- Adding new services

### Future Versioning

When v2 is needed:
```
internal/mover/proto/
├── api/
│   ├── v1/  # Stable, maintained for backwards compatibility
│   └── v2/  # New version with breaking changes
└── version/
    ├── v1/
    └── v2/
```

## Development Workflow

1. **Modify Proto Files**: Edit `.proto` files as needed
2. **Validate**: Run `make buf-lint` to check for issues
3. **Generate**: Run `make buf-generate` to create Go code
4. **Test**: Ensure all tests pass with `make test`
5. **Review**: Check generated files are correct

## Dependencies

- **buf**: Protocol buffer tooling (installed via `make buf`)
- **protoc-gen-go**: Go code generator
- **protoc-gen-go-grpc**: gRPC Go code generator

## Best Practices

1. **Always use semantic versioning** in packages
2. **Document all services, RPCs, and messages** with comments
3. **Use optional fields** for extensibility
4. **Validate changes** don't break existing clients
5. **Keep related services together** in the same version directory

## Related Documentation

- [gRPC Go Documentation](https://grpc.io/docs/languages/go/)
- [Protocol Buffers Documentation](https://protobuf.dev/)
- [Buf Documentation](https://buf.build/docs/)

## Support

For questions or issues with the proto definitions, please:
1. Check existing documentation
2. Review proto file comments
3. Consult the team's architecture documentation