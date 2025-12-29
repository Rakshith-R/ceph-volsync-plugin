# stunnel.sh Integration Summary

## Overview

This document summarizes the integration of `stunnel.sh` into the Ceph VolSync Plugin mover to provide TLS tunneling with PSK authentication for data replication.

## Changes Made

### 1. build/Containerfile.mover

**Modified Lines**: 25-34

**Changes**:
- Added `bash` package installation (required by stunnel.sh)
- Copied `build/stunnel.sh` into the container at `/stunnel.sh`
- Made stunnel.sh executable with `chmod +x`
- Removed non-root user creation (lines 31-32)
- Changed entrypoint from `/mover` to `/stunnel.sh`

**Rationale**: The stunnel.sh script needs to run as root to manage the stunnel daemon and bind to network ports. The script uses `exec` to replace itself with the mover binary, maintaining process management.

### 2. internal/mover/cephfs/mover.go

**Modified Function**: `ensureJob` (lines 394-425)

**Changes**:

#### Added Environment Variables (lines 397-413):
1. **WORKER_TYPE**: Set to "source" or "destination" based on `m.isSource`
   - Required by stunnel.sh to determine configuration mode
   
2. **SERVER_PORT**: Set to "8080" or custom port from `m.port`
   - Used by stunnel.sh to configure forwarding port
   - Destination: stunnel forwards to this port where mover gRPC server listens
   - Source: Passed to mover for consistency

#### Modified Security Context (line 424):
- Added `RunAsUser: ptr.To[int64](0)` to run container as root
- Maintains existing security restrictions:
  - `AllowPrivilegeEscalation: false`
  - `Capabilities.Drop: ["ALL"]`
  - `Privileged: false`
  - `ReadOnlyRootFilesystem: true`

**Rationale**: stunnel requires root privileges to bind to ports and manage daemon processes. The security context still maintains strong restrictions while allowing necessary operations.

### 3. cmd/mover/main.go

**Changes**: None required

**Rationale**: The existing implementation already supports all required command-line flags:
- `--worker-type`: Used by stunnel.sh to determine mode
- `--destination-address`: Passed by stunnel.sh (modified to 127.0.0.1:8001 for source)
- `--server-port`: Used by mover to start gRPC server

The stunnel.sh script correctly constructs these arguments (lines 103-108).

## Architecture

### Port Mapping

#### Destination Worker:
```
External Client → Service:8000 → Pod:8000 → stunnel → 127.0.0.1:8080 → mover gRPC server
```

#### Source Worker:
```
mover gRPC client → 127.0.0.1:8001 → stunnel → DESTINATION_ADDRESS:8000
```

### Environment Variables Flow

The Kubernetes Job spec sets these environment variables:
- `WORKER_TYPE`: "source" or "destination"
- `SERVER_PORT`: "8080" (or custom)
- `DESTINATION_ADDRESS`: Target address (source only)
- `DESTINATION_PORT`: Target port (source only, default 8000)

The stunnel.sh script:
1. Validates environment variables
2. Checks for PSK file at `/keys/psk.txt`
3. Generates stunnel configuration
4. Starts stunnel daemon
5. Executes `/mover` with appropriate arguments

### Security Model

**Container Security**:
- Runs as root (UID 0) for stunnel operation
- Read-only root filesystem (only /tmp is writable via emptyDir)
- All Linux capabilities dropped
- No privilege escalation allowed
- Not running in privileged mode

**Network Security**:
- TLS encryption for all data transfer
- PSK (Pre-Shared Key) authentication
- Secret mounted at `/keys/psk.txt` with mode 0600

**PSK Secret Format**:
```
volsync:<128-character-hex-encoded-key>
```

Generated automatically by the controller or provided by user.

## Testing Checklist

### Destination Worker
- [ ] stunnel starts successfully and listens on port 8000
- [ ] stunnel forwards connections to 127.0.0.1:8080
- [ ] mover gRPC server starts on port 8080
- [ ] Service exposes port 8000 correctly
- [ ] PSK authentication works
- [ ] TLS encryption is active

### Source Worker
- [ ] stunnel starts successfully and listens on 127.0.0.1:8001
- [ ] stunnel connects to destination address with TLS/PSK
- [ ] mover gRPC client connects to 127.0.0.1:8001
- [ ] Data transfer completes successfully
- [ ] Proper cleanup on completion

### Error Handling
- [ ] Missing PSK file is detected and reported
- [ ] Invalid WORKER_TYPE is rejected
- [ ] stunnel startup failures are caught
- [ ] Connection failures are logged appropriately

## Files Modified

| File | Lines | Description |
|------|-------|-------------|
| `build/Containerfile.mover` | 25-34 | Add stunnel.sh, change entrypoint, run as root |
| `internal/mover/cephfs/mover.go` | 394-425 | Add env vars, adjust security context |
| `cmd/mover/main.go` | - | No changes (already compatible) |

## Deployment Notes

1. **PSK Secret**: Ensure the PSK secret exists with the correct format before deploying
2. **Service Configuration**: The Service must expose port 8000 for destination workers
3. **Network Policies**: Ensure network policies allow traffic on port 8000
4. **Monitoring**: Check stunnel logs at `/tmp/stunnel.log` for debugging

## References

- stunnel.sh script: `build/stunnel.sh`
- stunnel documentation: https://www.stunnel.org/
- PSK cipher suites: https://www.stunnel.org/auth.html