# stunnel.sh Integration Summary

## Overview

This document summarizes the integration of `stunnel.sh` into the Ceph VolSync Plugin mover to provide TLS tunneling with PSK authentication for data replication. The implementation supports dual tunnels: one for gRPC communication (always enabled) and one for rsync daemon communication (configurable via environment variable).

## Changes Made

### 1. build/Containerfile.mover

**Modified Lines**: 5-6, 27-32

**Changes**:
- Added `RSYNC_VERSION` build argument with default empty value (line 6)
- Added `bash` package installation (required by stunnel.sh)
- Added `rsync` package installation with optional version specification
- Copied `build/stunnel.sh` into the container at `/stunnel.sh`
- Made stunnel.sh executable with `chmod +x`
- Changed entrypoint from `/mover` to `/stunnel.sh`

**Rationale**: The stunnel.sh script needs to run as root to manage the stunnel daemon and bind to network ports. The script uses `exec` to replace itself with the mover binary, maintaining process management. Rsync is installed to support the optional rsync tunnel functionality.

### 2. internal/mover/cephfs/mover.go

**Modified Function**: `ensureJob` (lines 394-445)

**Changes**:

#### Added Environment Variables (lines 397-427):
1. **WORKER_TYPE**: Set to "source" or "destination" based on `m.isSource`
   - Required by stunnel.sh to determine configuration mode
   
2. **SERVER_PORT**: Set to "8080" or custom port from `m.port`
   - Used by stunnel.sh to configure gRPC forwarding port
   - Destination: stunnel forwards to this port where mover gRPC server listens
   - Source: Passed to mover for consistency

3. **ENABLE_RSYNC_TUNNEL**: Set to "true" for CephFS mover (default enabled)
   - Controls whether the rsync stunnel tunnel is created
   - Accepts: true/false, 1/0, yes/no, on/off (case-insensitive)

4. **RSYNC_PORT**: Set to "8873"
   - External port for rsync tunnel on destination
   - Port that source connects to for rsync traffic

5. **RSYNC_DAEMON_PORT**: Set to "8873"
   - Internal port where rsync daemon listens on destination
   - Port that stunnel forwards to locally

#### Modified Security Context (line 445):
- Added `RunAsUser: ptr.To[int64](0)` to run container as root
- Maintains existing security restrictions:
  - `AllowPrivilegeEscalation: false`
  - `Capabilities.Drop: ["ALL"]`
  - `Privileged: false`
  - `ReadOnlyRootFilesystem: true`

**Rationale**: stunnel requires root privileges to bind to ports and manage daemon processes. The security context still maintains strong restrictions while allowing necessary operations. The rsync tunnel is enabled by default for CephFS to support direct rsync daemon communication alongside gRPC.

### 3. Makefile

**Modified Lines**: 58-63, 218-234

**Changes**:
- Added `RSYNC_VERSION` variable with default empty value (line 63)
- Modified `docker-build-mover` target to pass `RSYNC_VERSION` build arg (lines 218-221)
- Modified `docker-buildx-mover` target to pass `RSYNC_VERSION` build arg (lines 226-234)

**Rationale**: Allows users to specify a particular rsync version during build time using `make docker-build-mover RSYNC_VERSION=3.2.7-r4`. If not specified, the latest available rsync version from Alpine repositories is installed.

### 4. build/stunnel.sh

**Modified Lines**: Throughout

**Changes**:
- Added support for dual tunnels (gRPC + rsync)
- Added `ENABLE_RSYNC_TUNNEL` environment variable (default: false)
- Added `RSYNC_PORT` environment variable (default: 8873)
- Added `RSYNC_DAEMON_PORT` environment variable (default: 8873)
- Modified configuration to create separate stunnel services:
  - `[grpc-tls]`: Always created for mover gRPC communication
  - `[rsync-tls]`: Created only when `ENABLE_RSYNC_TUNNEL=true`

**Rationale**: Separates concerns between gRPC and rsync traffic, allowing independent configuration and troubleshooting. The rsync tunnel is optional and can be enabled per deployment based on requirements.

### 5. cmd/mover/main.go

**Changes**: Removed cobra CLI flags in favour of
environment variables.

The mover binary now reads its configuration directly
from environment variables set by the Kubernetes Job:
- `WORKER_TYPE`: Required, "source" or "destination"
- `DESTINATION_ADDRESS`: Optional, host:port
- `LOG_LEVEL`: Optional, default "info"
- `SERVER_PORT`: Optional, default "8080"

**Rationale**: The Job already sets these as env vars
and stunnel.sh was redundantly converting them to CLI
flags. Reading env vars directly removes the cobra
dependency and the intermediary conversion step.

## Architecture

### Port Mapping

#### Destination Worker (Dual Tunnel Mode):
```
gRPC Traffic:
External Client → Service:8000 → Pod:8000 → stunnel [grpc-tls] → 127.0.0.1:8080 → mover gRPC server

Rsync Traffic (when ENABLE_RSYNC_TUNNEL=true):
External Client → Service:8873 → Pod:8873 → stunnel [rsync-tls] → 127.0.0.1:8873 → rsync daemon
```

#### Source Worker (Dual Tunnel Mode):
```
gRPC Traffic:
mover gRPC client → 127.0.0.1:8001 → stunnel [grpc-tls] → DESTINATION_ADDRESS:8000

Rsync Traffic (when ENABLE_RSYNC_TUNNEL=true):
rsync client → 127.0.0.1:8873 → stunnel [rsync-tls] → DESTINATION_ADDRESS:8873
```

### Environment Variables Flow

The Kubernetes Job spec sets these environment variables:
- `WORKER_TYPE`: "source" or "destination" (required)
- `SERVER_PORT`: "8080" (or custom) - gRPC server port
- `LOG_LEVEL`: "info" (default) - mover log level
- `DESTINATION_ADDRESS`: Target address (source only)
- `DESTINATION_PORT`: Target port for gRPC
  (source only, default 8000)
- `ENABLE_RSYNC_TUNNEL`: "true" or "false"
  (default: false for generic, true for CephFS)
- `RSYNC_PORT`: External rsync port (default: 8873)
- `RSYNC_DAEMON_PORT`: Internal rsync daemon port
  (default: 8873)

The mover binary reads `WORKER_TYPE`,
`DESTINATION_ADDRESS`, `LOG_LEVEL`, and `SERVER_PORT`
directly from the environment (no CLI flags).

The stunnel.sh script:
1. Validates environment variables
2. Normalizes `ENABLE_RSYNC_TUNNEL` to true/false
3. Checks for PSK file at `/keys/psk.txt`
4. Generates stunnel configuration with:
   - `[grpc-tls]` service (always created)
   - `[rsync-tls]` service (created only if ENABLE_RSYNC_TUNNEL=true)
5. Starts stunnel daemon
6. Starts rsync daemon on destination if
   ENABLE_RSYNC_TUNNEL=true
   - Configured at `/tmp/rsyncd.conf`
   - Listens on RSYNC_DAEMON_PORT (default: 8873)
   - Serves `/data` directory
   - Uses PSK file for authentication
7. For source workers, overrides
   `DESTINATION_ADDRESS` to `127.0.0.1:8001`
   (the local stunnel endpoint)
8. Executes `/mover` (reads config from env vars)

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

### Destination Worker (gRPC Only)
- [ ] stunnel starts successfully and listens on port 8000
- [ ] stunnel [grpc-tls] forwards connections to 127.0.0.1:8080
- [ ] mover gRPC server starts on port 8080
- [ ] Service exposes port 8000 correctly
- [ ] PSK authentication works
- [ ] TLS encryption is active

### Destination Worker (Dual Tunnel: gRPC + Rsync)
- [ ] stunnel starts successfully and listens on ports 8000 and 8873
- [ ] stunnel [grpc-tls] forwards connections to 127.0.0.1:8080
- [ ] stunnel [rsync-tls] forwards connections to 127.0.0.1:8873
- [ ] mover gRPC server starts on port 8080
- [ ] rsync daemon starts on port 8873
- [ ] Service exposes ports 8000 and 8873 correctly
- [ ] PSK authentication works for both tunnels
- [ ] TLS encryption is active for both tunnels

### Source Worker (gRPC Only)
- [ ] stunnel starts successfully and listens on 127.0.0.1:8001
- [ ] stunnel [grpc-tls] connects to destination address with TLS/PSK
- [ ] mover gRPC client connects to 127.0.0.1:8001
- [ ] Data transfer completes successfully
- [ ] Proper cleanup on completion

### Source Worker (Dual Tunnel: gRPC + Rsync)
- [ ] stunnel starts successfully and listens on 127.0.0.1:8001 and 127.0.0.1:8873
- [ ] stunnel [grpc-tls] connects to DESTINATION_ADDRESS:8000 with TLS/PSK
- [ ] stunnel [rsync-tls] connects to DESTINATION_ADDRESS:8873 with TLS/PSK
- [ ] mover gRPC client connects to 127.0.0.1:8001
- [ ] rsync client connects to 127.0.0.1:8873
- [ ] Data transfer completes successfully via both channels
- [ ] Proper cleanup on completion

### Error Handling
- [ ] Missing PSK file is detected and reported
- [ ] Invalid WORKER_TYPE is rejected
- [ ] Invalid ENABLE_RSYNC_TUNNEL values are handled gracefully
- [ ] stunnel startup failures are caught
- [ ] Connection failures are logged appropriately

## Files Modified

| File | Lines | Description |
|------|-------|-------------|
| `build/Containerfile.mover` | 5-6, 27-32 | Add RSYNC_VERSION arg, install rsync, add stunnel.sh, change entrypoint |
| `internal/mover/cephfs/mover.go` | 394-445 | Add env vars (including rsync tunnel config), adjust security context |
| `build/stunnel.sh` | Throughout | Add dual tunnel support (gRPC + rsync) with ENABLE_RSYNC_TUNNEL flag |
| `Makefile` | 58-63, 218-234 | Add RSYNC_VERSION support for build targets |
| `cmd/mover/main.go` | - | No changes (already compatible) |

## Deployment Notes

1. **PSK Secret**: Ensure the PSK secret exists with the correct format before deploying
2. **Service Configuration**:
   - The Service must expose port 8000 for gRPC traffic on destination workers
   - If rsync tunnel is enabled, the Service must also expose port 8873 for rsync traffic
3. **Network Policies**:
   - Ensure network policies allow traffic on port 8000 for gRPC
   - If rsync tunnel is enabled, also allow traffic on port 8873
4. **Monitoring**: Check stunnel logs at `/tmp/stunnel.log` for debugging
5. **Build Configuration**:
   - Use `make docker-build-mover` to build with latest rsync
   - Use `make docker-build-mover RSYNC_VERSION=3.2.7-r4` to build with specific rsync version
6. **CephFS vs RBD**: The rsync tunnel is enabled by default for CephFS movers but not for RBD movers

## References

- stunnel.sh script: `build/stunnel.sh`
- stunnel documentation: https://www.stunnel.org/
- PSK cipher suites: https://www.stunnel.org/auth.html
- rsync documentation: https://rsync.samba.org/
- Alpine Linux rsync package: https://pkgs.alpinelinux.org/packages?name=rsync

## Usage Examples

### Building with Default Rsync Version
```bash
make docker-build-mover
```

### Building with Specific Rsync Version
```bash
make docker-build-mover RSYNC_VERSION=3.2.7-r4
```

### Enabling Rsync Tunnel (Environment Variable)
For CephFS, the rsync tunnel is enabled by default. To disable it, set:
```yaml
env:
  - name: ENABLE_RSYNC_TUNNEL
    value: "false"
```

For other movers, to enable the rsync tunnel:
```yaml
env:
  - name: ENABLE_RSYNC_TUNNEL
    value: "true"
```