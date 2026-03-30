# Tunnel Package

TLS tunneling for secure gRPC and rsync communication between source and destination mover pods.

## Architecture

```
Source Pod                              Destination Pod
┌─────────────────┐                    ┌─────────────────┐
│ Worker          │                    │ Worker          │
│   ↓ gRPC        │                    │   ↑ gRPC        │
│ stunnel (client)│── TLS over TCP ──→ │ stunnel (server)│
│   ↓ rsync       │── TLS over TCP ──→ │   ↑ rsync       │
│                 │                    │ rsync daemon    │
└─────────────────┘                    └─────────────────┘
```

All traffic is encrypted using PSK (Pre-Shared Key) authentication via stunnel. The gRPC connection itself uses insecure credentials since TLS is handled at the transport layer.

## Components

### Manager (`tunnel.go`)

Orchestrates the full tunnel lifecycle:

- **`Setup()`** — Validates PSK file, starts stunnel, optionally starts rsync daemon. Returns overridden destination address for source workers (pointing to local stunnel endpoint `127.0.0.1:8001`).
- **`Cleanup()`** — Stops rsync daemon and stunnel. Safe to call multiple times.

### Stunnel (`stunnel.go`)

Manages the stunnel subprocess:

- **Destination mode (server)**: Accepts TLS connections on the configured port, forwards to local gRPC server (`127.0.0.1:8080`) and optionally to rsync daemon.
- **Source mode (client)**: Listens locally on port `8001` (gRPC) and forwards over TLS to the destination's stunnel server.
- Generates stunnel config, manages PID file, waits for port readiness.

### Rsync Daemon (`rsync.go`)

Manages an rsync daemon subprocess on the destination side:

- Generates `rsyncd.conf` with a `[data]` module pointing to `/data`.
- Binds to `127.0.0.1` only (stunnel provides external access and authentication).
- Used for bulk file transfers that benefit from rsync's delta algorithm.

## Configuration

```go
StunnelConfig{
    WorkerType:         "source" | "destination",
    DestinationAddress: "dest-service.namespace.svc",
    DestinationPort:    "8443",
    EnableRsyncTunnel:  true,
    RsyncPort:          "8873",
    RsyncDaemonPort:    "873",
}
```

## PSK Authentication

The PSK file is expected at `/keys/psk.txt` (mounted from a Kubernetes Secret by the mover controller).
