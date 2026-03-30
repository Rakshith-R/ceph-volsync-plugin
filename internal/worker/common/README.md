# Worker Common Package

Shared infrastructure for all mover worker types (CephFS, RBD).

## Core Interfaces

- **`Worker`** — Base interface with `Run(ctx) error`, implemented by all workers.
- **`Syncer`** — Source-side sync interface, called by `BaseSourceWorker` after connection setup.
- **`WriteHandler`**, **`DeleteHandler`**, **`HashHandler`**, **`CommitHandler`** — gRPC stream handler interfaces for the `SyncServer`.

## Source/Destination Scaffolding

- **`BaseSourceWorker`** — Connects to destination via gRPC, performs version handshake, delegates to `Syncer.Sync()`, and cleans up the connection.
- **`BaseDestinationWorker`** — Starts a gRPC server with a `SyncServer` and blocks until shutdown.

## gRPC Infrastructure

- **`SyncServer`** — Implements `SyncService` gRPC server. Delegates RPCs (Write, Delete, CompareHashes, Commit) to pluggable handler interfaces. The `Done` RPC signals graceful shutdown.
- **`VersionServer`** — Implements `VersionService` for version handshake.
- **`RunDestinationServer()`** — Starts the gRPC server, registers services, and handles shutdown via context cancellation, server error, or Done signal.
- **`ConnectToDestination()`** — Establishes a gRPC client connection with version verification.
- **`SignalDone()`** — Sends the Done RPC to trigger destination shutdown.

## Constants

| Constant | Value | Description |
|---|---|---|
| `ConnectionTimeout` | 60s | First RPC timeout (includes DNS, TCP, TLS) |
| `RPCTimeout` | 30s | Subsequent RPC timeout |
| `WritePayloadMinSize` | 2MB | Min data before sending WriteRequest |
| `WritePayloadMaxSize` | 3MB | Max data per WriteRequest |
| `MaxGRPCMessageSize` | 8MB | Max gRPC message size |
| `DefaultServerPort` | 8080 | gRPC server listen port |

## Utilities

- **`IsAllZero([]byte) bool`** — Checks if a byte slice is all zeros (used for sparse data optimization).
