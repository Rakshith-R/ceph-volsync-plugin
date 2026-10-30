# Contributing to ceph-volsync-plugin

## Prerequisites

- **Go** 1.25+
- **Docker** or **Podman**
- **Ceph development headers** (required for mover build; CGO-based):
  - Fedora/RHEL: `dnf install libcephfs-devel librbd-devel librados-devel`
  - Ubuntu/Debian: `apt install libcephfs-dev librbd-dev librados-dev`
- **golangci-lint** — installed automatically via `make lint`

## Getting Started

```bash
git clone https://github.com/RamenDR/ceph-volsync-plugin.git
cd ceph-volsync-plugin

# Run all code generation (required before build/test)
make manifests generate proto-generate

# Build the manager binary
make build
```

## Development Container

The repository ships a `.devcontainer/` configuration (Docker-in-Docker) compatible with
VS Code Dev Containers and GitHub Codespaces. It includes Ceph headers and tooling.

> The devcontainer image currently uses Go 1.24. The project requires Go 1.25.7.
> Prefer a local Go 1.25+ installation or bump the image in `.devcontainer/devcontainer.json`.

## Code Generation Workflow

Generated files must be kept in sync with their sources. Run the appropriate
target whenever you change the input files:

| Change | Command |
|--------|---------|
| `.proto` files | `make proto-generate` |
| `+kubebuilder` markers or API types | `make generate` |
| RBAC markers or CRD specs | `make manifests` |

All three are run automatically as prerequisites of `make test`.

To verify nothing is out of sync before committing:

```bash
make check-uncommitted
```

## Running Tests

```bash
# Unit tests (uses controller-runtime envtest)
make test

# Lint
make lint        # report only
make lint-fix    # auto-fix where possible

# E2E tests — Kind cluster with Rook Ceph (~15-30 min)
make test-e2e

# Tear down the Kind cluster when done
make cleanup-test-e2e
```

The local e2e suite requires Docker, Kind, kubectl, and Helm. `make setup-test-e2e`
creates the Kind cluster; `make test-e2e` calls it automatically. CI uses Minikube instead.

## Pull Request Process

1. Fork the repository and create a feature branch.
2. Run `make lint test` locally before pushing — CI will fail otherwise.
3. Open a pull request against `main`. CI runs:
   - **lint**: codespell, golangci-lint, govulncheck
   - **build**: operator and mover container images (multi-arch)
   - **e2e**: Kind (local) / Minikube (CI) with Rook Ceph, full replication test suite
4. Squash merge is preferred for feature PRs; merge commits for release branches.
5. Add a brief description of **what** changed and **why** in the PR body.

## Code Style

- Follow existing patterns in the codebase.
- **Mover** (`cmd/mover`, `internal/worker/`) requires CGO (`CGO_ENABLED=1`)
  because it uses go-ceph bindings against the Ceph C libraries.
- **Manager** (`cmd/manager`, `internal/controller/`, `internal/mover/`)
  is pure Go (`CGO_ENABLED=0`, distroless image).
- Use the `ceph_preview` build tag for APIs marked experimental in go-ceph.
- Protobuf generated files live in `internal/proto/`; do not edit them by hand.
- Keep commit messages concise: `<type>: <short description>` (e.g. `fix: ...`,
  `feat: ...`, `refactor: ...`).

## Project Structure

```
cmd/
  manager/   Kubernetes operator entry point (controller setup, mover registration)
  mover/     Mover pod entry point (worker factory, tunnel setup)
internal/
  ceph/      go-ceph wrappers (CephFS snapshot diff, RBD diff, cluster connection)
  controller/ Kubernetes controllers for ReplicationSource / ReplicationDestination
  mover/     Mover builder pattern and Ceph mover implementation (Job orchestration)
  proto/     gRPC service definitions (.proto) and generated Go code
  worker/    Source and destination workers (cephfs/, rbd/, pipeline/, tunnel/)
config/      Kustomize manifests (CRDs, RBAC, manager deployment, samples)
test/
  e2e/       Ginkgo v2 end-to-end test suite
  scripts/   Cluster setup helpers (Rook Ceph, snapshot controller, VolSync)
```

## Updating Protobuf Definitions

1. Edit `.proto` files under `internal/proto/`.
2. Run `make proto-generate` (uses the pinned protoc image from `build/build.env`).
3. Commit both the `.proto` source and the generated `.pb.go` / `_grpc.pb.go` files.

To verify generated files match committed sources:

```bash
make proto-verify
```

## Building Container Images

```bash
# Manager image
make docker-build IMG=ghcr.io/your-org/ceph-volsync-plugin-operator:dev

# Mover image (requires Ceph base image; CGO)
make docker-build-mover MOVER_IMG=ghcr.io/your-org/ceph-volsync-plugin-mover:dev

# Multi-arch builds
make docker-buildx IMG=...
make docker-buildx-mover MOVER_IMG=...
```

Override the container tool with `CONTAINER_TOOL=podman` if needed.
