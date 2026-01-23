# Deployment Guide

## Simplified Command

Build, push, and deploy both operator and mover with a single command:

```bash
make build-push-deploy IMG=ghcr.io/rakshith-r/ceph-volsync-plugin-operator:test-v1 MOVER_IMG=ghcr.io/rakshith-r/ceph-volsync-plugin-mover:test-v1
```

This single command performs all of the following steps:
1. Build operator image
2. Push operator image
3. Build mover image
4. Push mover image
5. Deploy to cluster

## Individual Commands (if needed)

If you need to run the steps separately:

```bash
# Build and push operator
make docker-build IMG=ghcr.io/rakshith-r/ceph-volsync-plugin-operator:test-v2
make docker-push IMG=ghcr.io/rakshith-r/ceph-volsync-plugin-operator:test-v2

# Build and push mover
make docker-build-mover MOVER_IMG=ghcr.io/rakshith-r/ceph-volsync-plugin-mover:test-v1
make docker-push-mover MOVER_IMG=ghcr.io/rakshith-r/ceph-volsync-plugin-mover:test-v1

# Deploy
make deploy IMG=ghcr.io/rakshith-r/ceph-volsync-plugin-operator:test-v1 MOVER_IMG=ghcr.io/rakshith-r/ceph-volsync-plugin-mover:test-v1
