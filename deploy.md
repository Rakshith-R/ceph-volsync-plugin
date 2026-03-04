# Deployment Guide

## Simplified Command

Build, push, and deploy both operator and mover with a single command:

```bash
make build-push-deploy IMG=ghcr.io/rakshith-r/ceph-volsync-plugin-operator:pp-v10 MOVER_IMG=ghcr.io/rakshith-r/ceph-volsync-plugin-mover:pp-v10
```

stable:

make build-push-deploy IMG=ghcr.io/rakshith-r/ceph-volsync-plugin-operator:v0.1.0 MOVER_IMG=ghcr.io/rakshith-r/ceph-volsync-plugin-mover:v0.1.0 PUSH_TO_LOCAL=false

make deploy IMG=ghcr.io/rakshith-r
/ceph-volsync-plugin-operator:pp-v18 MOVER_IMG=ghcr.io/rakshith-r/ceph-volsync-plugin-mover:pp-v20

---
 make build-push-deploy IMG=ghcr.io/rakshith-r/ceph-volsync-plugin-operator:s-v1 MOVER_IMG=ghcr.io/rakshith-r/ceph-volsync-plugin-mover:s-v1 PUSH_TO_LOCAL=false

oc adm policy add-scc-to-user privileged \
    system:serviceaccount:ceph-volsync-plugin-operator-system:volsync-dst-cft-dest

oc adm policy add-scc-to-user privileged \
    system:serviceaccount:ceph-volsync-plugin-operator-system:volsync-src-cft-src

This single command performs all of the following steps:
1. Build operator image
2. Push operator image
3. Build mover imagek 
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
