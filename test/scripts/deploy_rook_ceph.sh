#!/bin/bash -e

# This script deploys or cleans up a Rook Ceph cluster
# and all required resources for e2e tests.

SCRIPT_DIR="$(dirname "${0}")"

# shellcheck disable=SC1091
[ ! -e "${SCRIPT_DIR}"/utils.sh ] || source "${SCRIPT_DIR}"/utils.sh

ROOK_VERSION="${ROOK_VERSION:-v1.19.2}"
ROOK_DIR="${ROOK_DIR:-/tmp/rook}"
ROOK_EXAMPLES="${ROOK_DIR}/deploy/examples"
ROOK_HELPER="${ROOK_DIR}/tests/scripts/github-action-helper.sh"
ROOK_CEPH_NS="rook-ceph"

function clone_rook() {
    if [ -d "${ROOK_DIR}" ]; then
        echo "Rook directory already exists, skipping clone"
        return
    fi
    echo "Cloning Rook ${ROOK_VERSION}..."
    git clone https://github.com/rook/rook.git "${ROOK_DIR}"
    cd "${ROOK_DIR}"
    git fetch --depth 1 origin tag "${ROOK_VERSION}"
    git checkout "${ROOK_VERSION}"
    cd - > /dev/null

    # TODO: remove this once, the fix is included in a Rook release.
    # Patch find_extra_block_dev to deduplicate boot devices
    sed -i "s/awk '{print \$2}'/awk '{print \$2}' | sort -u/" \
        "${ROOK_HELPER}"
}

function print_cluster_status() {
    "${ROOK_HELPER}" print_k8s_cluster_status
}

function use_local_disk() {
    "${ROOK_HELPER}" \
        use_local_disk_for_integration_test
}

function deploy_cluster_prerequisites() {
    echo "Deploying Rook CRDs and common resources..."
    "${ROOK_HELPER}" create_cluster_prerequisites
}

function deploy_rook_operator() {
    echo "Deploying Rook operator..."
    cd "${ROOK_DIR}"
    USE_LOCAL_BUILD=false \
        tests/scripts/github-action-helper.sh \
        deploy_manifest_with_local_build \
        deploy/examples/operator.yaml
    kubectl wait --for=condition=Available \
        deploy/rook-ceph-operator \
        -n "${ROOK_CEPH_NS}" \
        --timeout=120s
    kubectl get all --namespace "${ROOK_CEPH_NS}"
    cd - > /dev/null
}

function deploy_csi_operator() {
    echo "Deploying Ceph CSI Operator..."
    kubectl_retry create \
        -f "${ROOK_EXAMPLES}/csi-operator.yaml"
    kubectl wait --for=condition=Available \
        deploy/ceph-csi-controller-manager \
        -n "${ROOK_CEPH_NS}" \
        --timeout=120s
}

function deploy_ceph_cluster() {
    echo "Deploying CephCluster..."
    local device_filter
    device_filter=$("${ROOK_HELPER}" find_extra_block_dev)
    export DEVICE_FILTER="${device_filter}"
    sed -i \
        "s|#deviceFilter:|deviceFilter: ${DEVICE_FILTER}|g" \
        "${ROOK_EXAMPLES}/cluster-test.yaml"
    cat "${ROOK_EXAMPLES}/cluster-test.yaml"
    kubectl_retry create \
        -f "${ROOK_EXAMPLES}/cluster-test.yaml"
}

function deploy_toolbox() {
    echo "Deploying toolbox..."
    "${ROOK_HELPER}" deploy_toolbox
}

function wait_for_ceph() {
    echo "Waiting for OSD prepare pod..."
    kubectl get all --namespace "${ROOK_CEPH_NS}"
    "${ROOK_HELPER}" wait_for_prepare_pod 1
    kubectl get all --namespace "${ROOK_CEPH_NS}"

    echo "Waiting for Ceph to be ready..."
    cd "${ROOK_DIR}"
    tests/scripts/github-action-helper.sh \
        wait_for_ceph_to_be_ready osd 1
    cd - > /dev/null
}

function deploy_test_resources() {
    echo "Deploying e2e test resources..."
    kubectl_retry create \
        -f "${ROOK_EXAMPLES}/pool-test.yaml"
    kubectl_retry create \
        -f "${ROOK_EXAMPLES}/filesystem-test.yaml"
    kubectl_retry create \
        -f "${ROOK_EXAMPLES}/nfs-test.yaml"
    kubectl_retry create \
        -f "${ROOK_EXAMPLES}/subvolumegroup.yaml"
    kubectl_retry create \
        -f "${ROOK_EXAMPLES}/csi/cephfs/storageclass.yaml"
    kubectl_retry create \
        -f "${ROOK_EXAMPLES}/csi/cephfs/snapshotclass.yaml"
    kubectl_retry create \
        -f "${ROOK_EXAMPLES}/csi/nfs/storageclass.yaml"
    kubectl_retry create \
        -f "${ROOK_EXAMPLES}/csi/nfs/snapshotclass.yaml"
    kubectl_retry create \
        -f "${ROOK_EXAMPLES}/csi/rbd/storageclass-test.yaml"
    kubectl_retry create \
        -f "${ROOK_EXAMPLES}/csi/rbd/snapshotclass.yaml"
}

function wait_for_ceph_resources() {
    echo "Waiting for CephBlockPool..."
    local INC=0
    local phase
    until [ $INC -gt 30 ]; do
        phase=$(
            kubectl -n "${ROOK_CEPH_NS}" \
                get cephblockpool replicapool \
                -o jsonpath='{.status.phase}' \
                2>/dev/null || true
        )
        if [ "${phase}" = "Ready" ]; then
            echo "CephBlockPool is ${phase}"
            break
        fi
        echo "CephBlockPool phase:" \
            "${phase:-unknown} ($((INC + 1))/30)"
        sleep 10
        ((++INC))
    done

    echo "Waiting for CephFilesystem..."
    local INC=0
    local phase
    until [ $INC -gt 30 ]; do
        phase=$(
            kubectl -n "${ROOK_CEPH_NS}" \
                get cephfilesystem myfs \
                -o jsonpath='{.status.phase}' \
                2>/dev/null || true
        )
        if [ "${phase}" = "Ready" ] \
            || [ "${phase}" = "Connected" ]; then
            echo "CephFilesystem is ${phase}"
            break
        fi
        echo "CephFilesystem phase:" \
            "${phase:-unknown} ($((INC + 1))/30)"
        sleep 10
        ((++INC))
    done
    echo "Waiting for CephFilesystem MDS..."
    kubectl -n "${ROOK_CEPH_NS}" wait \
        --for=condition=Available \
        deploy/rook-ceph-mds-myfs-a \
        --timeout=300s

    echo "Waiting for CephNFS..."
    INC=0
    until [ $INC -gt 30 ]; do
        phase=$(
            kubectl -n "${ROOK_CEPH_NS}" \
                get cephnfs my-nfs \
                -o jsonpath='{.status.phase}' \
                2>/dev/null || true
        )
        if [ "${phase}" = "Ready" ] \
            || [ "${phase}" = "Connected" ]; then
            echo "CephNFS is ${phase}"
            break
        fi
        echo "CephNFS phase:" \
            "${phase:-unknown} ($((INC + 1))/30)"
        sleep 10
        ((++INC))
    done

    # Wait for CephNFS
    echo "Waiting for CephNFS server..."
    kubectl -n "${ROOK_CEPH_NS}" wait \
        --for=condition=Available \
        deploy/rook-ceph-nfs-my-nfs-a \
        --timeout=300s

    echo "All Ceph resources:"
    kubectl -n "${ROOK_CEPH_NS}" get \
        cephcluster,cephfilesystem,cephnfs,cephblockpool
    kubectl get storageclasses
    kubectl get volumesnapshotclasses

    kubectl -n "${ROOK_CEPH_NS}" wait \
        --for=condition=Ready pod \
        -lcontrol-plane=ceph-csi-op-controller-manager \
        --timeout=300s

    kubectl -n "${ROOK_CEPH_NS}" get pods
    kubectl -n "${ROOK_CEPH_NS}" get all
}

function install() {
    clone_rook
    print_cluster_status
    use_local_disk
    deploy_cluster_prerequisites
    deploy_rook_operator
    deploy_csi_operator
    deploy_ceph_cluster
    deploy_toolbox
    wait_for_ceph
    deploy_test_resources
    wait_for_ceph_resources
    echo "Rook Ceph deployment complete"
}

function cleanup() {
    echo "Cleaning up Rook Ceph resources..."
    kubectl_retry delete \
        -f "${ROOK_EXAMPLES}/csi/nfs/snapshotclass.yaml" \
        --ignore-not-found
    kubectl_retry delete \
        -f "${ROOK_EXAMPLES}/csi/nfs/storageclass.yaml" \
        --ignore-not-found
    kubectl_retry delete \
        -f "${ROOK_EXAMPLES}/csi/cephfs/snapshotclass.yaml" \
        --ignore-not-found
    kubectl_retry delete \
        -f "${ROOK_EXAMPLES}/csi/cephfs/storageclass.yaml" \
        --ignore-not-found
    kubectl_retry delete \
        -f "${ROOK_EXAMPLES}/subvolumegroup.yaml" \
        --ignore-not-found
    kubectl_retry delete \
        -f "${ROOK_EXAMPLES}/csi/rbd/snapshotclass.yaml" \
        --ignore-not-found
    kubectl_retry delete \
        -f "${ROOK_EXAMPLES}/csi/rbd/storageclass-test.yaml" \
        --ignore-not-found
    kubectl_retry delete \
        -f "${ROOK_EXAMPLES}/nfs-test.yaml" \
        --ignore-not-found
    kubectl_retry delete \
        -f "${ROOK_EXAMPLES}/filesystem-test.yaml" \
        --ignore-not-found
    kubectl_retry delete \
        -f "${ROOK_EXAMPLES}/pool-test.yaml" \
        --ignore-not-found
    kubectl_retry delete \
        -f "${ROOK_EXAMPLES}/cluster-test.yaml" \
        --ignore-not-found
    kubectl_retry delete \
        -f "${ROOK_EXAMPLES}/csi-operator.yaml" \
        --ignore-not-found
    echo "Rook Ceph cleanup complete"
}

case "${1:-}" in
install)
    install
    ;;
cleanup)
    cleanup
    ;;
*)
    echo "usage:" >&2
    echo "  $0 install" >&2
    echo "  $0 cleanup" >&2
    ;;
esac
