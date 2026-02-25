#!/bin/bash -e

# This script can be used to install/delete VolSync CRDs
# VolSync provides ReplicationSource and ReplicationDestination custom resources

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

# shellcheck disable=SC1091
[ ! -e "${SCRIPT_DIR}"/utils.sh ] || source "${SCRIPT_DIR}"/utils.sh

VOLSYNC_VERSION=${VOLSYNC_VERSION:-"v0.14.0"}
BASE_URL="https://raw.githubusercontent.com/backube/volsync"
VOLSYNC_URL="${BASE_URL}/${VOLSYNC_VERSION}/config/crd/bases"

# VolSync CRDs
SRC="${VOLSYNC_URL}/volsync.backube_replicationsources.yaml"
DEST="${VOLSYNC_URL}/volsync.backube_replicationdestinations.yaml"

function install_volsync_crds() {
	create_or_delete_resource "create"
}

function cleanup_volsync_crds() {
	create_or_delete_resource "delete"
}

function create_or_delete_resource() {
	local operation=$1
	kubectl_retry "${operation}" -f "${SRC}"
	kubectl_retry "${operation}" -f "${DEST}"
}

case "${1:-}" in
install)
	install_volsync_crds
	;;
cleanup)
	cleanup_volsync_crds
	;;
*)
	echo "usage:" >&2
	echo "  $0 install" >&2
	echo "  $0 cleanup" >&2
	;;
esac
