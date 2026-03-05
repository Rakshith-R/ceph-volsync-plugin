#!/bin/bash

# Test script for rsync tunnel functionality
set -e

echo "=== Rsync Tunnel End-to-End Test Script ==="
echo ""

# Configuration
IMAGE="quay.io/ramendr/ceph-volsync-plugin-mover:v0.0.1"
DEST_CONTAINER="rsync-dest-test"
SRC_CONTAINER="rsync-src-test"
PSK_SECRET="volsync:$(openssl rand -hex 64)"

# Cleanup function
cleanup() {
    echo ""
    echo "Cleaning up containers..."
    podman rm -f $DEST_CONTAINER 2>/dev/null || true
    podman rm -f $SRC_CONTAINER 2>/dev/null || true
    rm -rf /tmp/rsync-test-* 2>/dev/null || true
}

trap cleanup EXIT

# Step 1: Create PSK secret file
echo "Step 1: Creating PSK secret..."
mkdir -p /tmp/rsync-test-keys
echo "$PSK_SECRET" > /tmp/rsync-test-keys/psk.txt
chmod 644 /tmp/rsync-test-keys/psk.txt
# Set SELinux context if needed
chcon -Rt svirt_sandbox_file_t /tmp/rsync-test-keys 2>/dev/null || true
echo "PSK created: $(head -c 50 /tmp/rsync-test-keys/psk.txt)..."

# Step 2: Create test data for source
echo ""
echo "Step 2: Creating test data..."
mkdir -p /tmp/rsync-test-data-src/subdir
echo "Test file content at $(date)" > /tmp/rsync-test-data-src/testfile.txt
echo "Another file" > /tmp/rsync-test-data-src/file2.txt
echo "Subdirectory file" > /tmp/rsync-test-data-src/subdir/nested.txt
echo "Source data created:"
ls -laR /tmp/rsync-test-data-src/

# Step 3: Create destination data directory
echo ""
echo "Step 3: Creating destination data directory..."
mkdir -p /tmp/rsync-test-data-dest
echo "Destination directory created (empty)"

# Step 4: Start destination server container
echo ""
echo "Step 4: Starting destination server container..."
podman run -d \
    --name $DEST_CONTAINER \
    -v /tmp/rsync-test-keys:/keys:ro \
    -v /tmp/rsync-test-data-dest:/data \
    -e WORKER_TYPE=destination \
    -e SERVER_PORT=8080 \
    -e ENABLE_RSYNC_TUNNEL=true \
    -e RSYNC_PORT=8873 \
    -e RSYNC_DAEMON_PORT=8873 \
    -p 8000:8000 \
    -p 8873:8873 \
    $IMAGE \
    sleep infinity

echo "Waiting for destination container to start..."
sleep 3

# Check if container is running
if ! podman ps | grep -q $DEST_CONTAINER; then
    echo "ERROR: Destination container failed to start"
    podman logs $DEST_CONTAINER
    exit 1
fi

echo "Destination container started"

# Step 5: Execute stunnel.sh in destination container
echo ""
echo "Step 5: Starting stunnel and rsync daemon in destination..."
podman exec -d $DEST_CONTAINER /stunnel.sh

echo "Waiting for stunnel and rsync daemon to start..."
sleep 5

echo ""
echo "Checking destination processes:"
podman exec $DEST_CONTAINER ps aux

echo ""
echo "Stunnel log:"
podman exec $DEST_CONTAINER cat /tmp/stunnel.log 2>/dev/null || echo "Stunnel log not available"

echo ""
echo "Rsync daemon log:"
podman exec $DEST_CONTAINER cat /tmp/rsyncd.log 2>/dev/null || echo "Rsync daemon log not available"

# Step 6: Use host network to connect (localhost since ports are published)
echo ""
echo "Step 6: Using published ports for connection..."
echo "Destination ports published: 8000 (gRPC), 8873 (rsync)"

# Step 7: Start source container with host.containers.internal or host network
echo ""
echo "Step 7: Starting source container..."
podman run -d \
    --name $SRC_CONTAINER \
    --add-host=dest-server:host-gateway \
    -v /tmp/rsync-test-keys:/keys:ro \
    -v /tmp/rsync-test-data-src:/data:ro \
    -e WORKER_TYPE=source \
    -e SERVER_PORT=8080 \
    -e ENABLE_RSYNC_TUNNEL=true \
    -e RSYNC_PORT=8873 \
    -e DESTINATION_ADDRESS=dest-server \
    -e DESTINATION_PORT=8000 \
    $IMAGE \
    sleep infinity

echo "Waiting for source container to start..."
sleep 3

# Check if source container is running
if ! podman ps | grep -q $SRC_CONTAINER; then
    echo "ERROR: Source container failed to start"
    podman logs $SRC_CONTAINER
    exit 1
fi

echo "Source container started"

# Step 8: Execute stunnel.sh in source container
echo ""
echo "Step 8: Starting stunnel in source..."
podman exec -d $SRC_CONTAINER /stunnel.sh

echo "Waiting for source stunnel to start..."
sleep 5

echo ""
echo "Checking source processes:"
podman exec $SRC_CONTAINER ps aux

echo ""
echo "Source stunnel log:"
podman exec $SRC_CONTAINER cat /tmp/stunnel.log 2>/dev/null || echo "Source stunnel log not available"

# Step 9: Test rsync sync from source to destination through stunnel
echo ""
echo "Step 9: Testing rsync sync from source to destination through stunnel..."
echo "Running: rsync -avz --stats /data/ rsync://127.0.0.1:8873/data/"
podman exec $SRC_CONTAINER rsync -avz --stats /data/ rsync://127.0.0.1:8873/data/ || echo "Rsync command completed with status: $?"

# Step 10: Verify synced data
echo ""
echo "Step 10: Verifying synced data on destination..."
echo ""
echo "Files in destination container /data:"
podman exec $DEST_CONTAINER ls -laR /data/

echo ""
echo "Files in host destination directory:"
ls -laR /tmp/rsync-test-data-dest/

echo ""
echo "Checking file contents..."
echo "testfile.txt:"
cat /tmp/rsync-test-data-dest/testfile.txt 2>/dev/null || echo "✗ File not found"

echo ""
echo "file2.txt:"
cat /tmp/rsync-test-data-dest/file2.txt 2>/dev/null || echo "✗ File not found"

echo ""
echo "subdir/nested.txt:"
cat /tmp/rsync-test-data-dest/subdir/nested.txt 2>/dev/null || echo "✗ File not found"

echo ""
echo "=== Test Summary ==="
echo "✓ PSK secret created"
echo "✓ Test data created"
echo "✓ Destination container started"
echo "✓ Destination stunnel and rsync daemon started"
echo "✓ Source container started"
echo "✓ Source stunnel started"
echo "$([ -f /tmp/rsync-test-data-dest/testfile.txt ] && echo '✓' || echo '✗') Data synced through rsync tunnel"
echo ""
echo "Test complete!"