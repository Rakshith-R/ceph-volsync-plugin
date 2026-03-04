#!/bin/bash
# Test script for ceph-volsync-mover with rsync functionality
# Tests both destination (server) and source (client) modes with rsync data transfer

set -e

# Use podman instead of docker
CONTAINER_CMD="podman"

echo "=== Ceph VolSync Mover Container Test with Rsync ==="
echo ""

# Cleanup function
cleanup() {
    echo ""
    echo "Cleaning up..."
    ${CONTAINER_CMD} rm -f mover-destination 2>/dev/null || true
    ${CONTAINER_CMD} rm -f mover-source 2>/dev/null || true
    ${CONTAINER_CMD} network rm mover-test-net 2>/dev/null || true
    rm -rf /tmp/mover-test
    echo "Cleanup complete"
}

# Set trap for cleanup
trap cleanup EXIT INT TERM

# Create test directory and PSK file
echo "1. Creating test environment..."
mkdir -p /tmp/mover-test/keys
mkdir -p /tmp/mover-test/source-data
mkdir -p /tmp/mover-test/dest-data

# Generate a simple PSK (in production, use proper random generation)
PSK_KEY=$(cat /dev/urandom | tr -dc 'a-f0-9' | fold -w 128 | head -n 1)
echo "volsync:${PSK_KEY}" > /tmp/mover-test/keys/psk.txt
chmod 600 /tmp/mover-test/keys/psk.txt
echo "   PSK file created: /tmp/mover-test/keys/psk.txt"

# Create test data on source
echo "   Creating test data..."
echo "test file 1" > /tmp/mover-test/source-data/file1.txt
echo "test file 2" > /tmp/mover-test/source-data/file2.txt
mkdir -p /tmp/mover-test/source-data/subdir
echo "test file 3" > /tmp/mover-test/source-data/subdir/file3.txt
echo "   Test data created in /tmp/mover-test/source-data"

# Create Podman network
echo ""
echo "2. Creating Podman network..."
${CONTAINER_CMD} network create mover-test-net
echo "   Network 'mover-test-net' created"

# Start destination (server) container with rsync enabled
echo ""
echo "3. Starting DESTINATION container (server mode with rsync)..."
${CONTAINER_CMD} run -d \
    --name mover-destination \
    --network mover-test-net \
    -v /tmp/mover-test/keys:/keys:ro,z \
    -v /tmp/mover-test/dest-data:/data:z \
    -e WORKER_TYPE=destination \
    -e SERVER_PORT=8080 \
    -e DESTINATION_PORT=8000 \
    -e ENABLE_RSYNC_TUNNEL=true \
    -e RSYNC_PORT=8873 \
    -p 8000:8000 \
    -p 8873:8873 \
    localhost/ceph-volsync-mover:test

echo "   Waiting for destination to start..."
sleep 5

# Check destination logs
echo ""
echo "4. Destination container logs:"
echo "   ---"
${CONTAINER_CMD} logs mover-destination 2>&1 | head -40
echo "   ---"

# Check if stunnel and rsync daemon started successfully
if ${CONTAINER_CMD} logs mover-destination 2>&1 | grep -q "stunnel started successfully"; then
    echo "   ✓ Destination: stunnel configured successfully"
else
    echo "   ✗ Destination: stunnel configuration failed"
    ${CONTAINER_CMD} logs mover-destination
    exit 1
fi

if ${CONTAINER_CMD} logs mover-destination 2>&1 | grep -q "rsync daemon started successfully"; then
    echo "   ✓ Destination: rsync daemon started successfully"
else
    echo "   ✗ Destination: rsync daemon not started"
fi

if ${CONTAINER_CMD} logs mover-destination 2>&1 | grep -q "gRPC server listening"; then
    echo "   ✓ Destination: gRPC server is listening"
else
    echo "   ✗ Destination: gRPC server not listening"
fi

# Use container name for DNS resolution within the network
DEST_ADDRESS="mover-destination"
echo "   Destination address: $DEST_ADDRESS"

# Start source (client) container with rsync enabled
echo ""
echo "5. Starting SOURCE container (client mode with rsync)..."
${CONTAINER_CMD} run -d \
    --name mover-source \
    --network mover-test-net \
    -v /tmp/mover-test/keys:/keys:ro,z \
    -v /tmp/mover-test/source-data:/data:ro,z \
    -e WORKER_TYPE=source \
    -e DESTINATION_ADDRESS=${DEST_ADDRESS} \
    -e DESTINATION_PORT=8000 \
    -e SERVER_PORT=8080 \
    -e ENABLE_RSYNC_TUNNEL=true \
    -e RSYNC_PORT=8873 \
    localhost/ceph-volsync-mover:test

echo "   Waiting for source to start and complete rsync..."
sleep 10

# Check source logs
echo ""
echo "6. Source container logs:"
echo "   ---"
${CONTAINER_CMD} logs mover-source 2>&1
echo "   ---"

# Check if source is running or completed
SOURCE_STATUS=$(${CONTAINER_CMD} inspect -f '{{.State.Status}}' mover-source)
echo "   Source container status: $SOURCE_STATUS"

# Check final logs
echo ""
echo "7. Final status check:"
echo ""
echo "   Destination logs (last 40 lines):"
echo "   ---"
${CONTAINER_CMD} logs mover-destination 2>&1 | tail -40
echo "   ---"

# Check for successful connection and rsync
echo ""
echo "8. Verification:"
if ${CONTAINER_CMD} logs mover-destination 2>&1 | grep -q "stunnel started successfully"; then
    echo "   ✓ Destination: stunnel started successfully"
else
    echo "   ✗ Destination: stunnel may not have started"
fi

if ${CONTAINER_CMD} logs mover-destination 2>&1 | grep -q "gRPC server listening"; then
    echo "   ✓ Destination: gRPC server is listening"
else
    echo "   ✗ Destination: gRPC server not detected"
fi

if ${CONTAINER_CMD} logs mover-source 2>&1 | grep -q "stunnel started successfully"; then
    echo "   ✓ Source: stunnel started successfully"
else
    echo "   ✗ Source: stunnel may not have started"
fi

if ${CONTAINER_CMD} logs mover-source 2>&1 | grep -q "Retrieved version from destination"; then
    echo "   ✓ Source: Successfully connected to destination and retrieved version"
else
    echo "   ✗ Source: Did not retrieve version from destination"
fi

if ${CONTAINER_CMD} logs mover-source 2>&1 | grep -q "Starting rsync synchronization"; then
    echo "   ✓ Source: Started rsync synchronization"
else
    echo "   ✗ Source: Did not start rsync synchronization"
fi

if ${CONTAINER_CMD} logs mover-source 2>&1 | grep -q "Rsync completed"; then
    echo "   ✓ Source: Rsync completed"
else
    echo "   ✗ Source: Rsync did not complete"
fi

if ${CONTAINER_CMD} logs mover-source 2>&1 | grep -q "Successfully sent Done signal to destination"; then
    echo "   ✓ Source: Successfully sent Done signal to destination"
else
    echo "   ✗ Source: Did not send Done signal to destination"
fi

# Check if data was transferred
echo ""
echo "9. Data verification:"
echo "   Checking transferred files..."
if [ -f /tmp/mover-test/dest-data/file1.txt ]; then
    echo "   ✓ file1.txt transferred"
else
    echo "   ✗ file1.txt not found"
fi

if [ -f /tmp/mover-test/dest-data/file2.txt ]; then
    echo "   ✓ file2.txt transferred"
else
    echo "   ✗ file2.txt not found"
fi

if [ -f /tmp/mover-test/dest-data/subdir/file3.txt ]; then
    echo "   ✓ subdir/file3.txt transferred"
else
    echo "   ✗ subdir/file3.txt not found"
fi

# Verify content
if [ -f /tmp/mover-test/dest-data/file1.txt ]; then
    CONTENT=$(cat /tmp/mover-test/dest-data/file1.txt)
    if [ "$CONTENT" == "test file 1" ]; then
        echo "   ✓ file1.txt content verified"
    else
        echo "   ✗ file1.txt content mismatch"
    fi
fi

if ${CONTAINER_CMD} logs mover-destination 2>&1 | grep -q "Destination worker shutting down after Done request"; then
    echo "   ✓ Destination: Received Done signal and shutting down gracefully"
    echo ""
    echo "=== TEST PASSED ==="
else
    echo "   ℹ Destination: Done signal status unclear"
    echo ""
    echo "=== TEST COMPLETED ==="
fi

echo ""
echo "Test completed!"
