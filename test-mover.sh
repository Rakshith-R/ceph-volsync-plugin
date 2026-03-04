#!/bin/bash
# Test script for ceph-volsync-mover container
# Tests both destination (server) and source (client) modes

set -e

# Use podman instead of docker
CONTAINER_CMD="podman"

echo "=== Ceph VolSync Mover Container Test ==="
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
# Generate a simple PSK (in production, use proper random generation)
PSK_KEY=$(cat /dev/urandom | tr -dc 'a-f0-9' | fold -w 128 | head -n 1)
echo "volsync:${PSK_KEY}" > /tmp/mover-test/keys/psk.txt
chmod 600 /tmp/mover-test/keys/psk.txt
echo "   PSK file created: /tmp/mover-test/keys/psk.txt"

# Create Podman network
echo ""
echo "2. Creating Podman network..."
${CONTAINER_CMD} network create mover-test-net
echo "   Network 'mover-test-net' created"

# Start destination (server) container
echo ""
echo "3. Starting DESTINATION container (server mode)..."
${CONTAINER_CMD} run -d \
    --name mover-destination \
    --network mover-test-net \
    -v /tmp/mover-test/keys:/keys:ro,z \
    -e WORKER_TYPE=destination \
    -e SERVER_PORT=8080 \
    -e DESTINATION_PORT=8000 \
    -p 8000:8000 \
    localhost/ceph-volsync-mover:test

echo "   Waiting for destination to start..."
sleep 3

# Check destination logs
echo ""
echo "4. Destination container logs:"
echo "   ---"
${CONTAINER_CMD} logs mover-destination 2>&1 | head -30
echo "   ---"

# Check if stunnel started successfully
if ${CONTAINER_CMD} logs mover-destination 2>&1 | grep -q "stunnel started successfully"; then
    echo "   ✓ Destination: stunnel configured successfully"
else
    echo "   ✗ Destination: stunnel configuration failed"
    ${CONTAINER_CMD} logs mover-destination
    exit 1
fi

if ${CONTAINER_CMD} logs mover-destination 2>&1 | grep -q "gRPC server listening"; then
    echo "   ✓ Destination: gRPC server is listening"
else
    echo "   ✗ Destination: gRPC server not listening"
fi

# Use container name for DNS resolution within the network
DEST_ADDRESS="mover-destination"
echo "   Destination address: $DEST_ADDRESS"

# Start source (client) container
echo ""
echo "5. Starting SOURCE container (client mode)..."
${CONTAINER_CMD} run -d \
    --name mover-source \
    --network mover-test-net \
    -v /tmp/mover-test/keys:/keys:ro,z \
    -e WORKER_TYPE=source \
    -e DESTINATION_ADDRESS=${DEST_ADDRESS} \
    -e DESTINATION_PORT=8000 \
    -e SERVER_PORT=8080 \
    localhost/ceph-volsync-mover:test

echo "   Waiting for source to start..."
sleep 3

# Check source logs
echo ""
echo "6. Source container logs:"
echo "   ---"
${CONTAINER_CMD} logs mover-source 2>&1 | head -20
echo "   ---"

# Check if source is running or completed
SOURCE_STATUS=$(${CONTAINER_CMD} inspect -f '{{.State.Status}}' mover-source)
echo "   Source container status: $SOURCE_STATUS"

# Wait for mover to start and communicate
echo ""
echo "7. Waiting for mover startup and communication test..."
sleep 8

# Check final logs
echo ""
echo "8. Final status check:"
echo ""
echo "   Destination logs (last 30 lines):"
echo "   ---"
${CONTAINER_CMD} logs mover-destination 2>&1 | tail -30
echo "   ---"

echo ""
echo "   Source logs (last 30 lines):"
echo "   ---"
${CONTAINER_CMD} logs mover-source 2>&1 | tail -30
echo "   ---"

# Check for successful connection
echo ""
echo "9. Verification:"
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
    echo ""
    echo "=== TEST FAILED ==="
    exit 1
fi

if ${CONTAINER_CMD} logs mover-source 2>&1 | grep -q "Successfully sent Done signal to destination"; then
    echo "   ✓ Source: Successfully sent Done signal to destination"
else
    echo "   ✗ Source: Did not send Done signal to destination"
    echo ""
    echo "=== TEST FAILED ==="
    exit 1
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
echo "Test completed successfully!"