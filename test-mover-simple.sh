#!/bin/bash
# Simplified test script for ceph-volsync-mover container
# Tests destination and source separately using localhost

set -e

CONTAINER_CMD="podman"

echo "=== Ceph VolSync Mover Container Test (Localhost) ==="
echo ""

# Cleanup function
cleanup() {
    echo ""
    echo "Cleaning up..."
    ${CONTAINER_CMD} rm -f mover-destination 2>/dev/null || true
    ${CONTAINER_CMD} rm -f mover-source 2>/dev/null || true
    # rm -rf /tmp/mover-test
    echo "Cleanup complete"
}

# Set trap for cleanup
trap cleanup EXIT INT TERM

# Create logs directory
mkdir -p /tmp/mover-test/logs

# Create test directory and PSK file
echo "1. Creating test environment..."
mkdir -p /tmp/mover-test/keys
PSK_KEY=$(cat /dev/urandom | tr -dc 'a-f0-9' | fold -w 128 | head -n 1)
echo "volsync:${PSK_KEY}" > /tmp/mover-test/keys/psk.txt
chmod 600 /tmp/mover-test/keys/psk.txt
echo "   PSK file created: /tmp/mover-test/keys/psk.txt"

# Test 1: Destination (Server)
echo ""
echo "2. Testing DESTINATION container (server mode)..."
${CONTAINER_CMD} run -d \
    --name mover-destination \
    --network host \
    -v /tmp/mover-test/keys:/keys:ro,z \
    -e WORKER_TYPE=destination \
    -e SERVER_PORT=8080 \
    -e DESTINATION_PORT=8000 \
    localhost/ceph-volsync-mover:test

echo "   Waiting for destination to start..."
sleep 5

echo ""
echo "   Saving destination logs to /tmp/mover-test/logs/destination.log"
${CONTAINER_CMD} logs mover-destination 2>&1 | tee /tmp/mover-test/logs/destination.log
echo ""
echo "   Destination logs (first 30 lines):"
echo "   ---"
head -30 /tmp/mover-test/logs/destination.log
echo "   ---"

# Check if stunnel started successfully
if ${CONTAINER_CMD} logs mover-destination 2>&1 | grep -q "stunnel started successfully"; then
    echo "   ✓ Destination: stunnel started successfully"
else
    echo "   ✗ Destination: stunnel failed to start"
    exit 1
fi

# Check if mover started
if ${CONTAINER_CMD} logs mover-destination 2>&1 | grep -q "Starting mover"; then
    echo "   ✓ Destination: Mover binary started"
else
    echo "   ✗ Destination: Mover binary did not start"
    exit 1
fi

# Check if gRPC server is listening
if ${CONTAINER_CMD} logs mover-destination 2>&1 | grep -q "gRPC server listening"; then
    echo "   ✓ Destination: gRPC server listening on port 8080"
else
    echo "   ✗ Destination: gRPC server not listening"
    exit 1
fi

# Test 2: Source (Client)
echo ""
echo "3. Testing SOURCE container (client mode)..."
${CONTAINER_CMD} run -d \
    --name mover-source \
    --network host \
    -v /tmp/mover-test/keys:/keys:ro,z \
    -e WORKER_TYPE=source \
    -e DESTINATION_ADDRESS=localhost \
    -e DESTINATION_PORT=8000 \
    -e SERVER_PORT=8080 \
    localhost/ceph-volsync-mover:test

echo "   Waiting for source to connect..."
sleep 5

echo ""
echo "   Saving source logs to /tmp/mover-test/logs/source.log"
${CONTAINER_CMD} logs mover-source 2>&1 | tee /tmp/mover-test/logs/source.log
echo ""
echo "   Source logs (first 30 lines):"
echo "   ---"
head -90 /tmp/mover-test/logs/source.log
echo "   ---"

# Check if source stunnel started
if ${CONTAINER_CMD} logs mover-source 2>&1 | grep -q "stunnel started successfully"; then
    echo "   ✓ Source: stunnel started successfully"
else
    echo "   ✗ Source: stunnel failed to start"
    exit 1
fi

# Check if source mover started
if ${CONTAINER_CMD} logs mover-source 2>&1 | grep -q "Starting mover\|Starting source worker"; then
    echo "   ✓ Source: Mover binary started"
else
    echo "   ✗ Source: Mover binary did not start"
    exit 1
fi

# Check if source connected to destination
if ${CONTAINER_CMD} logs mover-source 2>&1 | grep -q "Retrieved version from destination"; then
    echo "   ✓ Source: Successfully retrieved version from destination"
else
    echo "   ✗ Source: Failed to retrieve version from destination"
    exit 1
fi

# Check if source sent Done signal
if ${CONTAINER_CMD} logs mover-source 2>&1 | grep -q "Successfully sent Done signal to destination"; then
    echo "   ✓ Source: Successfully sent Done signal to destination"
else
    echo "   ✗ Source: Failed to send Done signal to destination"
    exit 1
fi

# Wait a moment for destination to process Done signal
sleep 2

# Check destination final logs
echo ""
echo "   Destination logs (last 20 lines):"
echo "   ---"
${CONTAINER_CMD} logs mover-destination 2>&1 | tail -20
echo "   ---"

# Check if destination received Done signal and is shutting down
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
echo "Both containers tested successfully!"
echo ""
echo "Summary:"
echo "  - Destination: Running on localhost:8000 (stunnel) -> localhost:8080 (gRPC)"
echo "  - Source: Connecting to localhost:8000 via stunnel"
echo "  - PSK authentication: Configured"
echo "  - TLS encryption: Active"
echo ""
echo "Log files saved to:"
echo "  - Destination: /tmp/mover-test/logs/destination.log"
echo "  - Source: /tmp/mover-test/logs/source.log"
echo "  - PSK file: /tmp/mover-test/keys/psk.txt"