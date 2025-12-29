#!/bin/bash

# stunnel.sh - Setup stunnel and invoke mover with proper arguments
# This script configures stunnel based on worker type and invokes the mover

set -e

# Required environment variables
WORKER_TYPE="${WORKER_TYPE:-}"
DESTINATION_ADDRESS="${DESTINATION_ADDRESS:-}"
DESTINATION_PORT="${DESTINATION_PORT:-8000}"
SERVER_PORT="${SERVER_PORT:-8080}"

# Validate required parameters
if [[ -z "$WORKER_TYPE" ]]; then
    echo "Error: WORKER_TYPE environment variable is required (source|destination)"
    exit 1
fi

if [[ "$WORKER_TYPE" != "source" && "$WORKER_TYPE" != "destination" ]]; then
    echo "Error: WORKER_TYPE must be 'source' or 'destination', got: $WORKER_TYPE"
    exit 1
fi

# Check if PSK file exists
PSK_FILE="/keys/psk.txt"
if [[ ! -f "$PSK_FILE" ]]; then
    echo "Error: PSK file not found at $PSK_FILE"
    exit 1
fi

echo "Starting stunnel setup for worker type: $WORKER_TYPE"

# Create stunnel configuration directory
mkdir -p /tmp/stunnel
STUNNEL_CONF="/tmp/stunnel/stunnel.conf"
STUNNEL_PID="/tmp/stunnel/stunnel.pid"

# Base stunnel configuration
cat > "$STUNNEL_CONF" << EOF
; Global options
pid = $STUNNEL_PID
debug = 7
output = /tmp/stunnel.log

; Use PSK (Pre-Shared Key) authentication
ciphers = PSK
PSKsecrets = $PSK_FILE

EOF

if [[ "$WORKER_TYPE" == "destination" ]]; then
    echo "Configuring stunnel as server (destination)"
    
    # Destination acts as stunnel server
    cat >> "$STUNNEL_CONF" << EOF
[rsync-tls]
accept = $DESTINATION_PORT
connect = 127.0.0.1:$SERVER_PORT
EOF

    echo "Starting stunnel server on port $DESTINATION_PORT, forwarding to $SERVER_PORT"
    
elif [[ "$WORKER_TYPE" == "source" ]]; then
    echo "Configuring stunnel as client (source)"
    
    if [[ -z "$DESTINATION_ADDRESS" ]]; then
        echo "Error: DESTINATION_ADDRESS is required for source worker type"
        exit 1
    fi
    
    # Source acts as stunnel client
    cat >> "$STUNNEL_CONF" << EOF
[rsync-tls]
client = yes
accept = 127.0.0.1:8001
connect = $DESTINATION_ADDRESS:$DESTINATION_PORT
EOF

    echo "Starting stunnel client connecting to $DESTINATION_ADDRESS:$DESTINATION_PORT"
fi

# Start stunnel in background
echo "Starting stunnel with config:"
cat "$STUNNEL_CONF"
echo "---"

stunnel "$STUNNEL_CONF"

# Wait a moment for stunnel to start and write PID file
sleep 2

# Check if stunnel started successfully by checking PID file
if [[ ! -f "$STUNNEL_PID" ]]; then
    echo "Error: stunnel failed to start - PID file not created"
    cat /tmp/stunnel.log 2>/dev/null || echo "No stunnel log available"
    exit 1
fi

STUNNEL_PID_VAL=$(cat "$STUNNEL_PID")
if ! kill -0 $STUNNEL_PID_VAL 2>/dev/null; then
    echo "Error: stunnel process not running"
    cat /tmp/stunnel.log 2>/dev/null || echo "No stunnel log available"
    exit 1
fi

echo "stunnel started successfully with PID $STUNNEL_PID_VAL"

# Prepare mover arguments
MOVER_ARGS="--worker-type=$WORKER_TYPE --server-port=$SERVER_PORT"

if [[ "$WORKER_TYPE" == "source" && -n "$DESTINATION_ADDRESS" ]]; then
    # For source, we connect to local stunnel client port instead of direct destination
    MOVER_ARGS="$MOVER_ARGS --destination-address=127.0.0.1:8001"
fi

echo "Starting mover with arguments: $MOVER_ARGS"

# Function to cleanup on exit
cleanup() {
    echo "Cleaning up..."
    if kill -0 $STUNNEL_PID_VAL 2>/dev/null; then
        echo "Stopping stunnel (PID: $STUNNEL_PID_VAL)"
        kill $STUNNEL_PID_VAL
        wait $STUNNEL_PID_VAL 2>/dev/null || true
    fi
}

# Set trap to cleanup on script exit
trap cleanup EXIT INT TERM

# Start the mover and wait for it
exec /mover $MOVER_ARGS