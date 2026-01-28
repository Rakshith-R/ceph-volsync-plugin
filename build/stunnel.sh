#!/bin/bash

# stunnel.sh - Setup stunnel and invoke mover with proper arguments
# This script configures stunnel based on worker type and invokes the mover
# Supports dual tunnels: gRPC (always enabled) and rsync (optional)

set -e

# Required environment variables
WORKER_TYPE="${WORKER_TYPE:-}"
DESTINATION_ADDRESS="${DESTINATION_ADDRESS:-}"
DESTINATION_PORT="${DESTINATION_PORT:-8000}"
SERVER_PORT="${SERVER_PORT:-8080}"

# Optional rsync tunnel configuration
ENABLE_RSYNC_TUNNEL="${ENABLE_RSYNC_TUNNEL:-false}"
RSYNC_PORT="${RSYNC_PORT:-8873}"
RSYNC_DAEMON_PORT="${RSYNC_DAEMON_PORT:-8874}"

# ConfigMap configuration (optional)
CEPH_CSI_CONFIGMAP_NAME="${CEPH_CSI_CONFIGMAP_NAME:-}"
CEPH_CSI_CONFIGMAP_NAMESPACE="${CEPH_CSI_CONFIGMAP_NAMESPACE:-}"

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

# Normalize ENABLE_RSYNC_TUNNEL to true/false
case "${ENABLE_RSYNC_TUNNEL,,}" in
    true|1|yes|on)
        ENABLE_RSYNC_TUNNEL="true"
        ;;
    *)
        ENABLE_RSYNC_TUNNEL="false"
        ;;
esac

if [[ "$ENABLE_RSYNC_TUNNEL" == "true" ]]; then
    echo "Starting dual stunnel setup (gRPC + rsync) for worker type: $WORKER_TYPE"
else
    echo "Starting stunnel setup (gRPC only) for worker type: $WORKER_TYPE"
fi

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
    
    # Destination acts as stunnel server for gRPC
    cat >> "$STUNNEL_CONF" << EOF
; gRPC tunnel - for mover communication
[grpc-tls]
accept = $DESTINATION_PORT
connect = 127.0.0.1:$SERVER_PORT
EOF

    echo "  - gRPC tunnel on port $DESTINATION_PORT → 127.0.0.1:$SERVER_PORT"
    
    # Add rsync tunnel if enabled
    if [[ "$ENABLE_RSYNC_TUNNEL" == "true" ]]; then
        cat >> "$STUNNEL_CONF" << EOF

; Rsync tunnel - for rsync daemon communication
[rsync-tls]
accept = $RSYNC_PORT
connect = 127.0.0.1:$RSYNC_DAEMON_PORT
EOF
        echo "  - Rsync tunnel on port $RSYNC_PORT → 127.0.0.1:$RSYNC_DAEMON_PORT"
    fi
    
elif [[ "$WORKER_TYPE" == "source" ]]; then
    echo "Configuring stunnel as client (source)"
    
    if [[ -z "$DESTINATION_ADDRESS" ]]; then
        echo "Error: DESTINATION_ADDRESS is required for source worker type"
        exit 1
    fi
    
    # Source acts as stunnel client for gRPC
    cat >> "$STUNNEL_CONF" << EOF
; gRPC tunnel - for mover communication
[grpc-tls]
client = yes
accept = 127.0.0.1:8001
connect = $DESTINATION_ADDRESS:$DESTINATION_PORT
EOF

    echo "  - gRPC tunnel: 127.0.0.1:8001 → $DESTINATION_ADDRESS:$DESTINATION_PORT"
    
    # Add rsync tunnel if enabled
    if [[ "$ENABLE_RSYNC_TUNNEL" == "true" ]]; then
        cat >> "$STUNNEL_CONF" << EOF

; Rsync tunnel - for rsync daemon communication
[rsync-tls]
client = yes
accept = 127.0.0.1:$RSYNC_DAEMON_PORT
connect = $DESTINATION_ADDRESS:$RSYNC_PORT
EOF
        echo "  - Rsync tunnel: 127.0.0.1:$RSYNC_DAEMON_PORT → $DESTINATION_ADDRESS:$RSYNC_PORT"
    fi
fi

# Start stunnel in background
echo "Starting stunnel with config:"
cat "$STUNNEL_CONF"
echo "---"

stunnel "$STUNNEL_CONF"

# Wait a moment for stunnel to start and write PID file
sleep 2

# Function to check if a port is listening using bash's /dev/tcp
# (nc/netcat is not installed in the container)
check_port() {
    local host=$1
    local port=$2
    (echo > /dev/tcp/$host/$port) 2>/dev/null
}

# Wait for stunnel to be ready to accept connections
if [[ "$WORKER_TYPE" == "source" ]]; then
    echo "Waiting for stunnel client ports to be ready..."
    for i in {1..30}; do
        GRPC_READY=false
        RSYNC_READY=false
        
        # Always check gRPC port
        if check_port 127.0.0.1 8001; then
            GRPC_READY=true
        fi
        
        # Only check rsync port if rsync tunnel is enabled
        if [[ "$ENABLE_RSYNC_TUNNEL" == "true" ]]; then
            if check_port 127.0.0.1 8874; then
                RSYNC_READY=true
            fi
        else
            RSYNC_READY=true  # Not needed, so consider it ready
        fi
        
        if [[ "$GRPC_READY" == "true" && "$RSYNC_READY" == "true" ]]; then
            echo "Stunnel client ports are ready"
            break
        fi
        
        if [ $i -eq 30 ]; then
            echo "Error: Stunnel client ports not ready after 30 seconds"
            echo "gRPC port 8001 ready: $GRPC_READY"
            if [[ "$ENABLE_RSYNC_TUNNEL" == "true" ]]; then
                echo "Rsync port 8874 ready: $RSYNC_READY"
            fi
            cat /tmp/stunnel.log 2>/dev/null || echo "No stunnel log available"
            exit 1
        fi
        sleep 1
    done
fi

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

# Start rsync daemon on destination if rsync tunnel is enabled
RSYNC_PID_VAL=""
if [[ "$WORKER_TYPE" == "destination" && "$ENABLE_RSYNC_TUNNEL" == "true" ]]; then
    echo "Starting rsync daemon on port $RSYNC_DAEMON_PORT"
    
    # Create rsync configuration
    RSYNCD_CONF="/tmp/rsyncd.conf"
    cat > "$RSYNCD_CONF" << EOF
# Rsync daemon configuration
uid = root
gid = root
use chroot = no
max connections = 4
pid file = /tmp/rsyncd.pid
log file = /tmp/rsyncd.log
lock file = /tmp/rsyncd.lock

[data]
    path = /data
    comment = Data volume
    read only = false
    list = yes
    # No authentication - stunnel already provides PSK authentication
EOF

    # Start rsync daemon
    rsync --daemon --config="$RSYNCD_CONF" --port="$RSYNC_DAEMON_PORT" --no-detach &
    RSYNC_PID_VAL=$!
    
    # Wait a moment for rsync daemon to start
    sleep 2
    
    # Check if rsync daemon started successfully
    if ! kill -0 $RSYNC_PID_VAL 2>/dev/null; then
        echo "Error: rsync daemon failed to start"
        cat /tmp/rsyncd.log 2>/dev/null || echo "No rsync daemon log available"
        exit 1
    fi
    
    echo "rsync daemon started successfully with PID $RSYNC_PID_VAL"
fi

# Prepare mover arguments
MOVER_ARGS="--worker-type=$WORKER_TYPE --server-port=$SERVER_PORT"

if [[ "$WORKER_TYPE" == "source" && -n "$DESTINATION_ADDRESS" ]]; then
    # For source, we connect to local stunnel client port instead of direct destination
    MOVER_ARGS="$MOVER_ARGS --destination-address=127.0.0.1:8001"
fi

# Add ConfigMap arguments if provided
if [[ -n "$CEPH_CSI_CONFIGMAP_NAME" ]]; then
    MOVER_ARGS="$MOVER_ARGS --configmap-name=$CEPH_CSI_CONFIGMAP_NAME"
fi

if [[ -n "$CEPH_CSI_CONFIGMAP_NAMESPACE" ]]; then
    MOVER_ARGS="$MOVER_ARGS --configmap-namespace=$CEPH_CSI_CONFIGMAP_NAMESPACE"
fi

echo "Starting mover with arguments: $MOVER_ARGS"

# Function to cleanup on exit
cleanup() {
    echo "Cleaning up..."
    
    # Stop rsync daemon if it's running
    if [[ -n "$RSYNC_PID_VAL" ]] && kill -0 $RSYNC_PID_VAL 2>/dev/null; then
        echo "Stopping rsync daemon (PID: $RSYNC_PID_VAL)"
        kill $RSYNC_PID_VAL
        wait $RSYNC_PID_VAL 2>/dev/null || true
    fi
    
    # Stop stunnel
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