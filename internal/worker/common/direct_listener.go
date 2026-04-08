/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package common

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"strconv"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	apiv1 "github.com/RamenDR/ceph-volsync-plugin/internal/proto/api/v1"
	versionv1 "github.com/RamenDR/ceph-volsync-plugin/internal/proto/version/v1"
)

// DirectTLSPort is the default port for the direct
// TLS gRPC listener.
const DirectTLSPort = "8081"

// DirectTLSListener manages a TLS-secured gRPC server
// for direct (non-stunnel) connections.
type DirectTLSListener struct {
	server   *grpc.Server
	listener net.Listener
	port     int
}

// StartDirectTLSListener creates a TLS gRPC server on
// DirectTLSPort, registers SyncService and VersionService,
// and starts serving in a background goroutine.
func StartDirectTLSListener(
	_ context.Context,
	tlsConfig *tls.Config,
	syncServer *SyncServer,
	opts ...grpc.ServerOption,
) (*DirectTLSListener, error) {
	opts = append(opts,
		grpc.Creds(credentials.NewTLS(tlsConfig)),
		grpc.MaxRecvMsgSize(MaxGRPCMessageSize),
		grpc.WriteBufferSize(4*1024*1024),
		grpc.ReadBufferSize(4*1024*1024),
	)
	grpcServer := grpc.NewServer(opts...)

	versionServer := &VersionServer{Version: "v1.0.0"}
	versionv1.RegisterVersionServiceServer(grpcServer, versionServer)
	apiv1.RegisterSyncServiceServer(grpcServer, syncServer)

	lis, err := net.Listen("tcp", ":"+DirectTLSPort)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on port %s: %w", DirectTLSPort, err)
	}

	port, err := parsePort(lis.Addr())
	if err != nil {
		_ = lis.Close()
		return nil, err
	}

	go grpcServer.Serve(lis) //nolint:errcheck // server errors surfaced via Stop

	return &DirectTLSListener{
		server:   grpcServer,
		listener: lis,
		port:     port,
	}, nil
}

// Port returns the listening port number.
func (d *DirectTLSListener) Port() int {
	return d.port
}

// Stop gracefully stops the direct TLS gRPC server.
func (d *DirectTLSListener) Stop() {
	d.server.GracefulStop()
}

// parsePort extracts the port number from a net.Addr.
func parsePort(addr net.Addr) (int, error) {
	_, portStr, err := net.SplitHostPort(addr.String())
	if err != nil {
		return 0, fmt.Errorf("parse listener address: %w", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return 0, fmt.Errorf("parse port number: %w", err)
	}
	return port, nil
}

// CertExchangeHandler processes ExchangeCerts RPCs
// to establish direct TLS connections. It generates
// an ephemeral server certificate, pins the client's
// certificate fingerprint, and starts a direct TLS
// listener. The handler is idempotent -- subsequent
// calls return the existing certificate and port.
type CertExchangeHandler struct {
	mu             sync.Mutex
	ServerCtx      context.Context
	serverCert     *EphemeralCert
	directListener *DirectTLSListener
	SyncServer     *SyncServer
}

// ExchangeCerts generates an ephemeral server certificate,
// starts a direct TLS listener pinned to the client's
// certificate, and returns the server certificate PEM and
// direct TLS port.
func (h *CertExchangeHandler) ExchangeCerts(
	_ context.Context,
	req *apiv1.ExchangeCertsRequest,
) (*apiv1.ExchangeCertsResponse, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Idempotent: return existing cert if already started.
	if h.directListener != nil {
		return &apiv1.ExchangeCertsResponse{
			ServerCertPem: h.serverCert.CertPEM,
			DirectTlsPort: int32(h.directListener.Port()), //nolint:gosec // port fits int32
		}, nil
	}

	serverCert, err := GenerateEphemeralCert()
	if err != nil {
		return nil, fmt.Errorf("generate server cert: %w", err)
	}

	tlsConfig, err := NewServerTLSConfig(serverCert, req.ClientCertPem)
	if err != nil {
		return nil, fmt.Errorf("create server TLS config: %w", err)
	}

	// Use the long-lived server context, not the per-RPC ctx.
	dl, err := StartDirectTLSListener(h.ServerCtx, tlsConfig, h.SyncServer)
	if err != nil {
		return nil, fmt.Errorf("start direct TLS listener: %w", err)
	}

	h.serverCert = serverCert
	h.directListener = dl

	return &apiv1.ExchangeCertsResponse{
		ServerCertPem: serverCert.CertPEM,
		DirectTlsPort: int32(dl.Port()), //nolint:gosec // port fits int32
	}, nil
}

// StopDirectTLS stops the direct TLS listener if running.
func (h *CertExchangeHandler) StopDirectTLS() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.directListener != nil {
		h.directListener.Stop()
	}
}
