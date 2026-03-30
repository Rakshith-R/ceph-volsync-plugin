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
	"net"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"google.golang.org/grpc"

	versionv1 "github.com/RamenDR/ceph-volsync-plugin/internal/proto/version/v1"
)

func TestConnectToDestination_ImmediateSuccess(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	server := grpc.NewServer()
	versionv1.RegisterVersionServiceServer(server, &VersionServer{Version: "test-v1"})
	go func() { _ = server.Serve(lis) }()
	defer server.GracefulStop()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := ConnectToDestination(ctx, logr.Discard(), lis.Addr().String())
	if err != nil {
		t.Fatalf("ConnectToDestination failed: %v", err)
	}
	defer func() { _ = conn.Close() }()
}

func TestConnectToDestination_RetriesUntilServerReady(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	addr := lis.Addr().String()
	// Close the listener so initial attempts fail.
	_ = lis.Close()

	// Start the server after a delay so retry logic kicks in.
	go func() {
		time.Sleep(3 * time.Second)
		newLis, err := net.Listen("tcp", addr)
		if err != nil {
			return
		}
		server := grpc.NewServer()
		versionv1.RegisterVersionServiceServer(server, &VersionServer{Version: "delayed-v1"})
		_ = server.Serve(newLis)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := ConnectToDestination(ctx, logr.Discard(), addr)
	if err != nil {
		t.Fatalf("ConnectToDestination should have succeeded after retries: %v", err)
	}
	defer func() { _ = conn.Close() }()
}

func TestConnectToDestination_FailsOnTimeout(t *testing.T) {
	// Use an address that will never have a server.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	addr := lis.Addr().String()
	_ = lis.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := ConnectToDestination(ctx, logr.Discard(), addr)
	if err == nil {
		_ = conn.Close()
		t.Fatal("expected error when no server is available")
	}
	if ctx.Err() == nil {
		// The parent context shouldn't expire; the internal
		// ConnectionTimeout (60s) is longer. But we set a short
		// parent context to bound the test. The function should
		// fail when the parent context expires.
		t.Logf("error (expected): %v", err)
	}
}

func TestConnectToDestination_RespectsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	conn, err := ConnectToDestination(ctx, logr.Discard(), "127.0.0.1:1")
	if err == nil {
		_ = conn.Close()
		t.Fatal("expected error with cancelled context")
	}
}
