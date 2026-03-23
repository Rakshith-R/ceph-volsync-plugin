/*
Copyright 2025.

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

import "time"

const (
	// ConnectionTimeout is the timeout for the first
	// RPC call which establishes the connection. This
	// is longer because it includes DNS resolution,
	// TCP handshake, and TLS negotiation.
	ConnectionTimeout = 60 * time.Second

	// RPCTimeout is the timeout for subsequent RPC
	// calls after connection is established.
	RPCTimeout = 30 * time.Second

	// WritePayloadMinSize is the minimum accumulated
	// data payload size before sending a WriteRequest
	// over the gRPC stream.
	WritePayloadMinSize = 2 * 1024 * 1024 // 2MB

	// WritePayloadMaxSize is the maximum accumulated
	// data payload size. Prevents exceeding the 4MB
	// gRPC default server max receive message size.
	WritePayloadMaxSize = 3 * 1024 * 1024 // 3MB

	// MaxGRPCMessageSize is the maximum gRPC message
	// size for send and receive.
	MaxGRPCMessageSize = 8 * 1024 * 1024 // 8MB

	// RsyncDaemonPort is the rsync daemon port for
	// CephFS mover workers.
	RsyncDaemonPort int32 = 8874

	// DefaultServerPort is the gRPC server listen
	// port inside the mover container.
	DefaultServerPort = "8080"
)
