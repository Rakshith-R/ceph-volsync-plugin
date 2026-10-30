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
	"net"
	"testing"
)

func TestLoopbackListenAddr(t *testing.T) {
	t.Parallel()

	got := loopbackListenAddr(DefaultServerPort)
	want := "127.0.0.1:" + DefaultServerPort
	if got != want {
		t.Fatalf("loopbackListenAddr(%q) = %q, want %q",
			DefaultServerPort, got, want)
	}
}

func TestLoopbackListenAddr_BindsLoopbackOnly(t *testing.T) {
	t.Parallel()

	lis, err := net.Listen("tcp", loopbackListenAddr("0"))
	if err != nil {
		t.Fatalf("net.Listen() error: %v", err)
	}
	defer func() { _ = lis.Close() }()

	tcpAddr, ok := lis.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("listener addr type = %T, want *net.TCPAddr", lis.Addr())
	}
	if !tcpAddr.IP.IsLoopback() {
		t.Errorf("listener bound to %v, want loopback", tcpAddr.IP)
	}
}
