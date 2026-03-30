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

package pipeline

import "testing"

func TestConfig_SetDefaults_ZeroValue(t *testing.T) {
	cfg := &Config{}
	cfg.SetDefaults()
	if cfg.ChunkSize != 4*1024*1024 {
		t.Fatalf("ChunkSize: got %d", cfg.ChunkSize)
	}
	if cfg.ReadWorkers != 8 {
		t.Fatalf("ReadWorkers: got %d", cfg.ReadWorkers)
	}
	if cfg.MaxWindow != 64 {
		t.Fatalf("MaxWindow: got %d", cfg.MaxWindow)
	}
	if cfg.HashSendWorkers != 2 {
		t.Fatalf("HashSendWorkers: got %d", cfg.HashSendWorkers)
	}
	if cfg.DataSendWorkers != 4 {
		t.Fatalf("DataSendWorkers: got %d", cfg.DataSendWorkers)
	}
}

func TestConfig_SetDefaults_Preserves(t *testing.T) {
	cfg := &Config{ReadWorkers: 16}
	cfg.SetDefaults()
	if cfg.ReadWorkers != 16 {
		t.Fatalf("override lost: got %d", cfg.ReadWorkers)
	}
}

func TestConfig_Validate_ChunkSizeTooSmall(t *testing.T) {
	cfg := &Config{ChunkSize: 1024}
	cfg.SetDefaults()
	cfg.ChunkSize = 1024
	if err := cfg.validate(); err == nil {
		t.Fatal("expected error for small ChunkSize")
	}
}

func TestConfig_Validate_MaxWindowTooSmall(t *testing.T) {
	cfg := &Config{}
	cfg.SetDefaults()
	cfg.MaxWindow = 2
	if err := cfg.validate(); err == nil {
		t.Fatal("expected error for small MaxWindow")
	}
}

func TestConfig_Validate_ValidDefaults(t *testing.T) {
	cfg := &Config{}
	cfg.SetDefaults()
	if err := cfg.validate(); err != nil {
		t.Fatalf("defaults should be valid: %v", err)
	}
}
