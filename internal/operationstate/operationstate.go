// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package operationstate persists lifecycle RPC progress across retries.
package operationstate

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// Phase is the durable progress of an operation.
type Phase string

const (
	// PhasePrepared means all destructive local preparation is complete and
	// must not be repeated after an ambiguous downstream RPC result.
	PhasePrepared Phase = "prepared"
	// PhaseCompleted means the requested state transition and its durable
	// publication completed. Replays return the recorded result.
	PhaseCompleted Phase = "completed"
)

// Record is the durable result of one operation.
type Record struct {
	Phase         Phase           `json:"phase"`
	SnapshotFiles []string        `json:"snapshotFiles,omitempty"`
	Data          json.RawMessage `json:"data,omitempty"`
}

// ID returns a stable, filesystem-safe identity for semantic operation parts.
// Callers intentionally omit mutable request details (for example refreshed
// secrets) and include the workflow identity such as actor, worker, and
// snapshot prefix.
func ID(parts ...string) string {
	sum := sha256.New()
	for _, part := range parts {
		_, _ = sum.Write([]byte(part))
		_, _ = sum.Write([]byte{0})
	}
	return hex.EncodeToString(sum.Sum(nil))
}

// Store reads and atomically writes operation records in one directory.
type Store struct {
	Dir string
}

// Load returns nil when the operation has no record yet.
func (s Store) Load(id string) (*Record, error) {
	path, err := s.path(id)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("while reading operation record: %w", err)
	}
	var record Record
	if err := json.Unmarshal(data, &record); err != nil {
		return nil, fmt.Errorf("while parsing operation record: %w", err)
	}
	if record.Phase != PhasePrepared && record.Phase != PhaseCompleted {
		return nil, fmt.Errorf("operation record has invalid phase %q", record.Phase)
	}
	return &record, nil
}

// Save atomically publishes an operation record.
func (s Store) Save(id string, record Record) error {
	path, err := s.path(id)
	if err != nil {
		return err
	}
	if record.Phase != PhasePrepared && record.Phase != PhaseCompleted {
		return fmt.Errorf("invalid operation phase %q", record.Phase)
	}
	if err := os.MkdirAll(s.Dir, 0o700); err != nil {
		return fmt.Errorf("while creating operation directory: %w", err)
	}
	data, err := json.Marshal(&record)
	if err != nil {
		return fmt.Errorf("while marshaling operation record: %w", err)
	}
	tmp, err := os.CreateTemp(s.Dir, ".operation-*.tmp")
	if err != nil {
		return fmt.Errorf("while creating temporary operation record: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("while writing operation record: %w", err)
	}
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("while setting operation record permissions: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("while syncing operation record: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("while closing operation record: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("while publishing operation record: %w", err)
	}
	dir, err := os.Open(s.Dir)
	if err != nil {
		return fmt.Errorf("while opening operation directory: %w", err)
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("while syncing operation directory: %w", err)
	}
	return nil
}

func (s Store) path(id string) (string, error) {
	if len(id) != sha256.Size*2 {
		return "", fmt.Errorf("invalid operation id %q", id)
	}
	if _, err := hex.DecodeString(id); err != nil {
		return "", fmt.Errorf("invalid operation id %q: %w", id, err)
	}
	return filepath.Join(s.Dir, id+".json"), nil
}
