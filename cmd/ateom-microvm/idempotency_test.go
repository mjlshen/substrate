//go:build linux

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

package main

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/agent-substrate/substrate/internal/operationstate"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
)

func TestCompletedCheckpointIsReplayed(t *testing.T) {
	const actorUID = "actor"
	root := t.TempDir()
	s := &AteomService{
		operationsDir: func(actorUID string) string { return filepath.Join(root, actorUID) },
	}
	ctx := context.Background()

	checkpointID := operationstate.ID("checkpoint", actorUID)
	wantFiles := []string{"config.json", "state.json", "memory-ranges"}
	if err := s.operationStore(actorUID).Save(checkpointID, operationstate.Record{
		Phase: operationstate.PhaseCompleted, SnapshotFiles: wantFiles,
	}); err != nil {
		t.Fatal(err)
	}
	got, err := s.CheckpointWorkload(ctx, &ateompb.CheckpointWorkloadRequest{
		ActorUid: actorUID, OperationId: checkpointID,
	})
	if err != nil {
		t.Fatalf("CheckpointWorkload replay: %v", err)
	}
	if len(got.GetSnapshotFiles()) != len(wantFiles) {
		t.Fatalf("snapshot files = %v, want %v", got.GetSnapshotFiles(), wantFiles)
	}
	for i := range wantFiles {
		if got.GetSnapshotFiles()[i] != wantFiles[i] {
			t.Fatalf("snapshot files = %v, want %v", got.GetSnapshotFiles(), wantFiles)
		}
	}
}
