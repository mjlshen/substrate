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

package operationstate

import (
	"bytes"
	"testing"
)

func TestIDDeterministic(t *testing.T) {
	gotA := ID("run", "actor", "worker")
	gotB := ID("run", "actor", "worker")
	if gotA != gotB {
		t.Fatalf("IDs differ for equivalent parts: %q != %q", gotA, gotB)
	}
	if gotA == ID("run", "actor", "other-worker") {
		t.Fatal("ID did not distinguish a different worker")
	}
}

func TestStoreRoundTripAndReplace(t *testing.T) {
	store := Store{Dir: t.TempDir()}
	id := ID("checkpoint", "actor", "snapshot")
	if got, err := store.Load(id); err != nil || got != nil {
		t.Fatalf("Load before Save = (%v, %v), want (nil, nil)", got, err)
	}
	prepared := Record{Phase: PhasePrepared, Data: []byte(`{"sandbox":"gvisor"}`)}
	if err := store.Save(id, prepared); err != nil {
		t.Fatal(err)
	}
	got, err := store.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	if got.Phase != PhasePrepared || !bytes.Equal(got.Data, prepared.Data) {
		t.Fatalf("prepared record = %#v, want %#v", got, prepared)
	}
	if err := store.Save(id, Record{Phase: PhaseCompleted, SnapshotFiles: []string{"checkpoint.img"}}); err != nil {
		t.Fatal(err)
	}
	got, err = store.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	if got.Phase != PhaseCompleted || len(got.SnapshotFiles) != 1 || got.SnapshotFiles[0] != "checkpoint.img" {
		t.Fatalf("completed record = %#v", got)
	}
}

func TestStoreRejectsInvalidID(t *testing.T) {
	store := Store{Dir: t.TempDir()}
	if _, err := store.Load("../escape"); err == nil {
		t.Fatal("Load accepted invalid ID")
	}
}
