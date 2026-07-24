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
	"sync"

	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/operationstate"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
)

type actorLock struct {
	mutex sync.Mutex
	refs  int
}

func (s *AteomHerder) lockActor(actorUID string) func() {
	s.actorLocksMu.Lock()
	if s.actorLocks == nil {
		s.actorLocks = make(map[string]*actorLock)
	}
	lock := s.actorLocks[actorUID]
	if lock == nil {
		lock = &actorLock{}
		s.actorLocks[actorUID] = lock
	}
	lock.refs++
	s.actorLocksMu.Unlock()

	lock.mutex.Lock()
	return func() {
		lock.mutex.Unlock()
		s.actorLocksMu.Lock()
		lock.refs--
		if lock.refs == 0 {
			delete(s.actorLocks, actorUID)
		}
		s.actorLocksMu.Unlock()
	}
}

func (s *AteomHerder) operationStore(actorUID string) operationstate.Store {
	if s.operationsDir != nil {
		return operationstate.Store{Dir: s.operationsDir(actorUID)}
	}
	return operationstate.Store{Dir: ateompath.ActorOperationsDir(actorUID, "atelet")}
}

func checkpointOperationID(req *ateletpb.CheckpointRequest) string {
	return operationstate.ID(
		"checkpoint",
		req.GetActorUid(),
		req.GetTargetAteomUid(),
		req.GetType().String(),
		checkpointPrefix(req.GetType(), req.GetLocalConfig(), req.GetExternalConfig()),
		req.GetScope().String(),
	)
}

func checkpointPrefix(checkpointType ateletpb.CheckpointType, local *ateletpb.LocalCheckpointConfiguration, external *ateletpb.ExternalCheckpointConfiguration) string {
	if checkpointType == ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL {
		return local.GetSnapshotPrefix()
	}
	return external.GetSnapshotUriPrefix()
}
