// Copyright (c) 2026 Gabriel Alonso
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package appstate

import (
	"bytes"
	"context"
	"testing"

	"google.golang.org/protobuf/proto"

	"go.mau.fi/whatsmeow/proto/waServerSync"
	"go.mau.fi/whatsmeow/proto/waSyncAction"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	waLog "go.mau.fi/whatsmeow/util/log"
)

type policyTestStore struct {
	store.NoopStore
	state HashState
	macs  map[string][]byte
}

func (s *policyTestStore) GetAppStateVersion(context.Context, string) (uint64, [128]byte, error) {
	return s.state.Version, s.state.Hash, nil
}
func (s *policyTestStore) PutAppStateVersion(_ context.Context, _ string, v uint64, h [128]byte) error {
	s.state = HashState{Version: v, Hash: h}
	return nil
}
func (s *policyTestStore) GetAppStateMutationMAC(_ context.Context, _ string, index []byte) ([]byte, error) {
	return s.macs[string(index)], nil
}
func (s *policyTestStore) PutAppStateMutationMACs(_ context.Context, _ string, _ uint64, macs []store.AppStateMutationMAC) error {
	for _, m := range macs {
		s.macs[string(m.IndexMAC)] = m.ValueMAC
	}
	return nil
}
func (s *policyTestStore) DeleteAppStateMutationMACs(_ context.Context, _ string, indices [][]byte) error {
	for _, index := range indices {
		delete(s.macs, string(index))
	}
	return nil
}
func (s *policyTestStore) GetAppStateSyncKey(context.Context, []byte) (*store.AppStateSyncKey, error) {
	return &store.AppStateSyncKey{Data: bytes.Repeat([]byte{7}, 32)}, nil
}

// REMOVE needs its own operation-bound content MAC. Construct it with the
// native helpers, keeping all authentication enabled without a reference codec.
func TestRegularLowAuthenticatedRemove(t *testing.T) {
	s := &policyTestStore{macs: map[string][]byte{}}
	proc := NewProcessor(&store.Device{AppState: s, AppStateKeys: s}, waLog.Noop)
	info := BuildPin(types.NewJID("12345", types.DefaultUserServer), true)
	info.Mutations = append(info.Mutations, MutationInfo{Index: []string{"future-domain", "opaque"}, Value: &waSyncAction.SyncActionValue{}})
	raw, err := proc.EncodePatch(t.Context(), []byte{1}, HashState{}, info)
	if err != nil {
		t.Fatal(err)
	}
	var patch waServerSync.SyncdPatch
	if err = proto.Unmarshal(raw, &patch); err != nil {
		t.Fatal(err)
	}
	snapshot := &waServerSync.SyncdSnapshot{Version: &waServerSync.SyncdVersion{Version: proto.Uint64(1)}, KeyID: patch.KeyID, Mac: patch.SnapshotMAC}
	for _, m := range patch.Mutations {
		snapshot.Records = append(snapshot.Records, m.Record)
	}
	session, err := NewRegularLowDecodeSession(proc, PatchSnapshotAllowMismatch)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = session.Decode(t.Context(), &PatchList{Name: WAPatchRegularLow, Snapshot: snapshot}); err != nil {
		t.Fatal(err)
	}
	remove := proto.Clone(patch.Mutations[0]).(*waServerSync.SyncdMutation)
	remove.Operation = waServerSync.SyncdMutation_REMOVE.Enum()
	keys, err := proc.getAppStateKey(t.Context(), remove.Record.KeyID.ID)
	if err != nil {
		t.Fatal(err)
	}
	value := remove.Record.Value.Blob
	content := value[:len(value)-32]
	remove.Record.Value.Blob = append(content, generateContentMAC(waServerSync.SyncdMutation_REMOVE, content, remove.Record.KeyID.ID, keys.ValueMAC)...)
	patch.Mutations = []*waServerSync.SyncdMutation{remove}
	patch.Version = &waServerSync.SyncdVersion{Version: proto.Uint64(2)}
	next := s.state
	next.Version++
	if _, err = next.updateHash(patch.Mutations, func(index []byte, _ int) ([]byte, error) {
		return s.GetAppStateMutationMAC(t.Context(), "regular_low", index)
	}); err != nil {
		t.Fatal(err)
	}
	patch.SnapshotMAC = next.generateSnapshotMAC(WAPatchRegularLow, keys.SnapshotMAC)
	patch.PatchMAC = generatePatchMAC(&patch, WAPatchRegularLow, keys.PatchMAC, 2)
	result, err := session.Decode(t.Context(), &PatchList{Name: WAPatchRegularLow, Patches: []*waServerSync.SyncdPatch{&patch}})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Mutations) != 1 || result.Mutations[0].Operation != waServerSync.SyncdMutation_REMOVE || s.state != next || len(s.macs) != 1 {
		t.Fatalf("REMOVE did not retain exactly the unsupported crypto record: %+v", result)
	}
	if _, ok := s.macs[string(snapshot.Records[1].Index.Blob)]; !ok {
		t.Fatal("unknown record was discarded")
	}
}
