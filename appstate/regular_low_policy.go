// Copyright (c) 2026 Gabriel Alonso
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package appstate

import (
	"context"
	"fmt"

	"go.mau.fi/whatsmeow/proto/waServerSync"
)

// PatchSnapshotPolicy controls only regular_low patch SnapshotMAC enforcement.
// Content, decryption, index, PatchMAC and the initial snapshot remain mandatory.
type PatchSnapshotPolicy uint8

const (
	PatchSnapshotStrict PatchSnapshotPolicy = iota
	PatchSnapshotAllowMismatch
)

// PatchIntegrity records accepted patches only. Degraded never resets during a
// session, even if a later patch has a valid SnapshotMAC. It is not readiness.
type PatchIntegrity struct {
	SnapshotVerified      bool
	Degraded              bool
	SnapshotMACMismatches int
}

type SessionDecodeResult struct {
	Mutations []Mutation
	State     HashState
	Integrity PatchIntegrity
}

// RegularLowDecodeSession is anchored by one verified snapshot. The caller must
// supply an exclusively owned EMPTY store. The opt-in Client wrapper owns that
// lifecycle; direct session use does not connect, emit events, declare readiness
// or provide recovery. The caller must serialize calls with all store access.
// Persistence uses the existing non-atomic storeMACs contract. A store error can
// leave partial writes; any error permanently closes this session to further use.
type RegularLowDecodeSession struct {
	proc      *Processor
	policy    PatchSnapshotPolicy
	state     HashState
	integrity PatchIntegrity
	failed    bool
}

func NewRegularLowDecodeSession(proc *Processor, policy PatchSnapshotPolicy) (*RegularLowDecodeSession, error) {
	if proc == nil || proc.Store == nil || proc.Store.AppState == nil || proc.Store.AppStateKeys == nil || (policy != PatchSnapshotStrict && policy != PatchSnapshotAllowMismatch) {
		return nil, fmt.Errorf("invalid regular_low decode policy")
	}
	return &RegularLowDecodeSession{proc: proc, policy: policy}, nil
}

// Decode verifies every operation before the first write for its patch. It
// advances the complete cryptographic map, including unknown action domains.
// A rejected patch contributes no returned mutations or successful observation.
func (s *RegularLowDecodeSession) Decode(ctx context.Context, list *PatchList) (result SessionDecodeResult, err error) {
	defer func() {
		result.State, result.Integrity = s.state, s.integrity
		if err != nil {
			s.failed = true
		}
	}()
	if s.failed || list == nil || list.Name != WAPatchRegularLow {
		return result, fmt.Errorf("invalid regular_low decode session")
	}
	if err = ctx.Err(); err != nil {
		return
	}
	if err = s.checkStoreCursor(ctx); err != nil {
		return
	}
	if list.Snapshot != nil {
		if s.integrity.SnapshotVerified || list.Snapshot.GetVersion().GetVersion() == 0 || len(list.Snapshot.GetMac()) != 32 {
			return result, fmt.Errorf("invalid regular_low snapshot anchor")
		}
		seen := make(map[string]bool, len(list.Snapshot.Records))
		for _, record := range list.Snapshot.Records {
			index := string(record.GetIndex().GetBlob())
			if !policyRecordShape(record) || seen[index] {
				return result, fmt.Errorf("invalid regular_low snapshot record")
			}
			seen[index] = true
		}
		var state HashState
		result.Mutations, state, err = s.proc.decodeSnapshot(ctx, list.Name, list.Snapshot, HashState{}, true, nil)
		if err != nil {
			return
		}
		s.state = state
		s.integrity.SnapshotVerified = true
	}
	if !s.integrity.SnapshotVerified {
		return result, fmt.Errorf("verified regular_low snapshot required")
	}
	for _, patch := range list.Patches {
		if err = ctx.Err(); err != nil {
			return
		}
		if err = s.checkStoreCursor(ctx); err != nil {
			return
		}
		if patch == nil || s.state.Version == ^uint64(0) || patch.GetVersion().GetVersion() != s.state.Version+1 || len(patch.GetSnapshotMAC()) != 32 || len(patch.GetPatchMAC()) != 32 {
			return result, fmt.Errorf("invalid regular_low patch chain")
		}
		for _, mutation := range patch.Mutations {
			if mutation == nil || mutation.Operation == nil || (mutation.GetOperation() != waServerSync.SyncdMutation_SET && mutation.GetOperation() != waServerSync.SyncdMutation_REMOVE) || !policyRecordShape(mutation.GetRecord()) {
				return result, fmt.Errorf("invalid regular_low patch mutation")
			}
		}
		var next HashState
		var mismatch bool
		next, _, mismatch, err = s.proc.validatePatchWithSnapshotPolicy(ctx, list.Name, patch, s.state, true, s.policy == PatchSnapshotAllowMismatch)
		if err != nil {
			return
		}
		var out patchOutput
		err = s.proc.decodeMutations(ctx, patch.Mutations, &out, true, next.Version, nil)
		if err != nil {
			return
		}
		if err = ctx.Err(); err != nil {
			return
		}
		err = s.proc.storeMACs(ctx, list.Name, next, &out)
		if err != nil {
			return
		}
		s.state = next
		if mismatch {
			s.integrity.Degraded = true
			s.integrity.SnapshotMACMismatches++
		}
		result.Mutations = append(result.Mutations, out.Mutations...)
	}
	return
}

// This detects a stale version/hash, not an in-flight external writer or a map
// changed without its cursor. The underlying store has no CAS/transaction here;
// exclusive ownership remains mandatory throughout the session.
func (s *RegularLowDecodeSession) checkStoreCursor(ctx context.Context) error {
	version, hash, err := s.proc.Store.AppState.GetAppStateVersion(ctx, string(WAPatchRegularLow))
	if err != nil {
		return err
	}
	if version != s.state.Version || hash != s.state.Hash {
		return fmt.Errorf("stale regular_low store cursor")
	}
	return nil
}

func policyRecordShape(record *waServerSync.SyncdRecord) bool {
	value := record.GetValue().GetBlob()
	return record != nil && len(record.GetIndex().GetBlob()) == 32 && len(record.GetKeyID().GetID()) > 0 && len(value) >= 64 && (len(value)-48)%16 == 0
}
