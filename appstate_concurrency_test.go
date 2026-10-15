// Copyright (c) 2026 Gabriel Alonso
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package whatsmeow

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waSyncdSnapshotRecovery"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

// The reset gate exposes the interval where a recovery has removed the old
// version but has not installed its replacement. No network or device is used.
type recoveryTestStore struct {
	store.NoopStore
	mu     sync.Mutex
	state  appstate.HashState
	reset  chan struct{}
	resume chan struct{}
}

func (s *recoveryTestStore) GetAppStateVersion(context.Context, string) (uint64, [128]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state.Version, s.state.Hash, nil
}

func (s *recoveryTestStore) PutAppStateVersion(_ context.Context, _ string, version uint64, hash [128]byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = appstate.HashState{Version: version, Hash: hash}
	return nil
}

func (s *recoveryTestStore) DeleteAppStateVersion(context.Context, string) error {
	s.mu.Lock()
	s.state = appstate.HashState{}
	s.mu.Unlock()
	if s.reset != nil {
		close(s.reset)
		<-s.resume
	}
	return nil
}

func recoveryTestClient(s *recoveryTestStore) *Client {
	return NewClient(&store.Device{AppState: s, AppStateKeys: s}, nil)
}

func recoveryTestResult(t *testing.T, version uint64) []*waE2E.PeerDataOperationRequestResponseMessage_PeerDataOperationResult {
	t.Helper()
	data, err := proto.Marshal(&waSyncdSnapshotRecovery.SyncdSnapshotRecovery{
		CollectionName:   proto.String(string(appstate.WAPatchRegularLow)),
		Version:          &waSyncdSnapshotRecovery.SyncdVersion{Version: proto.Uint64(version)},
		CollectionLthash: make([]byte, 128),
	})
	if err != nil {
		t.Fatal(err)
	}
	return []*waE2E.PeerDataOperationRequestResponseMessage_PeerDataOperationResult{{
		SyncdSnapshotFatalRecoveryResponse: &waE2E.PeerDataOperationRequestResponseMessage_PeerDataOperationResult_SyncDSnapshotFatalRecoveryResponse{CollectionSnapshot: data},
	}}
}

func recoveryTestAwait[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case result := <-ch:
		return result
	case <-time.After(3 * time.Second):
		t.Fatal("app-state operation did not finish")
		var zero T
		return zero
	}
}

func TestAppStateOperationsWaitForRecovery(t *testing.T) {
	for _, operation := range []string{"fetch", "send"} {
		t.Run(operation, func(t *testing.T) {
			s := &recoveryTestStore{state: appstate.HashState{Version: 1}, reset: make(chan struct{}), resume: make(chan struct{})}
			cli := recoveryTestClient(s)
			result := recoveryTestResult(t, 2)
			recovered := make(chan bool, 1)
			go func() { recovered <- cli.handleAppStateRecovery(t.Context(), "synthetic", result) }()
			recoveryTestAwait(t, s.reset)
			started := make(chan struct{})
			done := make(chan error, 1)
			go func() {
				close(started)
				if operation == "fetch" {
					done <- cli.FetchAppState(t.Context(), appstate.WAPatchRegularLow, false, true)
				} else {
					done <- cli.SendAppState(t.Context(), appstate.PatchInfo{Type: appstate.WAPatchRegularLow})
				}
			}()
			recoveryTestAwait(t, started)
			var err error
			early := false
			select {
			case err = <-done:
				early = true
			case <-time.After(30 * time.Millisecond):
			}
			close(s.resume)
			if !early {
				err = recoveryTestAwait(t, done)
			}
			if !recoveryTestAwait(t, recovered) {
				t.Fatal("recovery handler failed")
			}
			if early {
				t.Fatal("operation observed a partially installed recovery")
			}
			if operation == "fetch" && err != nil {
				t.Fatal(err)
			}
			// The synthetic device has no send key; reaching this error after
			// waiting proves that send resumed without attempting transport.
			if operation == "send" && (err == nil || err.Error() != "no app state keys found, creating app state keys is not yet supported") {
				t.Fatalf("unexpected send result: %v", err)
			}
			version, _, _ := s.GetAppStateVersion(t.Context(), string(appstate.WAPatchRegularLow))
			if version != 2 {
				t.Fatalf("recovery version = %d", version)
			}
		})
	}
}

func TestAppStateRecoveryCallbacksCanFetch(t *testing.T) {
	cli := recoveryTestClient(&recoveryTestStore{})
	var called bool
	cli.AddEventHandlerWithSuccessStatus(func(evt any) bool {
		if _, ok := evt.(*events.AppStateSyncComplete); ok {
			called = true
			if err := cli.FetchAppState(t.Context(), appstate.WAPatchRegularLow, false, true); err != nil {
				t.Error(err)
			}
			return false
		}
		return true
	})
	result := recoveryTestResult(t, 2)
	done := make(chan bool, 1)
	go func() { done <- cli.handleAppStateRecovery(t.Context(), "synthetic", result) }()
	if recoveryTestAwait(t, done) || !called {
		t.Fatal("recovery callback success semantics changed")
	}
}

func TestCanceledAppStateOperationsPreserveState(t *testing.T) {
	for _, operation := range []string{"fetch", "send", "recovery"} {
		t.Run(operation, func(t *testing.T) {
			s := &recoveryTestStore{state: appstate.HashState{Version: 1}}
			cli := recoveryTestClient(s)
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			var err error
			switch operation {
			case "fetch":
				err = cli.FetchAppState(ctx, appstate.WAPatchRegularLow, true, false)
			case "send":
				err = cli.SendAppState(ctx, appstate.PatchInfo{Type: appstate.WAPatchRegularLow})
			case "recovery":
				cli.handleAppStateRecovery(ctx, "synthetic", recoveryTestResult(t, 2))
			}
			if operation != "recovery" && !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation lost: %v", err)
			}
			version, _, _ := s.GetAppStateVersion(t.Context(), string(appstate.WAPatchRegularLow))
			if version != 1 {
				t.Fatalf("canceled operation advanced state to %d", version)
			}
		})
	}
}
