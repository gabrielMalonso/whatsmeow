// Copyright (c) 2026 Gabriel Alonso
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package whatsmeow

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"google.golang.org/protobuf/proto"

	"go.mau.fi/whatsmeow/appstate"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waServerSync"
	"go.mau.fi/whatsmeow/proto/waSyncAction"
	"go.mau.fi/whatsmeow/proto/waSyncdSnapshotRecovery"
	"go.mau.fi/whatsmeow/socket"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"go.mau.fi/whatsmeow/util/gcmutil"
	"go.mau.fi/whatsmeow/util/hkdfutil"
	waLog "go.mau.fi/whatsmeow/util/log"
)

// A local peer scripts IQ responses while the real transport, app-state
// processor, hash checks, and MAC checks remain enabled.
type appStateTestPeer struct {
	requests  chan *waBinary.Node
	responses chan waBinary.Node
}

func newAppStateTestPeer(t *testing.T, cli *Client) *appStateTestPeer {
	t.Helper()
	peer := &appStateTestPeer{requests: make(chan *waBinary.Node), responses: make(chan waBinary.Node)}
	ctx, cancel := context.WithCancel(t.Context())
	finished := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(finished)
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			t.Errorf("accept test websocket: %v", err)
			return
		}
		defer conn.CloseNow()
		if err = peer.serve(ctx, conn); err != nil && ctx.Err() == nil {
			t.Errorf("test peer: %v", err)
		}
	}))
	t.Cleanup(server.Close)
	t.Cleanup(cancel)
	fs := socket.NewFrameSocket(waLog.Noop, server.Client())
	fs.URL = "ws" + strings.TrimPrefix(server.URL, "http")
	fs.Header = nil
	if err := fs.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancel()
		fs.Close(0)
		recoveryTestAwait(t, finished)
	})
	handshake := socket.NewNoiseHandshake()
	handshake.Start(socket.NoiseStartPattern, nil)
	var err error
	cli.socket, err = handshake.Finish(fs, cli.makeFrameHandler(nil), func(context.Context, *socket.NoiseSocket, bool) {})
	if err != nil {
		t.Fatal(err)
	}
	return peer
}

func (peer *appStateTestPeer) serve(ctx context.Context, conn *websocket.Conn) error {
	keys := hkdfutil.SHA256(nil, []byte(socket.NoiseStartPattern), nil, 64)
	readKey, err := gcmutil.Prepare(keys[:32])
	if err != nil {
		return err
	}
	writeKey, err := gcmutil.Prepare(keys[32:])
	if err != nil {
		return err
	}
	for counter := uint32(0); ; counter++ {
		_, frame, err := conn.Read(ctx)
		if err != nil {
			return err
		}
		if len(frame) < 3 || len(frame)-3 != int(frame[0])<<16|int(frame[1])<<8|int(frame[2]) {
			return fmt.Errorf("invalid test frame length: %d", len(frame))
		}
		iv := make([]byte, 12)
		binary.BigEndian.PutUint32(iv[8:], counter)
		data, err := readKey.Open(nil, iv, frame[3:], nil)
		if err != nil {
			return err
		}
		data, err = waBinary.Unpack(data)
		if err != nil {
			return err
		}
		request, err := waBinary.Unmarshal(data)
		if err != nil {
			return err
		}
		select {
		case peer.requests <- request:
		case <-ctx.Done():
			return ctx.Err()
		}
		var collection waBinary.Node
		select {
		case collection = <-peer.responses:
		case <-ctx.Done():
			return ctx.Err()
		}
		data, err = waBinary.Marshal(waBinary.Node{
			Tag: "iq", Attrs: waBinary.Attrs{"id": request.Attrs["id"], "type": "result"},
			Content: []waBinary.Node{{Tag: "sync", Content: []waBinary.Node{collection}}},
		})
		if err != nil {
			return err
		}
		data = writeKey.Seal(nil, iv, data, nil)
		frame = append([]byte{byte(len(data) >> 16), byte(len(data) >> 8), byte(len(data))}, data...)
		if err = conn.Write(ctx, websocket.MessageBinary, frame); err != nil {
			return err
		}
	}
}

func (peer *appStateTestPeer) request(t *testing.T, version uint64, sending bool) waBinary.Node {
	t.Helper()
	request := recoveryTestAwait(t, peer.requests)
	collection := request.GetChildByTag("sync", "collection")
	ag := collection.AttrGetter()
	if request.Tag != "iq" || ag.String("name") != string(appstate.WAPatchRegularLow) || ag.Uint64("version") != version {
		t.Fatalf("unexpected app-state request: %s", request)
	}
	if (collection.GetChildByTag("patch").Tag == "patch") != sending {
		t.Fatalf("expected sending=%v: %s", sending, request)
	}
	return collection
}

func (peer *appStateTestPeer) respond(t *testing.T, collection waBinary.Node) {
	t.Helper()
	select {
	case peer.responses <- collection:
	case <-time.After(3 * time.Second):
		t.Fatal("test peer did not accept response")
	}
}

type appStateTestFixture struct {
	patch    *waServerSync.SyncdPatch
	state    appstate.HashState
	recovery *waSyncdSnapshotRecovery.SyncdSnapshotRecovery
}

func newAppStateProtocolClient() *Client {
	return recoveryTestClient(&recoveryTestStore{key: &store.AppStateSyncKey{Data: bytes.Repeat([]byte{7}, 32)}})
}

// Fixtures replace the same pin index, so the last mutation also forms a full
// recovery snapshot with a different stored value MAC at each version.
func appStateTestFixtures(t *testing.T, count int) []appStateTestFixture {
	t.Helper()
	cli := newAppStateProtocolClient()
	fixtures := make([]appStateTestFixture, 0, count)
	var state appstate.HashState
	for i := range count {
		data, err := cli.appStateProc.EncodePatch(t.Context(), []byte{1}, state, appstate.BuildPin(types.NewJID("12345", types.DefaultUserServer), i%2 == 0))
		if err != nil {
			t.Fatal(err)
		}
		fixture := appStateTestDecode(t, cli, state, data)
		fixtures = append(fixtures, fixture)
		state = fixture.state
	}
	return fixtures
}

func appStateTestDecode(t *testing.T, cli *Client, state appstate.HashState, data []byte) appStateTestFixture {
	t.Helper()
	var patch waServerSync.SyncdPatch
	if err := proto.Unmarshal(data, &patch); err != nil {
		t.Fatal(err)
	}
	patch.Version = &waServerSync.SyncdVersion{Version: proto.Uint64(state.Version + 1)}
	mutations, next, err := cli.appStateProc.DecodePatches(t.Context(), &appstate.PatchList{
		Name: appstate.WAPatchRegularLow, Patches: []*waServerSync.SyncdPatch{&patch},
	}, state, true)
	if err != nil {
		t.Fatal(err)
	}
	recovery := &waSyncdSnapshotRecovery.SyncdSnapshotRecovery{
		CollectionName: proto.String(string(appstate.WAPatchRegularLow)),
		Version:        &waSyncdSnapshotRecovery.SyncdVersion{Version: proto.Uint64(next.Version)}, CollectionLthash: next.Hash[:],
	}
	for _, mutation := range mutations {
		index, err := json.Marshal(mutation.Index)
		if err != nil {
			t.Fatal(err)
		}
		recovery.MutationRecords = append(recovery.MutationRecords, &waSyncdSnapshotRecovery.SyncdPlainTextRecord{
			KeyID: mutation.KeyID, Mac: mutation.ValueMAC,
			Value: &waSyncAction.SyncActionData{Index: index, Value: mutation.Action, Version: proto.Int32(mutation.Version)},
		})
	}
	return appStateTestFixture{patch: &patch, state: next, recovery: recovery}
}

func (fixture appStateTestFixture) result(t *testing.T) []*waE2E.PeerDataOperationRequestResponseMessage_PeerDataOperationResult {
	t.Helper()
	data, err := proto.Marshal(fixture.recovery)
	if err != nil {
		t.Fatal(err)
	}
	return []*waE2E.PeerDataOperationRequestResponseMessage_PeerDataOperationResult{{
		SyncdSnapshotFatalRecoveryResponse: &waE2E.PeerDataOperationRequestResponseMessage_PeerDataOperationResult_SyncDSnapshotFatalRecoveryResponse{CollectionSnapshot: data},
	}}
}

func (fixture appStateTestFixture) install(t *testing.T, cli *Client) {
	t.Helper()
	if !cli.handleAppStateRecovery(t.Context(), "seed", fixture.result(t)) {
		t.Fatal("failed to install fixture")
	}
}

func (fixture appStateTestFixture) check(t *testing.T, cli *Client) {
	t.Helper()
	version, hash, err := cli.Store.AppState.GetAppStateVersion(t.Context(), string(appstate.WAPatchRegularLow))
	if err != nil || (appstate.HashState{Version: version, Hash: hash}) != fixture.state {
		t.Fatalf("state after operation: version=%d error=%v", version, err)
	}
	for _, mutation := range fixture.patch.Mutations {
		mac, err := cli.Store.AppState.GetAppStateMutationMAC(t.Context(), string(appstate.WAPatchRegularLow), mutation.GetRecord().GetIndex().GetBlob())
		value := mutation.GetRecord().GetValue().GetBlob()
		if err != nil || !bytes.Equal(mac, value[len(value)-32:]) {
			t.Fatalf("mutation MAC differs after operation: %v", err)
		}
	}
}

func appStateTestCollection(t *testing.T, hasMore, conflict bool, patches ...*waServerSync.SyncdPatch) waBinary.Node {
	t.Helper()
	collection := waBinary.Node{Tag: "collection", Attrs: waBinary.Attrs{
		"name": string(appstate.WAPatchRegularLow), "has_more_patches": hasMore,
	}}
	var children []waBinary.Node
	if conflict {
		collection.Attrs["type"] = "error"
		children = append(children, waBinary.Node{Tag: "error", Attrs: waBinary.Attrs{"code": 409}})
	}
	if len(patches) > 0 {
		var nodes []waBinary.Node
		for _, patch := range patches {
			data, err := proto.Marshal(patch)
			if err != nil {
				t.Fatal(err)
			}
			nodes = append(nodes, waBinary.Node{Tag: "patch", Content: data})
		}
		children = append(children, waBinary.Node{Tag: "patches", Content: nodes})
	}
	collection.Content = children
	return collection
}

func TestAppStatePaginatedFetchDefersRecovery(t *testing.T) {
	fixtures := appStateTestFixtures(t, 4)
	cli := newAppStateProtocolClient()
	fixtures[0].install(t, cli)
	peer := newAppStateTestPeer(t, cli)
	fetched := make(chan error, 1)
	go func() { fetched <- cli.FetchAppState(t.Context(), appstate.WAPatchRegularLow, false, false) }()
	peer.request(t, 1, false)
	peer.respond(t, appStateTestCollection(t, true, false, fixtures[1].patch))
	peer.request(t, 2, false)

	result := fixtures[2].result(t)
	recovered := make(chan bool, 1)
	go func() { recovered <- cli.handleAppStateRecovery(t.Context(), "between-pages", result) }()
	recoveryFinished := false
	select {
	case ok := <-recovered:
		recoveryFinished = true
		if !ok {
			t.Fatal("recovery handler failed")
		}
	case <-time.After(30 * time.Millisecond):
	}
	peer.respond(t, appStateTestCollection(t, false, false, fixtures[2].patch, fixtures[3].patch))
	if err := recoveryTestAwait(t, fetched); err != nil {
		t.Fatal(err)
	}
	if !recoveryFinished && !recoveryTestAwait(t, recovered) {
		t.Fatal("recovery handler failed")
	}
	// The queued v3 recovery must be ignored after the fetch commits v4.
	fixtures[3].check(t, cli)
}

func TestAppStateSendConflictRetryDefersRecovery(t *testing.T) {
	for _, conflictAgain := range []bool{false, true} {
		t.Run(fmt.Sprintf("second_conflict=%v", conflictAgain), func(t *testing.T) {
			fixtures := appStateTestFixtures(t, 2)
			cli := newAppStateProtocolClient()
			fixtures[0].install(t, cli)
			peer := newAppStateTestPeer(t, cli)
			sent := make(chan error, 1)
			go func() {
				sent <- cli.SendAppState(t.Context(), appstate.BuildPin(types.NewJID("12345", types.DefaultUserServer), true))
			}()
			peer.request(t, 1, true)
			peer.respond(t, appStateTestCollection(t, false, true, fixtures[1].patch))
			retry := peer.request(t, 2, true)
			server := newAppStateProtocolClient()
			fixtures[1].install(t, server)
			data, ok := retry.GetChildByTag("patch").Content.([]byte)
			if !ok {
				t.Fatal("retry did not contain encoded patch")
			}
			accepted := appStateTestDecode(t, server, fixtures[1].state, data)
			if conflictAgain {
				peer.respond(t, appStateTestCollection(t, false, true))
				if err := recoveryTestAwait(t, sent); !errors.Is(err, ErrAppStateUpdate) {
					t.Fatalf("second conflict must terminate retry: %v", err)
				}
				fixtures[1].check(t, cli)
				return
			}
			result := accepted.result(t)
			recovered := make(chan bool, 1)
			go func() { recovered <- cli.handleAppStateRecovery(t.Context(), "during-retry", result) }()
			select {
			case <-recovered:
				t.Fatal("recovery interleaved with conflict retry")
			case <-time.After(30 * time.Millisecond):
			}
			peer.respond(t, appStateTestCollection(t, false, false))
			peer.request(t, 2, false)
			select {
			case <-recovered:
				t.Fatal("recovery interleaved with the post-send fetch")
			default:
			}
			peer.respond(t, appStateTestCollection(t, false, false, accepted.patch))
			if err := recoveryTestAwait(t, sent); err != nil {
				t.Fatal(err)
			}
			if !recoveryTestAwait(t, recovered) {
				t.Fatal("recovery handler failed")
			}
			accepted.check(t, cli)
		})
	}
}

func TestAppStateRejectsTamperedPatchAfterUnlock(t *testing.T) {
	for _, tamper := range []string{"hash", "patch_mac"} {
		t.Run(tamper, func(t *testing.T) {
			fixtures := appStateTestFixtures(t, 2)
			patch := fixtures[1].patch
			expectedErr := appstate.ErrMismatchingLTHash
			if tamper == "hash" {
				patch.SnapshotMAC[0] ^= 1
			} else {
				patch.PatchMAC[0] ^= 1
				expectedErr = appstate.ErrMismatchingPatchMAC
			}
			cli := newAppStateProtocolClient()
			fixtures[0].install(t, cli)
			callback := make(chan error, 1)
			cli.AddEventHandler(func(evt any) {
				if syncError, ok := evt.(*events.AppStateSyncError); ok {
					if !errors.Is(syncError.Error, expectedErr) {
						t.Errorf("unexpected callback error: %v", syncError.Error)
					}
					callback <- cli.FetchAppState(t.Context(), appstate.WAPatchRegularLow, false, true)
				}
			})
			peer := newAppStateTestPeer(t, cli)
			fetched := make(chan error, 1)
			go func() { fetched <- cli.FetchAppState(t.Context(), appstate.WAPatchRegularLow, false, false) }()
			peer.request(t, 1, false)
			peer.respond(t, appStateTestCollection(t, false, false, patch))
			if err := recoveryTestAwait(t, fetched); !errors.Is(err, expectedErr) {
				t.Fatalf("integrity check bypassed: %v", err)
			}
			if err := recoveryTestAwait(t, callback); err != nil {
				t.Fatalf("reentrant callback failed: %v", err)
			}
			fixtures[0].check(t, cli)
		})
	}
}
