// Copyright (c) 2026 Gabriel Alonso
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package whatsmeow

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"go.mau.fi/whatsmeow/appstate"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waServerSync"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

var lowTestJID = types.NewJID("12345", types.DefaultUserServer)
var errLowTestStore = errors.New("synthetic store refusal")

// Reuse the base56 stores, encoder/decoder and local IQ peer. No alternate
// crypto implementation, exported test bridge or production transport hook.
type lowTestStore struct {
	*recoveryTestStore
	writes, resets         int
	failMACs, failSettings bool
	settings               map[types.JID]types.LocalChatSettings
}

func (s *lowTestStore) PutAppStateVersion(ctx context.Context, name string, version uint64, hash [128]byte) error {
	s.writes++
	return s.recoveryTestStore.PutAppStateVersion(ctx, name, version, hash)
}
func (s *lowTestStore) DeleteAppStateVersion(ctx context.Context, name string) error {
	s.resets++
	return s.recoveryTestStore.DeleteAppStateVersion(ctx, name)
}
func (s *lowTestStore) PutAppStateMutationMACs(ctx context.Context, name string, version uint64, macs []store.AppStateMutationMAC) error {
	if s.failMACs {
		return errLowTestStore
	}
	return s.recoveryTestStore.PutAppStateMutationMACs(ctx, name, version, macs)
}
func (s *lowTestStore) PutArchived(_ context.Context, jid types.JID, value bool) error {
	if s.failSettings {
		return errLowTestStore
	}
	row := s.settings[jid]
	row.Archived = value
	s.settings[jid] = row
	return nil
}
func (s *lowTestStore) PutPinned(_ context.Context, jid types.JID, value bool) error {
	if s.failSettings {
		return errLowTestStore
	}
	row := s.settings[jid]
	row.Pinned = value
	s.settings[jid] = row
	return nil
}

func lowTestClient(t *testing.T, optIn bool) (*Client, *lowTestStore, *appStateTestPeer) {
	t.Helper()
	s := &lowTestStore{recoveryTestStore: &recoveryTestStore{key: &store.AppStateSyncKey{Data: bytes.Repeat([]byte{7}, 32)}}, settings: map[types.JID]types.LocalChatSettings{}}
	device := &store.Device{AppState: s, AppStateKeys: s, ChatSettings: s}
	cli := NewClient(device, nil)
	if optIn {
		var err error
		cli, err = NewClientWithRegularLowPolicy(device, nil, appstate.PatchSnapshotAllowMismatch)
		if err != nil {
			t.Fatal(err)
		}
	}
	return cli, s, newAppStateTestPeer(t, cli)
}

func lowTestPatch(t *testing.T, author *Client, info appstate.PatchInfo, diverge bool) appStateTestFixture {
	t.Helper()
	version, hash, err := author.Store.AppState.GetAppStateVersion(t.Context(), string(info.Type))
	if err != nil {
		t.Fatal(err)
	}
	state := appstate.HashState{Version: version, Hash: hash}
	if diverge {
		state.Hash[0] ^= 1
	}
	data, err := author.appStateProc.EncodePatch(t.Context(), []byte{1}, state, info)
	if err != nil {
		t.Fatal(err)
	}
	return appStateTestDecode(t, author, state, data)
}

// Serve a synthetic external snapshot through the existing media downloader.
// Its own SHA256 and all app-state authentication checks remain enabled.
func lowTestSnapshot(t *testing.T, cli *Client, patch *waServerSync.SyncdPatch) waBinary.Node {
	t.Helper()
	snapshot := &waServerSync.SyncdSnapshot{Version: patch.Version, KeyID: patch.KeyID, Mac: patch.SnapshotMAC}
	for _, m := range patch.Mutations {
		snapshot.Records = append(snapshot.Records, m.Record)
	}
	raw, err := proto.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(raw) }))
	t.Cleanup(server.Close)
	cli.SetMediaHTTPClient(server.Client())
	cli.mediaConnCache = &MediaConn{FetchedAt: time.Now(), TTL: 3600, Hosts: []MediaConnHost{{Hostname: strings.TrimPrefix(server.URL, "https://")}}}
	hash := sha256.Sum256(raw)
	ref, err := proto.Marshal(&waServerSync.ExternalBlobReference{DirectPath: proto.String("/snapshot?"), FileSHA256: hash[:]})
	if err != nil {
		t.Fatal(err)
	}
	return waBinary.Node{Tag: "collection", Attrs: waBinary.Attrs{"name": string(appstate.WAPatchRegularLow)}, Content: []waBinary.Node{{Tag: "snapshot", Content: ref}}}
}

type lowTestResult struct {
	events []any
	err    error
}

func lowTestFetch(t *testing.T, cli *Client, peer *appStateTestPeer, full bool, version uint64, collection waBinary.Node) lowTestResult {
	t.Helper()
	done := make(chan lowTestResult, 1)
	go func() {
		evts, err := cli.fetchAppState(t.Context(), appstate.WAPatchRegularLow, full, false)
		done <- lowTestResult{evts, err}
	}()
	request := peer.request(t, version, false)
	if got := request.AttrGetter().Bool("return_snapshot"); got != full {
		t.Fatalf("snapshot request=%v, full=%v", got, full)
	}
	peer.respond(t, collection)
	return recoveryTestAwait(t, done)
}

func lowTestAnchor(t *testing.T, cli *Client, peer *appStateTestPeer, fixture appStateTestFixture) {
	t.Helper()
	if result := lowTestFetch(t, cli, peer, true, 0, lowTestSnapshot(t, cli, fixture.patch)); result.err != nil {
		t.Fatal(result.err)
	}
}

func TestRegularLowAuthentication(t *testing.T) {
	for _, kind := range []string{"default_strict", "snapshot_mac", "snapshot_content", "patch_mac", "content", "index", "short_record"} {
		t.Run(kind, func(t *testing.T) {
			cli, s, peer := lowTestClient(t, kind != "default_strict")
			author := newAppStateProtocolClient()
			first := lowTestPatch(t, author, appstate.BuildPin(lowTestJID, true), false)
			var result lowTestResult
			var want error
			if strings.HasPrefix(kind, "snapshot_") {
				if kind == "snapshot_mac" {
					first.patch.SnapshotMAC[0] ^= 1
					want = appstate.ErrMismatchingLTHash
				} else {
					first.patch.Mutations[0].Record.Value.Blob[20] ^= 1
					want = appstate.ErrMismatchingContentMAC
				}
				result = lowTestFetch(t, cli, peer, true, 0, lowTestSnapshot(t, cli, first.patch))
			} else {
				lowTestAnchor(t, cli, peer, first)
				next := lowTestPatch(t, author, appstate.BuildPin(lowTestJID, false), kind == "default_strict")
				switch kind {
				case "default_strict":
					want = appstate.ErrMismatchingLTHash
				case "patch_mac":
					next.patch.PatchMAC[0] ^= 1
					want = appstate.ErrMismatchingPatchMAC
				case "content":
					next.patch.Mutations[0].Record.Value.Blob[20] ^= 1
					want = appstate.ErrMismatchingContentMAC
				case "index":
					next.patch.Mutations[0].Record.Index.Blob[0] ^= 1
					want = appstate.ErrMismatchingIndexMAC
				case "short_record":
					next.patch.Mutations[0].Record.Value.Blob = []byte{1}
				}
				result = lowTestFetch(t, cli, peer, false, 1, appStateTestCollection(t, false, false, next.patch))
			}
			if result.err == nil || want != nil && !errors.Is(result.err, want) {
				t.Fatalf("error=%v, want=%v", result.err, want)
			}
			wantWrites := 1
			if strings.HasPrefix(kind, "snapshot_") {
				wantWrites = 0
			}
			if s.writes != wantWrites || len(result.events) != 0 {
				t.Fatalf("rejected data wrote state or returned events: writes=%d events=%d", s.writes, len(result.events))
			}
			if cli.RegularLowPolicyStatus().Valid {
				t.Fatal("failed decode retained a valid session")
			}
		})
	}
}

func TestRegularLowDegradedReconvergence(t *testing.T) {
	cli, s, peer := lowTestClient(t, true)
	author := newAppStateProtocolClient()
	first := lowTestPatch(t, author, appstate.BuildPin(lowTestJID, true), false)
	lowTestAnchor(t, cli, peer, first)
	next := lowTestPatch(t, author, appstate.BuildPin(lowTestJID, false), true)
	if result := lowTestFetch(t, cli, peer, false, 1, appStateTestCollection(t, false, false, next.patch)); result.err != nil {
		t.Fatal(result.err)
	}
	status := cli.RegularLowPolicyStatus()
	if !status.Anchored || !status.Valid || !status.FetchComplete || !status.Degraded || status.Version != 2 || status.SnapshotMACMismatches != 1 {
		t.Fatalf("status: %+v", status)
	}
	if projection := cli.RegularLowChatProjection(lowTestJID); !projection.PinKnown || projection.Pinned || projection.ArchiveKnown {
		t.Fatalf("projection: %+v", projection)
	}
	// The next aggregate matches the receiver again. Degradation stays visible.
	version, hash, _ := s.GetAppStateVersion(t.Context(), "regular_low")
	if err := author.Store.AppState.PutAppStateVersion(t.Context(), "regular_low", version, hash); err != nil {
		t.Fatal(err)
	}
	last := lowTestPatch(t, author, appstate.BuildPin(lowTestJID, true), false)
	if result := lowTestFetch(t, cli, peer, false, 2, appStateTestCollection(t, false, false, last.patch)); result.err != nil {
		t.Fatal(result.err)
	}
	status = cli.RegularLowPolicyStatus()
	if !status.Degraded || status.SnapshotMACMismatches != 1 || status.Version != 3 {
		t.Fatalf("reconvergence hid degradation: %+v", status)
	}
}

func TestRegularLowLifecycle(t *testing.T) {
	for _, kind := range []string{"restart", "read_iq", "paged_read_iq", "cursor", "store", "projection", "invalidate"} {
		t.Run(kind, func(t *testing.T) {
			cli, s, peer := lowTestClient(t, true)
			author := newAppStateProtocolClient()
			first := lowTestPatch(t, author, appstate.BuildPin(lowTestJID, true), false)
			if kind == "restart" {
				s.state = first.state
				if err := cli.FetchAppState(t.Context(), appstate.WAPatchRegularLow, false, true); !errors.Is(err, ErrRegularLowAnchorRequired) {
					t.Fatalf("stored cursor became anchor: %v", err)
				}
				if s.resets != 0 || s.writes != 0 {
					t.Fatal("implicit reset/write")
				}
			}
			lowTestAnchor(t, cli, peer, first)
			if kind == "restart" {
				return
			}
			next := lowTestPatch(t, author, appstate.BuildPin(lowTestJID, false), false)
			switch kind {
			case "read_iq", "paged_read_iq":
				version := uint64(1)
				retry := appStateTestCollection(t, false, false, next.patch)
				var result lowTestResult
				if kind == "paged_read_iq" {
					done := make(chan lowTestResult, 1)
					go func() {
						evts, err := cli.fetchAppState(t.Context(), appstate.WAPatchRegularLow, false, false)
						done <- lowTestResult{evts, err}
					}()
					peer.request(t, 1, false)
					peer.respond(t, appStateTestCollection(t, true, false, next.patch))
					peer.request(t, 2, false)
					peer.respond(t, appStateTestCollection(t, false, true))
					result = recoveryTestAwait(t, done)
					version = 2
					retry = appStateTestCollection(t, false, false)
					if !s.settings[lowTestJID].Pinned {
						t.Fatal("incomplete pages projected intermediate state")
					}
				} else {
					result = lowTestFetch(t, cli, peer, false, 1, appStateTestCollection(t, false, true))
				}
				if !errors.Is(result.err, ErrAppStateUpdate) {
					t.Fatalf("read refusal: %v", result.err)
				}
				status := cli.RegularLowPolicyStatus()
				if !status.Anchored || !status.Valid || status.FetchComplete {
					t.Fatalf("IQ refusal lost anchor: %+v", status)
				}
				if cli.RegularLowChatProjection(lowTestJID).PinKnown {
					t.Fatal("incomplete getter exposed stale fields")
				}
				if _, err := cli.SendRegularLowStateEvents(t.Context(), appstate.BuildPin(lowTestJID, false)); !errors.Is(err, ErrRegularLowFetchRequired) {
					t.Fatalf("send while incomplete: %v", err)
				}
				if result = lowTestFetch(t, cli, peer, false, version, retry); result.err != nil {
					t.Fatal(result.err)
				}
				if s.resets != 1 {
					t.Fatal("delta retry reset the cursor")
				}
				return
			case "cursor":
				s.mu.Lock()
				s.state.Version++
				s.mu.Unlock()
				if err := cli.FetchAppState(t.Context(), appstate.WAPatchRegularLow, false, true); !errors.Is(err, ErrRegularLowSessionInvalid) {
					t.Fatalf("external cursor: %v", err)
				}
			case "store", "projection":
				s.failMACs = kind == "store"
				s.failSettings = kind == "projection"
				if result := lowTestFetch(t, cli, peer, false, 1, appStateTestCollection(t, false, false, next.patch)); !errors.Is(result.err, errLowTestStore) {
					t.Fatalf("store failure: %v", result.err)
				}
				// Native writes are not rolled back when a later MAC/settings write fails.
				version, _, _ := s.GetAppStateVersion(t.Context(), "regular_low")
				if version != 2 {
					t.Fatalf("partial writes unexpectedly rolled back: %d", version)
				}
			case "invalidate":
				cli.InvalidateRegularLowPolicy()
			}
			if cli.RegularLowPolicyStatus().Valid || cli.RegularLowChatProjection(lowTestJID).PinKnown {
				t.Fatal("invalid session exposed current fields")
			}
			if _, err := cli.DangerousInternals().ApplyAppStatePatches(t.Context(), appstate.WAPatchRegularLow, cli.regularLow.state, &appstate.PatchList{Name: appstate.WAPatchRegularLow}, false, nil); !errors.Is(err, ErrRegularLowSessionInvalid) {
				t.Fatalf("internal decoder resurrected an invalid session: %v", err)
			}
			if err := cli.FetchAppState(t.Context(), appstate.WAPatchRegularLow, false, false); !errors.Is(err, ErrRegularLowSessionInvalid) {
				t.Fatalf("invalid session retried: %v", err)
			}
			if s.resets != 1 {
				t.Fatal("failure triggered automatic reset")
			}
		})
	}
}

func TestRegularLowSendConfirmation(t *testing.T) {
	for _, kind := range []string{"owned", "native", "conflict", "conflict_ambiguous", "no_echo", "send_refusal"} {
		t.Run(kind, func(t *testing.T) {
			cli, s, peer := lowTestClient(t, true)
			author := newAppStateProtocolClient()
			first := lowTestPatch(t, author, appstate.BuildPin(lowTestJID, false), false)
			lowTestAnchor(t, cli, peer, first)
			dispatched := make(chan any, 8)
			cli.AddEventHandler(func(evt any) { dispatched <- evt })
			done := make(chan lowTestResult, 1)
			go func() {
				if kind == "native" {
					done <- lowTestResult{err: cli.SendAppState(t.Context(), appstate.BuildPin(lowTestJID, true))}
					return
				}
				evts, err := cli.SendRegularLowStateEvents(t.Context(), appstate.BuildPin(lowTestJID, true))
				done <- lowTestResult{evts, err}
			}()
			request := peer.request(t, 1, true)
			version := uint64(1)
			if strings.HasPrefix(kind, "conflict") {
				info := appstate.BuildPin(lowTestJID, true)
				if kind == "conflict_ambiguous" {
					info.Mutations[0].Index = append(info.Mutations[0].Index, "physical-alias")
				}
				conflict := lowTestPatch(t, author, info, false)
				peer.respond(t, appStateTestCollection(t, false, true, conflict.patch))
				if kind == "conflict_ambiguous" {
					result := recoveryTestAwait(t, done)
					if !errors.Is(result.err, ErrRegularLowProjectionUnknown) || len(result.events) != 0 || cli.RegularLowPolicyStatus().Valid {
						t.Fatalf("ambiguous rebase: %+v", result)
					}
					if s.settings[lowTestJID].Pinned {
						t.Fatal("conflicting intermediate state leaked into product store")
					}
					return
				}
				version = 2
				request = peer.request(t, version, true)
			}
			if kind == "send_refusal" {
				refusal := appStateTestCollection(t, false, true)
				refusal.GetChildren()[0].Attrs["code"] = 500
				peer.respond(t, refusal)
				result := recoveryTestAwait(t, done)
				if !errors.Is(result.err, ErrAppStateUpdate) || cli.RegularLowPolicyStatus().Valid {
					t.Fatalf("send refusal: %+v", result)
				}
				return
			}
			raw := request.GetChildByTag("patch").Content.([]byte)
			authorVersion, authorHash, _ := author.Store.AppState.GetAppStateVersion(t.Context(), "regular_low")
			accepted := appStateTestDecode(t, author, appstate.HashState{Version: authorVersion, Hash: authorHash}, raw)
			peer.respond(t, appStateTestCollection(t, false, false)) // IQ ACK alone is insufficient.
			peer.request(t, version, false)
			if kind == "no_echo" {
				peer.respond(t, appStateTestCollection(t, false, false))
				result := recoveryTestAwait(t, done)
				if !errors.Is(result.err, ErrRegularLowSendUnconfirmed) || len(result.events) != 0 || cli.RegularLowPolicyStatus().Valid {
					t.Fatalf("ACK-only send confirmed: %+v", result)
				}
				return
			}
			peer.respond(t, appStateTestCollection(t, false, false, accepted.patch))
			result := recoveryTestAwait(t, done)
			if result.err != nil {
				t.Fatal(result.err)
			}
			if kind == "native" {
				if _, ok := recoveryTestAwait(t, dispatched).(*events.Pin); !ok {
					t.Fatal("native send lost dispatch")
				}
			} else {
				if len(result.events) != 1 {
					t.Fatalf("owned events: %v", result.events)
				}
				pin, ok := result.events[0].(*events.Pin)
				if !ok || pin.JID != lowTestJID || !pin.Action.GetPinned() {
					t.Fatalf("returned events differ from echo: %v", result.events)
				}
				select {
				case evt := <-dispatched:
					t.Fatalf("owned events dispatched twice: %T", evt)
				default:
				}
				// A later notification can hide the live getter; the owned result
				// still contains its exact immutable authenticated field value.
				notified := make(chan bool, 1)
				go func() {
					cli.handleAppStateNotification(t.Context(), &waBinary.Node{Content: []waBinary.Node{{Tag: "collection", Attrs: waBinary.Attrs{"name": "regular_low", "version": version + 1}}}})
					notified <- true
				}()
				peer.request(t, version+1, false)
				if cli.RegularLowChatProjection(lowTestJID).PinKnown || !pin.Action.GetPinned() {
					t.Fatal("owned result depends on a later live getter")
				}
				peer.respond(t, appStateTestCollection(t, false, false))
				recoveryTestAwait(t, notified)
			}
			accepted.check(t, cli)
		})
	}
}

func TestRegularLowCurrentProjectionAndRecovery(t *testing.T) {
	cli, s, peer := lowTestClient(t, true)
	author := newAppStateProtocolClient()
	info := appstate.BuildArchive(lowTestJID, false, time.Time{}, nil)
	info.Mutations = append(info.Mutations, appstate.BuildMarkChatAsRead(lowTestJID, true, time.Unix(10, 0), nil).Mutations...)
	info.Mutations = append(info.Mutations, appstate.BuildDeleteChat(lowTestJID, time.Unix(10, 0), nil, false).Mutations...)
	first := lowTestPatch(t, author, info, false)
	s.settings[lowTestJID] = types.LocalChatSettings{Pinned: true}
	result := lowTestFetch(t, cli, peer, true, 0, lowTestSnapshot(t, cli, first.patch))
	if result.err != nil {
		t.Fatal(result.err)
	}
	if len(result.events) != 2 {
		t.Fatalf("unsupported/destructive historical event escaped: %v", result.events)
	}
	if len(s.macs) != len(first.patch.Mutations) {
		t.Fatal("unsupported records were removed from crypto map")
	}
	if !s.settings[lowTestJID].Pinned {
		t.Fatal("missing pin was treated as false")
	}
	projection := cli.RegularLowChatProjection(lowTestJID)
	if !projection.ArchiveKnown || projection.Archived || projection.PinKnown || !projection.ReadKnown || !projection.Read {
		t.Fatalf("projection: %+v", projection)
	}
	projection.ReadAction.MessageRange.LastMessageTimestamp = proto.Int64(999)
	if cli.RegularLowChatProjection(lowTestJID).ReadAction.GetMessageRange().GetLastMessageTimestamp() != 10 {
		t.Fatal("getter exposed mutable read range")
	}
	// Two physical records for the same product field are unknown, not ordered
	// by timestamp/map iteration. A valid archive field remains independent.
	alias := appstate.BuildMarkChatAsRead(lowTestJID, false, time.Time{}, nil)
	alias.Mutations[0].Index = append(alias.Mutations[0].Index, "physical-alias")
	next := lowTestPatch(t, author, alias, false)
	result = lowTestFetch(t, cli, peer, false, 1, appStateTestCollection(t, false, false, next.patch))
	if result.err != nil {
		t.Fatal(result.err)
	}
	projection = cli.RegularLowChatProjection(lowTestJID)
	if projection.ReadKnown || !projection.ReadAmbiguous || !projection.ArchiveKnown {
		t.Fatalf("aliases chose a winner: %+v", projection)
	}
	if len(result.events) != 1 {
		t.Fatalf("ambiguous read escaped into callbacks: %v", result.events)
	}
	// Low and indeterminate peer recovery must neither reset/store nor reach
	// normal Message sinks, including after a native wrapper is unwrapped.
	messages := 0
	cli.AddEventHandler(func(evt any) {
		if _, ok := evt.(*events.Message); ok {
			messages++
		}
	})
	for _, recovery := range [][]*waE2E.PeerDataOperationRequestResponseMessage_PeerDataOperationResult{first.result(t), {{SyncdSnapshotFatalRecoveryResponse: &waE2E.PeerDataOperationRequestResponseMessage_PeerDataOperationResult_SyncDSnapshotFatalRecoveryResponse{CollectionSnapshot: []byte{0xff}}}}} {
		if !cli.handleAppStateRecovery(t.Context(), "quarantine", recovery) {
			t.Fatal("quarantine handler failed")
		}
		evt := &events.Message{RawMessage: &waE2E.Message{EphemeralMessage: &waE2E.FutureProofMessage{Message: &waE2E.Message{ProtocolMessage: &waE2E.ProtocolMessage{PeerDataOperationRequestResponseMessage: &waE2E.PeerDataOperationRequestResponseMessage{PeerDataOperationRequestType: waE2E.PeerDataOperationRequestType_COMPANION_SYNCD_SNAPSHOT_FATAL_RECOVERY.Enum(), PeerDataOperationResult: recovery}}}}}}
		cli.dispatchRegularLowAwareMessage(t.Context(), evt.UnwrapRaw())
	}
	if messages != 0 || s.resets != 1 || s.writes != 2 {
		t.Fatalf("recovery escaped: messages=%d resets=%d writes=%d", messages, s.resets, s.writes)
	}
	for _, name := range appstate.AllPatchNames {
		if regularLowRecoveryCollectionAllowed(name) != (name != appstate.WAPatchRegularLow) {
			t.Fatalf("recovery classification: %s", name)
		}
	}
	if regularLowRecoveryCollectionAllowed(appstate.WAPatchName("unknown")) {
		t.Fatal("unknown recovery permitted")
	}
}
