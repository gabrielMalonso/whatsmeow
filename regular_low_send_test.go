// Copyright (c) 2026 Gabriel Alonso
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package whatsmeow

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

type lowSendTestKeys struct {
	store.AppStateSyncKeyStore
	latest       []byte
	keys         map[string]*store.AppStateSyncKey
	beforeLatest func()
}

func (s *lowSendTestKeys) GetAppStateSyncKey(_ context.Context, id []byte) (*store.AppStateSyncKey, error) {
	return s.keys[string(id)], nil
}

func (s *lowSendTestKeys) GetLatestAppStateSyncKeyID(context.Context) ([]byte, error) {
	if s.beforeLatest != nil {
		s.beforeLatest()
	}
	return bytes.Clone(s.latest), nil
}

func TestRegularLowGroupSendEcho(t *testing.T) {
	for _, kind := range []string{"same_key", "same_material_new_id", "changed_key", "conflict_key_change"} {
		t.Run(kind, func(t *testing.T) {
			cli, s, peer := lowTestClient(t, true)
			keys := &lowSendTestKeys{latest: []byte{1}, keys: map[string]*store.AppStateSyncKey{
				string([]byte{1}): {Data: bytes.Repeat([]byte{7}, 32)},
				string([]byte{2}): {Data: bytes.Repeat([]byte{8}, 32)},
			}}
			cli.Store.AppStateKeys = keys
			if kind == "same_material_new_id" {
				keys.keys[string([]byte{2})] = keys.keys[string([]byte{1})]
			}
			author := newAppStateProtocolClient()
			author.Store.AppStateKeys = keys
			group := types.NewJID("123456-789", types.GroupServer)
			first := lowTestPatch(t, author, appstate.BuildMarkChatAsRead(group, true, time.Unix(30, 0), nil), false)
			lowTestAnchor(t, cli, peer, first)
			if kind == "changed_key" || kind == "same_material_new_id" {
				keys.latest = []byte{2}
			}
			writes, resets := s.writes, s.resets
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			done := make(chan lowTestResult, 1)
			go func() {
				events, err := cli.SendRegularLowStateEvents(ctx, appstate.BuildMarkChatAsRead(group, false, time.Unix(30, 0), nil))
				done <- lowTestResult{events: events, err: err}
			}()
			if kind == "changed_key" {
				select {
				case <-peer.requests:
					t.Fatal("key change was submitted before detecting the physical alias")
				case result := <-done:
					var preflight *RegularLowSendPreflightError
					if !errors.As(result.err, &preflight) || !errors.Is(result.err, ErrRegularLowProjectionUnknown) || len(result.events) != 0 {
						t.Fatalf("key change was not an explicit preflight refusal: %+v", result)
					}
				case <-ctx.Done():
					t.Fatal("preflight refusal timed out")
				}
				status := cli.RegularLowPolicyStatus()
				p := cli.RegularLowChatProjection(group)
				if !status.Valid || !status.FetchComplete || status.Version != 1 || !p.ReadKnown || !p.Read || s.writes != writes || s.resets != resets {
					t.Fatalf("preflight changed the anchor or product state: %+v %+v", status, p)
				}
				return
			}
			request := peer.request(t, 1, true)
			if kind == "conflict_key_change" {
				conflict := lowTestPatch(t, author, appstate.BuildPin(group, false), false)
				keys.latest = []byte{2}
				peer.respond(t, appStateTestCollection(t, false, true, conflict.patch))
				select {
				case <-peer.requests:
					t.Fatal("unsafe key-changing retry was submitted")
				case result := <-done:
					var preflight *RegularLowSendPreflightError
					if !errors.Is(result.err, ErrRegularLowProjectionUnknown) || errors.As(result.err, &preflight) || cli.RegularLowPolicyStatus().Valid {
						t.Fatalf("409 effects were mislabeled as a preflight refusal: %+v", result)
					}
				case <-ctx.Done():
					t.Fatal("409 refusal timed out")
				}
				return
			}
			accepted := appStateTestDecode(t, author, first.state, request.GetChildByTag("patch").Content.([]byte))
			peer.respond(t, appStateTestCollection(t, false, false))
			peer.request(t, 1, false)
			peer.respond(t, appStateTestCollection(t, false, false, accepted.patch))
			result := recoveryTestAwait(t, done)
			if result.err != nil || !cli.RegularLowChatProjection(group).ReadKnown || cli.RegularLowChatProjection(group).Read || len(result.events) != 1 {
				t.Fatalf("authenticated group echo not confirmed: %+v", result)
			}
		})
	}
}

func TestRegularLowSendRefusesJIDSpellingAlias(t *testing.T) {
	cli, s, peer := lowTestClient(t, true)
	author := newAppStateProtocolClient()
	info := appstate.BuildPin(lowTestJID, false)
	info.Mutations[0].Index[1] = "12345:0@s.whatsapp.net"
	first := lowTestPatch(t, author, info, false)
	lowTestAnchor(t, cli, peer, first)
	writes := s.writes
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	done := make(chan lowTestResult, 1)
	go func() {
		evts, err := cli.SendRegularLowStateEvents(ctx, appstate.BuildPin(lowTestJID, true))
		done <- lowTestResult{evts, err}
	}()
	select {
	case <-peer.requests:
		t.Fatal("logical JID alias was submitted under a second physical index")
	case result := <-done:
		var preflight *RegularLowSendPreflightError
		if !errors.As(result.err, &preflight) || !errors.Is(result.err, ErrRegularLowProjectionUnknown) || len(result.events) != 0 {
			t.Fatalf("alias did not receive a typed preflight refusal: %+v", result)
		}
	case <-ctx.Done():
		t.Fatal("alias preflight timed out")
	}
	if status := cli.RegularLowPolicyStatus(); !status.Valid || !status.FetchComplete || s.writes != writes {
		t.Fatalf("preflight changed the session: %+v", status)
	}
}

func TestRegularLowSendRefusesDuplicateFields(t *testing.T) {
	for _, spelling := range []string{lowTestJID.String(), "12345:0@s.whatsapp.net"} {
		t.Run(spelling, func(t *testing.T) {
			cli, s, peer := lowTestClient(t, true)
			author := newAppStateProtocolClient()
			lowTestAnchor(t, cli, peer, lowTestPatch(t, author, appstate.BuildArchive(lowTestJID, false, time.Time{}, nil), false))
			info := appstate.BuildPin(lowTestJID, false)
			alias := appstate.BuildPin(lowTestJID, true).Mutations[0]
			alias.Index[1] = spelling
			info.Mutations = append(info.Mutations, alias)
			writes := s.writes
			events, err := cli.SendRegularLowStateEvents(t.Context(), info)
			var preflight *RegularLowSendPreflightError
			if !errors.As(err, &preflight) || !errors.Is(err, ErrRegularLowProjectionUnknown) || len(events) != 0 || s.writes != writes {
				t.Fatalf("duplicate field was not refused before submission: %v", err)
			}
			select {
			case <-peer.requests:
				t.Fatal("duplicate field was submitted")
			default:
			}
		})
	}
}

func TestRegularLowSendGeneration(t *testing.T) {
	for _, stage := range []string{"ack", "conflict", "reencode", "postfetch", "confirmation"} {
		t.Run(stage, func(t *testing.T) {
			cli, s, peer := lowTestClient(t, true)
			author := newAppStateProtocolClient()
			first := lowTestPatch(t, author, appstate.BuildPin(lowTestJID, false), false)
			lowTestAnchor(t, cli, peer, first)
			if stage == "reencode" {
				calls := 0
				cli.Store.AppStateKeys = &lowSendTestKeys{latest: []byte{1}, keys: map[string]*store.AppStateSyncKey{string([]byte{1}): s.key}, beforeLatest: func() {
					calls++
					if calls == 2 {
						cli.regularLow.disconnected()
					}
				}}
			}
			dispatched := make(chan any, 8)
			cli.AddEventHandler(func(evt any) { dispatched <- evt })
			done := make(chan lowTestResult, 1)
			go func() {
				events, err := cli.SendRegularLowStateEvents(t.Context(), appstate.BuildPin(lowTestJID, true))
				done <- lowTestResult{events, err}
			}()
			request := peer.request(t, 1, true)
			if stage == "ack" || stage == "conflict" {
				cli.regularLow.disconnected()
			}
			if stage == "conflict" || stage == "reencode" {
				conflict := lowTestPatch(t, author, appstate.BuildPin(lowTestJID, false), false)
				peer.respond(t, appStateTestCollection(t, false, true, conflict.patch))
			} else {
				accepted := appStateTestDecode(t, author, first.state, request.GetChildByTag("patch").Content.([]byte))
				peer.respond(t, appStateTestCollection(t, false, false))
				if stage != "ack" {
					peer.request(t, 1, false)
					if stage == "confirmation" {
						s.beforeGetMAC = func() {
							if cli.RegularLowPolicyStatus().FetchComplete {
								cli.regularLow.disconnected()
							}
						}
					}
					if stage == "postfetch" {
						cli.regularLow.disconnected()
					}
					peer.respond(t, appStateTestCollection(t, false, false, accepted.patch))
				}
			}
			result := recoveryTestAwait(t, done)
			var preflight *RegularLowSendPreflightError
			if !errors.Is(result.err, ErrRegularLowFetchRequired) || errors.As(result.err, &preflight) || len(result.events) != 0 {
				t.Fatalf("submitted send crossed generation: %+v", result)
			}
			if status := cli.RegularLowPolicyStatus(); status.FetchComplete || status.Valid || cli.RegularLowChatProjection(lowTestJID).PinKnown {
				t.Fatalf("stale send published known state: %+v", status)
			}
			select {
			case <-peer.requests:
				t.Fatal("stale send retried or fetched after refusal")
			default:
			}
			for len(dispatched) > 0 {
				if _, ok := (<-dispatched).(*events.AppStateSyncError); !ok {
					t.Fatal("owned product event was globally dispatched")
				}
			}
		})
	}
}
