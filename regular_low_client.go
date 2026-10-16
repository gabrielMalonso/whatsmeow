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
	"fmt"
	"slices"
	"sort"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/proto/waServerSync"
	"go.mau.fi/whatsmeow/proto/waSyncAction"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
)

var ErrRegularLowAnchorRequired = errors.New("regular_low requires explicit full sync with an authenticated snapshot")
var ErrRegularLowSendUnconfirmed = errors.New("regular_low send outcome unconfirmed by authenticated fetched records")
var ErrRegularLowFetchRequired = errors.New("regular_low requires a completed delta fetch before send")
var ErrRegularLowSessionInvalid = errors.New("regular_low session invalid; explicit reanchor required")
var ErrRegularLowProjectionUnknown = errors.New("regular_low requested projection is unknown or ambiguous")

// RegularLowSendPreflightError means this send invocation submitted no mutating
// app-state IQ. It does not roll back earlier fetches or external operations.
// Once an IQ is attempted, including a 409 followed by a refused retry, errors
// are never wrapped in this type.
type RegularLowSendPreflightError struct {
	Err error
}

func (e *RegularLowSendPreflightError) Error() string {
	return fmt.Sprintf("regular_low send refused before submission: %v", e.Err)
}

func (e *RegularLowSendPreflightError) Unwrap() error { return e.Err }

// RegularLowPolicyStatus describes the in-memory session. FetchComplete covers
// only the last local fetch/projection, not freshness, canonical aggregate
// authentication, downstream persistence or operation admission. Version is the
// last complete Decode result; a failed native write may have advanced the store.
// Disconnect supersedes in-flight fetch/send results. A later delta fetch can
// reuse a valid anchor; a failed submitted send requires explicit reanchoring.
type RegularLowPolicyStatus struct {
	Enabled               bool   `json:"enabled"`
	Anchored              bool   `json:"anchored"`
	Valid                 bool   `json:"valid"`
	FetchComplete         bool   `json:"fetch_complete"`
	Degraded              bool   `json:"degraded"`
	Version               uint64 `json:"version"`
	SnapshotMACMismatches int    `json:"snapshot_mac_mismatches"`
}

// RegularLowChatProjection exposes only known current archive/pin/read fields.
// Missing, removed, malformed or physically ambiguous records are unknown.
type RegularLowChatProjection struct {
	ArchiveKnown     bool
	PinKnown         bool
	ReadKnown        bool
	Archived         bool
	Pinned           bool
	Read             bool
	ReadAction       *waSyncAction.MarkChatAsReadAction
	ArchiveAmbiguous bool
	PinAmbiguous     bool
	ReadAmbiguous    bool
	Ambiguous        bool
}

type regularLowClient struct {
	mu     sync.RWMutex // Status/getters never acquire the fetch/send lock.
	status RegularLowPolicyStatus
	// Protected by mu; disconnect supersedes the whole fetch/send operation.
	generation uint64
	session    *appstate.RegularLowDecodeSession // Only appStateSyncLock writers.
	state      appstate.HashState
	active     map[string]appstate.Mutation // Supported product projection, not crypto map.
	chat       map[types.JID]RegularLowChatProjection
}

// NewClientWithRegularLowPolicy selects an immutable regular_low policy. Other
// collections and NewClient remain strict. AllowMismatch requires an explicit
// FetchAppState(fullSync=true) with a strictly authenticated snapshot before use,
// including after restart. No existing store cursor becomes an anchor.
//
// The Client must exclusively own the app-state store during this session. Before
// external writes, callers must invalidate the policy. Cursor checks detect stale
// version/hash, but the native store has no CAS or detection of map-only writes.
// Native per-patch/page and ChatSettings writes are non-atomic and are not rolled
// back after failure; FetchComplete does not certify downstream persistence.
func NewClientWithRegularLowPolicy(device *store.Device, log waLog.Logger, policy appstate.PatchSnapshotPolicy) (*Client, error) {
	if policy != appstate.PatchSnapshotStrict && policy != appstate.PatchSnapshotAllowMismatch {
		return nil, fmt.Errorf("invalid regular_low client policy")
	}
	if policy == appstate.PatchSnapshotAllowMismatch && (device == nil || device.AppState == nil || device.AppStateKeys == nil || device.ChatSettings == nil) {
		return nil, fmt.Errorf("regular_low policy requires app-state, keys and chat-settings stores")
	}
	cli := NewClient(device, log)
	if policy == appstate.PatchSnapshotAllowMismatch {
		cli.regularLow = &regularLowClient{status: RegularLowPolicyStatus{Enabled: true}, active: map[string]appstate.Mutation{}, chat: map[types.JID]RegularLowChatProjection{}}
	}
	return cli, nil
}

// RegularLowPolicyStatus returns a copy; it does not read or reset the store.
func (cli *Client) RegularLowPolicyStatus() RegularLowPolicyStatus {
	if cli == nil || cli.regularLow == nil {
		return RegularLowPolicyStatus{}
	}
	p := cli.regularLow
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.status
}

// RegularLowChatProjection returns unknown while invalid or fetch-incomplete.
// Its read action/range is cloned and can be retained independently of callbacks.
func (cli *Client) RegularLowChatProjection(jid types.JID) RegularLowChatProjection {
	if cli == nil || cli.regularLow == nil {
		return RegularLowChatProjection{}
	}
	p := cli.regularLow
	p.mu.RLock()
	defer p.mu.RUnlock()
	// The last completed projection is not current after invalidation or a
	// partial/failed fetch. This does not invalidate an otherwise usable anchor.
	if !p.status.Valid || !p.status.FetchComplete {
		return RegularLowChatProjection{}
	}
	out := p.chat[jid]
	if out.ReadAction != nil {
		out.ReadAction = proto.Clone(out.ReadAction).(*waSyncAction.MarkChatAsReadAction)
	}
	return out
}

func (p *regularLowClient) incomplete() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.status.FetchComplete = false
	return p.generation
}

func (p *regularLowClient) disconnected() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.generation++
	p.status.FetchComplete = false
}

func (p *regularLowClient) checkGeneration(generation uint64) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.generation != generation {
		return ErrRegularLowFetchRequired
	}
	return nil
}

func (p *regularLowClient) sendGeneration() (uint64, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if !p.status.Anchored {
		return 0, ErrRegularLowAnchorRequired
	}
	if !p.status.Valid {
		return 0, ErrRegularLowSessionInvalid
	}
	if !p.status.FetchComplete {
		return 0, ErrRegularLowFetchRequired
	}
	return p.generation, nil
}

func (p *regularLowClient) invalidate() {
	p.mu.Lock()
	p.status.Valid = false
	p.status.FetchComplete = false
	p.mu.Unlock()
}

func (p *regularLowClient) requireAnchor() error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if !p.status.Anchored {
		return ErrRegularLowAnchorRequired
	}
	if !p.status.Valid {
		return ErrRegularLowSessionInvalid
	}
	return nil
}

// Called only for the caller's explicit fullSync=true AFTER native DeleteVersion
// succeeded. Native reset is deliberately not a side effect of opting in.
func (cli *Client) regularLowReset() error {
	p := cli.regularLow
	session, err := appstate.NewRegularLowDecodeSession(cli.appStateProc, appstate.PatchSnapshotAllowMismatch)
	if err != nil {
		p.invalidate()
		return err
	}
	p.session = session
	p.state = appstate.HashState{}
	p.active = map[string]appstate.Mutation{}
	p.mu.Lock()
	p.status = RegularLowPolicyStatus{Enabled: true, Valid: true}
	p.chat = map[types.JID]RegularLowChatProjection{}
	p.mu.Unlock()
	return nil
}

func (cli *Client) decodeRegularLow(ctx context.Context, list *appstate.PatchList, state appstate.HashState) (appstate.HashState, error) {
	p := cli.regularLow
	if !cli.RegularLowPolicyStatus().Valid {
		return state, ErrRegularLowSessionInvalid
	}
	if p.session == nil {
		return state, ErrRegularLowAnchorRequired
	}
	if state != p.state {
		p.invalidate()
		return state, ErrRegularLowSessionInvalid
	}
	result, err := p.session.Decode(ctx, list)
	if err != nil {
		p.invalidate()
		return state, err
	}
	p.state = result.State
	for _, mutation := range result.Mutations {
		if len(mutation.Index) < 1 {
			continue
		}
		switch mutation.Index[0] {
		case appstate.IndexArchive, appstate.IndexPin, appstate.IndexMarkChatAsRead:
		default:
			continue
		}
		key := string(mutation.IndexMAC)
		if mutation.Operation == waServerSync.SyncdMutation_REMOVE {
			delete(p.active, key)
			continue
		}
		if mutation.Action != nil {
			mutation.Action = proto.Clone(mutation.Action).(*waSyncAction.SyncActionValue)
		}
		mutation.Index = append([]string(nil), mutation.Index...)
		p.active[key] = mutation
	}
	p.mu.Lock()
	p.status.Anchored = result.Integrity.SnapshotVerified
	p.status.Valid = true
	p.status.Degraded = result.Integrity.Degraded
	p.status.Version = result.State.Version
	p.status.SnapshotMACMismatches = result.Integrity.SnapshotMACMismatches
	p.mu.Unlock()
	return result.State, nil
}

type lowProjectionKey struct {
	jid    types.JID
	domain string
}

// Project only current supported state after every page was received/committed.
// No generic AppState, destructive historical callbacks or intermediate effects.
func (cli *Client) projectRegularLow(ctx context.Context, generation uint64) ([]any, error) {
	p := cli.regularLow
	if err := ctx.Err(); err != nil {
		p.invalidate()
		return nil, err
	}
	if err := p.checkGeneration(generation); err != nil {
		return nil, err
	}
	grouped := map[lowProjectionKey][]appstate.Mutation{}
	for _, m := range p.active {
		if len(m.Index) < 2 {
			continue
		}
		jid, err := types.ParseJID(m.Index[1])
		if err != nil || jid.IsEmpty() {
			continue
		}
		key := lowProjectionKey{jid, m.Index[0]}
		grouped[key] = append(grouped[key], m)
	}
	keys := make([]lowProjectionKey, 0, len(grouped))
	for k := range grouped {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		a, b := keys[i], keys[j]
		if a.jid.String() != b.jid.String() {
			return a.jid.String() < b.jid.String()
		}
		return a.domain < b.domain
	})
	chats := map[types.JID]RegularLowChatProjection{}
	var output []any
	for _, k := range keys {
		if err := ctx.Err(); err != nil {
			p.invalidate()
			return nil, err
		}
		rows := grouped[k]
		chat := chats[k.jid]
		if len(rows) != 1 || len(rows[0].Index) != 2 || rows[0].Action == nil {
			markLowAmbiguous(&chat, k.domain)
			chats[k.jid] = chat
			continue
		}
		m := rows[0]
		ts := m.Action.GetTimestamp()
		var event any
		var err error
		switch k.domain {
		case appstate.IndexArchive:
			action := m.Action.GetArchiveChatAction()
			if action == nil || action.Archived == nil {
				markLowAmbiguous(&chat, k.domain)
				break
			}
			chat.ArchiveKnown = true
			chat.Archived = action.GetArchived()
			if cli.Store.ChatSettings == nil {
				err = ErrRegularLowProjectionUnknown
			} else {
				err = cli.Store.ChatSettings.PutArchived(ctx, k.jid, chat.Archived)
			}
			event = &events.Archive{JID: k.jid, Timestamp: time.UnixMilli(ts), Action: proto.Clone(action).(*waSyncAction.ArchiveChatAction), FromFullSync: true}
		case appstate.IndexPin:
			action := m.Action.GetPinAction()
			if action == nil || action.Pinned == nil {
				markLowAmbiguous(&chat, k.domain)
				break
			}
			chat.PinKnown = true
			chat.Pinned = action.GetPinned()
			if cli.Store.ChatSettings == nil {
				err = ErrRegularLowProjectionUnknown
			} else {
				err = cli.Store.ChatSettings.PutPinned(ctx, k.jid, chat.Pinned)
			}
			event = &events.Pin{JID: k.jid, Timestamp: time.UnixMilli(ts), Action: proto.Clone(action).(*waSyncAction.PinAction), FromFullSync: true}
		case appstate.IndexMarkChatAsRead:
			action := m.Action.GetMarkChatAsReadAction()
			if action == nil || action.Read == nil {
				markLowAmbiguous(&chat, k.domain)
				break
			}
			if action.GetRead() && !lowReadBounded(action) {
				markLowAmbiguous(&chat, k.domain)
				break
			}
			chat.ReadKnown = true
			chat.Read = action.GetRead()
			chat.ReadAction = proto.Clone(action).(*waSyncAction.MarkChatAsReadAction)
			event = &events.MarkChatAsRead{JID: k.jid, Timestamp: time.UnixMilli(ts), Action: proto.Clone(action).(*waSyncAction.MarkChatAsReadAction), FromFullSync: true}
		}
		if err != nil {
			p.invalidate()
			return nil, fmt.Errorf("regular_low projection persistence: %w", err)
		}
		chats[k.jid] = chat
		if event != nil {
			output = append(output, event)
		}
	}
	if err := ctx.Err(); err != nil {
		p.invalidate()
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.generation != generation {
		return nil, ErrRegularLowFetchRequired
	}
	p.chat = chats
	p.status.FetchComplete = true
	return output, nil
}

func lowReadBounded(action *waSyncAction.MarkChatAsReadAction) bool {
	r := action.GetMessageRange()
	if r.GetLastMessageTimestamp() > 0 {
		return true
	}
	for _, m := range r.GetMessages() {
		if m.GetTimestamp() > 0 && m.GetKey().GetID() != "" {
			return true
		}
	}
	return false
}

// InvalidateRegularLowPolicy revokes the session without resetting persistent
// state or establishing an anchor. Call before external store writes, outside
// store methods invoked under the Client's fetch/send lock.
func (cli *Client) InvalidateRegularLowPolicy() {
	if cli == nil || cli.regularLow == nil {
		return
	}
	_ = cli.appStateSyncLock.Acquire(context.Background(), 1)
	defer cli.appStateSyncLock.Release(1)
	cli.regularLow.invalidate()
}

func (cli *Client) confirmRegularLowSend(ctx context.Context, encoded []byte, previousVersion uint64) error {
	if cli.regularLow.state.Version <= previousVersion {
		return ErrRegularLowSendUnconfirmed
	}
	version, hash, err := cli.Store.AppState.GetAppStateVersion(ctx, string(appstate.WAPatchRegularLow))
	if err != nil {
		return err
	}
	if (appstate.HashState{Version: version, Hash: hash}) != cli.regularLow.state {
		return ErrRegularLowSessionInvalid
	}
	var patch waServerSync.SyncdPatch
	if proto.Unmarshal(encoded, &patch) != nil {
		return ErrRegularLowSendUnconfirmed
	}
	for _, mutation := range patch.Mutations {
		record := mutation.GetRecord()
		value := record.GetValue().GetBlob()
		if len(value) < 32 {
			return ErrRegularLowSendUnconfirmed
		}
		observed, ok := cli.regularLow.active[string(record.GetIndex().GetBlob())]
		if !ok || observed.PatchVersion <= previousVersion || !bytes.Equal(observed.ValueMAC, value[len(value)-32:]) {
			return ErrRegularLowSendUnconfirmed
		}
		current, err := cli.Store.AppState.GetAppStateMutationMAC(ctx, "regular_low", record.GetIndex().GetBlob())
		if err != nil {
			return err
		}
		if !bytes.Equal(current, value[len(value)-32:]) {
			return ErrRegularLowSendUnconfirmed
		}
	}
	return nil
}

func markLowAmbiguous(chat *RegularLowChatProjection, domain string) {
	chat.Ambiguous = true
	switch domain {
	case appstate.IndexArchive:
		chat.ArchiveAmbiguous = true
	case appstate.IndexPin:
		chat.PinAmbiguous = true
	case appstate.IndexMarkChatAsRead:
		chat.ReadAmbiguous = true
	}
}

// Called under appStateSyncLock, including after a 409 rebase. Inspect the
// authenticated active records directly without publishing intermediate product
// state or relying on the last completed fetch's cached projection.
func (cli *Client) checkRegularLowSendProjection(patch appstate.PatchInfo) error {
	if len(patch.Mutations) == 0 {
		return ErrRegularLowProjectionUnknown
	}
	requested := map[lowProjectionKey]bool{}
	for _, mutation := range patch.Mutations {
		if len(mutation.Index) != 2 || mutation.Value == nil {
			return ErrRegularLowProjectionUnknown
		}
		jid, err := types.ParseJID(mutation.Index[1])
		if err != nil || jid.IsEmpty() {
			return ErrRegularLowProjectionUnknown
		}
		domain := mutation.Index[0]
		key := lowProjectionKey{jid, domain}
		if requested[key] {
			return ErrRegularLowProjectionUnknown
		}
		requested[key] = true
		switch domain {
		case appstate.IndexArchive:
			if action := mutation.Value.GetArchiveChatAction(); action == nil || action.Archived == nil {
				return ErrRegularLowProjectionUnknown
			}
		case appstate.IndexPin:
			if action := mutation.Value.GetPinAction(); action == nil || action.Pinned == nil {
				return ErrRegularLowProjectionUnknown
			}
		case appstate.IndexMarkChatAsRead:
			action := mutation.Value.GetMarkChatAsReadAction()
			if action == nil || action.Read == nil || action.GetRead() && !lowReadBounded(action) {
				return ErrRegularLowProjectionUnknown
			}
		default:
			return ErrRegularLowProjectionUnknown
		}
		count := 0
		for _, row := range cli.regularLow.active {
			if len(row.Index) < 2 || row.Index[0] != domain {
				continue
			}
			rowJID, err := types.ParseJID(row.Index[1])
			if err != nil || rowJID != jid {
				continue
			}
			count++
			if count > 1 || len(row.Index) != 2 || row.Index[1] != mutation.Index[1] || row.Action == nil {
				return ErrRegularLowProjectionUnknown
			}
			switch domain {
			case appstate.IndexArchive:
				if action := row.Action.GetArchiveChatAction(); action == nil || action.Archived == nil {
					return ErrRegularLowProjectionUnknown
				}
			case appstate.IndexPin:
				if action := row.Action.GetPinAction(); action == nil || action.Pinned == nil {
					return ErrRegularLowProjectionUnknown
				}
			case appstate.IndexMarkChatAsRead:
				action := row.Action.GetMarkChatAsReadAction()
				if action == nil || action.Read == nil || action.GetRead() && !lowReadBounded(action) {
					return ErrRegularLowProjectionUnknown
				}
			}
		}
	}
	return nil
}

// A SET under a different physical index would leave the previous logical
// record active. Until explicit key rotation is supported, refuse it before
// sending rather than discovering that ambiguity in the authenticated echo.
func (cli *Client) checkRegularLowEncodedIndexes(patch appstate.PatchInfo, encoded []byte) error {
	var wire waServerSync.SyncdPatch
	if err := proto.Unmarshal(encoded, &wire); err != nil || len(wire.Mutations) != len(patch.Mutations) {
		return ErrRegularLowProjectionUnknown
	}
	for i, mutation := range wire.Mutations {
		indexMAC := mutation.GetRecord().GetIndex().GetBlob()
		for _, previous := range cli.regularLow.active {
			if slices.Equal(previous.Index, patch.Mutations[i].Index) && !bytes.Equal(previous.IndexMAC, indexMAC) {
				return ErrRegularLowProjectionUnknown
			}
		}
	}
	return nil
}
