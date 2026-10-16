// Copyright (c) 2026 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package whatsmeow

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/util/exslices"

	"go.mau.fi/whatsmeow/appstate"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waServerSync"
	"go.mau.fi/whatsmeow/proto/waSyncAction"
	"go.mau.fi/whatsmeow/proto/waSyncdSnapshotRecovery"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

// FetchAppState fetches updates to the given type of app state. If fullSync is true, the current
// cached state will be removed and all app state patches will be re-fetched from the server.
func (cli *Client) FetchAppState(ctx context.Context, name appstate.WAPatchName, fullSync, onlyIfNotSynced bool) error {
	eventsToDispatch, err := cli.fetchAppState(ctx, name, fullSync, onlyIfNotSynced)
	if err != nil {
		return err
	}
	for _, evt := range eventsToDispatch {
		cli.dispatchEvent(evt)
	}
	return nil
}

func (cli *Client) fetchAppState(ctx context.Context, name appstate.WAPatchName, fullSync, onlyIfNotSynced bool) ([]any, error) {
	if cli == nil {
		return nil, ErrClientIsNil
	}
	if err := cli.appStateSyncLock.Acquire(ctx, 1); err != nil {
		return nil, err
	}
	var syncErrors []*events.AppStateSyncError
	var eventsToDispatch []any
	var err error
	func() {
		defer cli.appStateSyncLock.Release(1)
		var generation uint64
		if cli.regularLow != nil && name == appstate.WAPatchRegularLow {
			generation = cli.regularLow.incomplete()
		}
		eventsToDispatch, err = cli.fetchAppStateLocked(ctx, name, fullSync, onlyIfNotSynced, &syncErrors, generation)
	}()
	for _, evt := range syncErrors {
		cli.dispatchEvent(evt)
	}
	return eventsToDispatch, err
}

// fetchAppStateLocked keeps the in-memory hash and stored mutation MACs in the
// same operation, including all pages. Callers dispatch callbacks after unlock.
func (cli *Client) fetchAppStateLocked(ctx context.Context, name appstate.WAPatchName, fullSync, onlyIfNotSynced bool, syncErrors *[]*events.AppStateSyncError, generation uint64) (result []any, returnErr error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	low := cli.regularLow != nil && name == appstate.WAPatchRegularLow
	if low {
		cli.regularLow.incomplete()
		if err := cli.regularLow.checkGeneration(generation); err != nil {
			return nil, err
		}
		errorCount := len(*syncErrors)
		defer func() {
			if returnErr != nil && len(*syncErrors) == errorCount {
				*syncErrors = append(*syncErrors, &events.AppStateSyncError{Name: name, FullSync: fullSync, Error: returnErr})
			}
		}()
		if !fullSync {
			if err := cli.regularLow.requireAnchor(); err != nil {
				return nil, err
			}
		}
	}
	if fullSync {
		if low {
			cli.regularLow.invalidate()
		}
		err := cli.Store.AppState.DeleteAppStateVersion(ctx, string(name))
		if err != nil {
			return nil, fmt.Errorf("failed to reset app state %s version: %w", name, err)
		}
		if low {
			if err := cli.regularLowReset(); err != nil {
				return nil, err
			}
		}
	}
	version, hash, err := cli.Store.AppState.GetAppStateVersion(ctx, string(name))
	if err != nil {
		if low {
			cli.regularLow.invalidate()
		}
		return nil, fmt.Errorf("failed to get app state %s version: %w", name, err)
	}
	if version == 0 {
		fullSync = true
	} else if onlyIfNotSynced && !low {
		return nil, nil
	}

	state := appstate.HashState{Version: version, Hash: hash}
	if low && state != cli.regularLow.state {
		cli.regularLow.invalidate()
		return nil, ErrRegularLowSessionInvalid
	}

	hasMore := true
	wantSnapshot := fullSync
	var eventsToDispatch []any
	eventsToDispatchPtr := &eventsToDispatch
	if fullSync && !cli.EmitAppStateEventsOnFullSync {
		eventsToDispatchPtr = nil
	}
	for hasMore {
		patches, err := cli.fetchAppStatePatches(ctx, name, state.Version, wantSnapshot)
		if err != nil {
			if fullSync && ctx.Err() == nil && cli.BackgroundEventCtx.Err() == nil {
				*syncErrors = append(*syncErrors, &events.AppStateSyncError{Name: name, FullSync: fullSync, Error: err})
			}
			return nil, fmt.Errorf("failed to fetch app state %s patches: %w", name, err)
		} else if !wantSnapshot && patches.Snapshot != nil {
			if low {
				cli.regularLow.invalidate()
			}
			return nil, fmt.Errorf("server unexpectedly returned snapshot for %s without asking", name)
		} else if patches.Snapshot != nil && state != (appstate.HashState{}) {
			if low {
				cli.regularLow.invalidate()
			}
			return nil, fmt.Errorf("unexpected non-empty input state (v%d) for %s when applying snapshot", state.Version, name)
		}
		if low {
			if err := cli.regularLow.checkGeneration(generation); err != nil {
				return nil, err
			}
		}
		wantSnapshot = false
		hasMore = patches.HasMorePatches
		state, err = cli.applyAppStatePatchesNoDispatch(ctx, name, state, patches, fullSync, eventsToDispatchPtr, syncErrors)
		if err != nil {
			return nil, err
		}
	}
	if low {
		return cli.projectRegularLow(ctx, generation)
	}
	if fullSync {
		cli.Log.Debugf("Full sync of app state %s completed. Current version: %d", name, state.Version)
		eventsToDispatch = append(eventsToDispatch, &events.AppStateSyncComplete{Name: name, Version: state.Version})
	} else {
		cli.Log.Debugf("Synced app state %s from version %d to %d", name, version, state.Version)
	}
	return eventsToDispatch, nil
}

func (cli *Client) handleAppStateRecovery(
	ctx context.Context,
	reqID types.MessageID,
	result []*waE2E.PeerDataOperationRequestResponseMessage_PeerDataOperationResult,
) bool {
	if cli.regularLow != nil && cli.quarantineRegularLowRecovery(ctx, result) {
		return true
	}
	if len(result) == 0 || result[0].GetSyncdSnapshotFatalRecoveryResponse() == nil {
		cli.Log.Warnf("No app state recovery data received for %s", reqID)
		return true
	} else if len(result) > 1 {
		cli.Log.Warnf("Unexpected number of app state recovery results for %s: %d", reqID, len(result))
	}
	var eventsToDispatch []any
	eventsToDispatchPtr := &eventsToDispatch
	if !cli.EmitAppStateEventsOnFullSync {
		eventsToDispatchPtr = nil
	}
	snapshot, err := appstate.ParseRecovery(result[0].GetSyncdSnapshotFatalRecoveryResponse())
	if err != nil {
		cli.Log.Warnf("Failed to parse app state recovery blob for %s: %v", reqID, err)
		return true
	}
	name := appstate.WAPatchName(snapshot.GetCollectionName())
	version := snapshot.GetVersion().GetVersion()
	eventsToDispatch = cli.applyAppStateRecovery(ctx, reqID, snapshot, eventsToDispatchPtr)
	if len(eventsToDispatch) == 0 {
		return true
	}
	for _, evt := range eventsToDispatch {
		handlerFailed := cli.dispatchEvent(evt)
		if handlerFailed {
			return false
		}
	}
	cli.Log.Debugf("Finished handling app state recovery response for %s (%s to v%d)", reqID, name, version)
	return true
}

func (cli *Client) applyAppStateRecovery(ctx context.Context, reqID types.MessageID, snapshot *waSyncdSnapshotRecovery.SyncdSnapshotRecovery, eventsToDispatch *[]any) []any {
	// Internal callers cannot bypass the opt-in recovery quarantine.
	if cli.regularLow != nil && !regularLowRecoveryCollectionAllowed(appstate.WAPatchName(snapshot.GetCollectionName())) {
		return nil
	}
	if err := cli.appStateSyncLock.Acquire(ctx, 1); err != nil {
		return nil
	}
	defer cli.appStateSyncLock.Release(1)
	if ctx.Err() != nil {
		return nil
	}
	name := appstate.WAPatchName(snapshot.GetCollectionName())
	version := snapshot.GetVersion().GetVersion()
	currentVersion, _, err := cli.Store.AppState.GetAppStateVersion(ctx, string(name))
	if err != nil {
		cli.Log.Errorf("Failed to get current app state %s version for %s: %v", name, reqID, err)
		return nil
	} else if currentVersion >= version {
		cli.Log.Infof("Ignoring app state recovery response for %s as current version %d is newer than or equal to recovery version %d", reqID, currentVersion, snapshot.GetVersion().GetVersion())
		return nil
	}
	cli.Log.Debugf("Handling app state recovery response for %s", reqID)
	mutations, err := cli.appStateProc.ProcessRecovery(ctx, snapshot)
	if err != nil {
		cli.Log.Warnf("Failed to parse app state recovery blob for %s: %v", reqID, err)
		return nil
	}
	err = cli.collectEventsToDispatch(ctx, name, mutations, true, eventsToDispatch)
	if err != nil {
		cli.Log.Warnf("Failed to collect app state events for %s: %v", reqID, err)
		return nil
	}
	var result []any
	if eventsToDispatch != nil {
		result = *eventsToDispatch
	}
	return append(result, &events.AppStateSyncComplete{Name: name, Version: version, Recovery: true})
}

func (cli *Client) applyAppStatePatches(
	ctx context.Context,
	name appstate.WAPatchName,
	state appstate.HashState,
	patches *appstate.PatchList,
	fullSync bool,
	eventsToDispatch *[]any,
) (appstate.HashState, error) {
	var syncErrors []*events.AppStateSyncError
	var newState appstate.HashState
	var err error
	if cli.regularLow != nil && name == appstate.WAPatchRegularLow {
		if err := cli.appStateSyncLock.Acquire(ctx, 1); err != nil {
			return state, err
		}
		func() {
			defer cli.appStateSyncLock.Release(1)
			// This internal API is not a complete fetch/projection frontier.
			cli.regularLow.incomplete()
			newState, err = cli.applyAppStatePatchesNoDispatch(ctx, name, state, patches, fullSync, eventsToDispatch, &syncErrors)
		}()
	} else {
		newState, err = cli.applyAppStatePatchesNoDispatch(ctx, name, state, patches, fullSync, eventsToDispatch, &syncErrors)
	}
	for _, evt := range syncErrors {
		cli.dispatchEvent(evt)
	}
	return newState, err
}

// applyAppStatePatchesNoDispatch defers sync error callbacks until the caller
// releases its operation lock. The DangerousInternals wrapper dispatches directly.
func (cli *Client) applyAppStatePatchesNoDispatch(
	ctx context.Context,
	name appstate.WAPatchName,
	state appstate.HashState,
	patches *appstate.PatchList,
	fullSync bool,
	eventsToDispatch *[]any,
	syncErrors *[]*events.AppStateSyncError,
) (appstate.HashState, error) {
	if cli.regularLow != nil && name == appstate.WAPatchRegularLow {
		newState, err := cli.decodeRegularLow(ctx, patches, state)
		if err != nil {
			*syncErrors = append(*syncErrors, &events.AppStateSyncError{Name: name, FullSync: fullSync, Error: err})
		}
		return newState, err
	}
	mutations, newState, err := cli.appStateProc.DecodePatches(ctx, patches, state, true)
	if err != nil {
		if errors.Is(err, appstate.ErrKeyNotFound) {
			go cli.requestMissingAppStateKeys(context.WithoutCancel(ctx), patches)
		} else {
			*syncErrors = append(*syncErrors, &events.AppStateSyncError{Name: name, FullSync: fullSync, Error: err})
		}
		return state, fmt.Errorf("failed to decode app state %s patches: %w", name, err)
	}
	return newState, cli.collectEventsToDispatch(ctx, name, mutations, fullSync, eventsToDispatch)
}

func (cli *Client) collectEventsToDispatch(
	ctx context.Context,
	name appstate.WAPatchName,
	mutations []appstate.Mutation,
	fullSync bool,
	eventsToDispatch *[]any,
) error {
	if cli.regularLow != nil && name == appstate.WAPatchRegularLow {
		return fmt.Errorf("regular_low requires current authenticated projection")
	}
	if name == appstate.WAPatchCriticalUnblockLow && fullSync && !cli.EmitAppStateEventsOnFullSync {
		var contacts []store.ContactEntry
		mutations, contacts = cli.filterContacts(mutations)
		cli.Log.Debugf("Mass inserting app state snapshot with %d contacts into the store", len(contacts))
		err := cli.Store.Contacts.PutAllContactNames(ctx, contacts)
		if err != nil {
			// This is a fairly serious failure, so just abort the whole thing
			return fmt.Errorf("failed to update contact store with data from snapshot: %v", err)
		}
	}
	for _, mutation := range mutations {
		if eventsToDispatch != nil && mutation.Operation == waServerSync.SyncdMutation_SET {
			*eventsToDispatch = append(*eventsToDispatch, &events.AppState{Index: mutation.Index, SyncActionValue: mutation.Action})
		}
		evt := cli.dispatchAppState(ctx, name, mutation, fullSync)
		if eventsToDispatch != nil && evt != nil {
			*eventsToDispatch = append(*eventsToDispatch, evt)
		}
	}
	return nil
}

func (cli *Client) filterContacts(mutations []appstate.Mutation) ([]appstate.Mutation, []store.ContactEntry) {
	filteredMutations := mutations[:0]
	contacts := make([]store.ContactEntry, 0, len(mutations))
	for _, mutation := range mutations {
		if mutation.Index[0] == "contact" && len(mutation.Index) > 1 {
			jid, _ := types.ParseJID(mutation.Index[1])
			act := mutation.Action.GetContactAction()
			contacts = append(contacts, store.ContactEntry{
				JID:       jid,
				FirstName: act.GetFirstName(),
				FullName:  act.GetFullName(),
			})
		} else {
			filteredMutations = append(filteredMutations, mutation)
		}
	}
	return filteredMutations, contacts
}

func (cli *Client) dispatchAppState(ctx context.Context, name appstate.WAPatchName, mutation appstate.Mutation, fullSync bool) (eventToDispatch any) {
	if cli.regularLow != nil && name == appstate.WAPatchRegularLow {
		return nil
	}
	logLevel := zerolog.TraceLevel
	log := zerolog.Ctx(ctx)
	if cli.AppStateDebugLogs && log.GetLevel() != zerolog.TraceLevel {
		logLevel = zerolog.DebugLevel
	}
	logEvt := log.WithLevel(logLevel).
		Str("patch_name", string(name)).
		Uint64("patch_version", mutation.PatchVersion).
		Stringer("operation", mutation.Operation).
		Int32("version", mutation.Version).
		Strs("index", mutation.Index).
		Hex("index_mac", mutation.IndexMAC).
		Hex("value_mac", mutation.ValueMAC)
	if logLevel == zerolog.TraceLevel {
		logEvt.Any("action", mutation.Action)
	}
	logEvt.Msg("Received app state mutation")

	if len(mutation.Index) == 1 && mutation.Index[0] == appstate.IndexNCTSaltSync {
		var err error
		if mutation.Operation == waServerSync.SyncdMutation_SET {
			err = cli.storeNCTSalt(ctx, mutation.Action.GetNctSaltSyncAction().GetSalt())
		} else if mutation.Operation == waServerSync.SyncdMutation_REMOVE {
			err = cli.clearNCTSalt(ctx)
		}
		if err != nil {
			cli.Log.Warnf("Failed to update NCT salt from app state mutation: %v", err)
		}
		return
	}

	if mutation.Operation != waServerSync.SyncdMutation_SET {
		return
	}

	var jid types.JID
	if len(mutation.Index) > 1 {
		jid, _ = types.ParseJID(mutation.Index[1])
	}
	ts := time.UnixMilli(mutation.Action.GetTimestamp())

	var storeUpdateError error
	switch mutation.Index[0] {
	case appstate.IndexMute:
		act := mutation.Action.GetMuteAction()
		eventToDispatch = &events.Mute{JID: jid, Timestamp: ts, Action: act, FromFullSync: fullSync}
		var mutedUntil time.Time
		if act.GetMuted() {
			if act.GetMuteEndTimestamp() < 0 {
				mutedUntil = store.MutedForever
			} else {
				mutedUntil = time.UnixMilli(act.GetMuteEndTimestamp())
			}
		}
		if cli.Store.ChatSettings != nil {
			storeUpdateError = cli.Store.ChatSettings.PutMutedUntil(ctx, jid, mutedUntil)
		}
	case appstate.IndexPin:
		act := mutation.Action.GetPinAction()
		eventToDispatch = &events.Pin{JID: jid, Timestamp: ts, Action: act, FromFullSync: fullSync}
		if cli.Store.ChatSettings != nil {
			storeUpdateError = cli.Store.ChatSettings.PutPinned(ctx, jid, act.GetPinned())
		}
	case appstate.IndexArchive:
		act := mutation.Action.GetArchiveChatAction()
		eventToDispatch = &events.Archive{JID: jid, Timestamp: ts, Action: act, FromFullSync: fullSync}
		if cli.Store.ChatSettings != nil {
			storeUpdateError = cli.Store.ChatSettings.PutArchived(ctx, jid, act.GetArchived())
		}
	case appstate.IndexContact:
		act := mutation.Action.GetContactAction()
		eventToDispatch = &events.Contact{JID: jid, Timestamp: ts, Action: act, FromFullSync: fullSync}
		if cli.Store.Contacts != nil {
			storeUpdateError = cli.Store.Contacts.PutContactName(ctx, jid, act.GetFirstName(), act.GetFullName())
		}
	case appstate.IndexClearChat:
		act := mutation.Action.GetClearChatAction()
		var deleteMedia bool
		// TODO what's index 2 here?
		if len(mutation.Index) > 3 && mutation.Index[3] == "1" {
			deleteMedia = true
		}
		eventToDispatch = &events.ClearChat{
			JID:          jid,
			Timestamp:    ts,
			Action:       act,
			DeleteMedia:  deleteMedia,
			FromFullSync: fullSync,
		}
	case appstate.IndexDeleteChat:
		act := mutation.Action.GetDeleteChatAction()
		var deleteMedia bool
		if len(mutation.Index) > 2 && mutation.Index[2] == "1" {
			deleteMedia = true
		}
		eventToDispatch = &events.DeleteChat{
			JID:          jid,
			Timestamp:    ts,
			Action:       act,
			DeleteMedia:  deleteMedia,
			FromFullSync: fullSync,
		}
	case appstate.IndexStar:
		if len(mutation.Index) < 5 {
			return
		}
		evt := events.Star{
			ChatJID:      jid,
			MessageID:    mutation.Index[2],
			Timestamp:    ts,
			Action:       mutation.Action.GetStarAction(),
			IsFromMe:     mutation.Index[3] == "1",
			FromFullSync: fullSync,
		}
		if mutation.Index[4] != "0" {
			evt.SenderJID, _ = types.ParseJID(mutation.Index[4])
		}
		eventToDispatch = &evt
	case appstate.IndexDeleteMessageForMe:
		if len(mutation.Index) < 5 {
			return
		}
		evt := events.DeleteForMe{
			ChatJID:      jid,
			MessageID:    mutation.Index[2],
			Timestamp:    ts,
			Action:       mutation.Action.GetDeleteMessageForMeAction(),
			IsFromMe:     mutation.Index[3] == "1",
			FromFullSync: fullSync,
		}
		if mutation.Index[4] != "0" {
			evt.SenderJID, _ = types.ParseJID(mutation.Index[4])
		}
		eventToDispatch = &evt
	case appstate.IndexMarkChatAsRead:
		eventToDispatch = &events.MarkChatAsRead{
			JID:          jid,
			Timestamp:    ts,
			Action:       mutation.Action.GetMarkChatAsReadAction(),
			FromFullSync: fullSync,
		}
	case appstate.IndexSettingPushName:
		eventToDispatch = &events.PushNameSetting{
			Timestamp:    ts,
			Action:       mutation.Action.GetPushNameSetting(),
			FromFullSync: fullSync,
		}
		cli.Store.PushName = mutation.Action.GetPushNameSetting().GetName()
		err := cli.Store.Save(ctx)
		if err != nil {
			cli.Log.Errorf("Failed to save device store after updating push name: %v", err)
		}
	case appstate.IndexSettingUnarchiveChats:
		eventToDispatch = &events.UnarchiveChatsSetting{
			Timestamp:    ts,
			Action:       mutation.Action.GetUnarchiveChatsSetting(),
			FromFullSync: fullSync,
		}
	case appstate.IndexUserStatusMute:
		eventToDispatch = &events.UserStatusMute{
			JID:          jid,
			Timestamp:    ts,
			Action:       mutation.Action.GetUserStatusMuteAction(),
			FromFullSync: fullSync,
		}
	case appstate.IndexLabelEdit:
		act := mutation.Action.GetLabelEditAction()
		eventToDispatch = &events.LabelEdit{
			Timestamp:    ts,
			LabelID:      mutation.Index[1],
			Action:       act,
			FromFullSync: fullSync,
		}
	case appstate.IndexLabelAssociationChat:
		if len(mutation.Index) < 3 {
			return
		}
		jid, _ = types.ParseJID(mutation.Index[2])
		act := mutation.Action.GetLabelAssociationAction()
		eventToDispatch = &events.LabelAssociationChat{
			JID:          jid,
			Timestamp:    ts,
			LabelID:      mutation.Index[1],
			Action:       act,
			FromFullSync: fullSync,
		}
	case appstate.IndexLabelAssociationMessage:
		if len(mutation.Index) < 6 {
			return
		}
		jid, _ = types.ParseJID(mutation.Index[2])
		act := mutation.Action.GetLabelAssociationAction()
		eventToDispatch = &events.LabelAssociationMessage{
			JID:          jid,
			Timestamp:    ts,
			LabelID:      mutation.Index[1],
			MessageID:    mutation.Index[3],
			Action:       act,
			FromFullSync: fullSync,
		}
	case appstate.IndexWasaRootSecretAction:
		if len(mutation.Index) < 2 {
			return
		}
		botJID, _ := types.ParseJID(mutation.Index[1])
		ownLID := cli.getOwnLID()
		inputSecrets := mutation.Action.GetWasaRootSecretAction().GetSecrets()
		ids := make([]string, 0, len(inputSecrets))
		storeUpdateError = cli.Store.MsgSecrets.PutMessageSecrets(ctx, exslices.CastFunc(inputSecrets, func(secret *waSyncAction.WASARootSecretAction_RootSecretEntry) store.MessageSecretInsert {
			ids = append(ids, secret.GetID())
			return store.MessageSecretInsert{
				Chat:   botJID,
				Sender: ownLID,
				ID:     secret.GetID(),
				Secret: secret.GetRootSecret(),
			}
		}))
		if storeUpdateError == nil && cli.Store.ChatSettings != nil {
			var active *waSyncAction.WASARootSecretAction_RootSecretEntry
			for _, secret := range inputSecrets {
				if secret.GetStatus() == waSyncAction.WASARootSecretAction_RootSecretEntry_ACTIVE && (active == nil || secret.GetEpoch() > active.GetEpoch()) {
					active = secret
				}
			}
			storeUpdateError = cli.Store.ChatSettings.PutWASARootSecretID(ctx, botJID, types.MessageID(active.GetID()))
		}
		if storeUpdateError == nil {
			zerolog.Ctx(ctx).Debug().
				Strs("ids", ids).
				Stringer("bot_jid", botJID).
				Msg("Stored WASA root secrets from app state")
		}
	}
	if storeUpdateError != nil {
		cli.Log.Errorf("Failed to update device store after app state mutation: %v", storeUpdateError)
	}
	return
}

func (cli *Client) downloadExternalAppStateBlob(ctx context.Context, ref *waServerSync.ExternalBlobReference) ([]byte, error) {
	return cli.Download(ctx, ref)
}

func (cli *Client) fetchAppStatePatches(ctx context.Context, name appstate.WAPatchName, fromVersion uint64, snapshot bool) (*appstate.PatchList, error) {
	attrs := waBinary.Attrs{
		"name":            string(name),
		"return_snapshot": snapshot,
	}
	if !snapshot {
		attrs["version"] = fromVersion
	}
	resp, err := cli.sendIQ(ctx, infoQuery{
		Namespace: "w:sync:app:state",
		NoRetry:   cli.regularLow != nil && name == appstate.WAPatchRegularLow,
		Type:      "set",
		To:        types.ServerJID,
		Content: []waBinary.Node{{
			Tag: "sync",
			Content: []waBinary.Node{{
				Tag:   "collection",
				Attrs: attrs,
			}},
		}},
	})
	if err != nil {
		return nil, err
	}
	collection, ok := resp.GetOptionalChildByTag("sync", "collection")
	if !ok {
		if cli.regularLow != nil && name == appstate.WAPatchRegularLow {
			cli.regularLow.invalidate()
		}
		return nil, &ElementMissingError{Tag: "collection", In: "app state patch response"}
	}
	if cli.regularLow != nil && name == appstate.WAPatchRegularLow && collection.AttrGetter().OptionalString("type") == "error" {
		// An IQ refusal precedes decode/store: keep a good anchor for a delta retry.
		return nil, fmt.Errorf("%w: regular_low fetch collection rejected", ErrAppStateUpdate)
	}
	list, err := appstate.ParsePatchList(ctx, &collection, cli.downloadExternalAppStateBlob)
	if err != nil && cli.regularLow != nil && name == appstate.WAPatchRegularLow {
		cli.regularLow.invalidate()
	}
	return list, err
}

func (cli *Client) requestMissingAppStateKeys(ctx context.Context, patches *appstate.PatchList) {
	cli.appStateKeyRequestsLock.Lock()
	rawKeyIDs := cli.appStateProc.GetMissingKeyIDs(ctx, patches)
	filteredKeyIDs := make([][]byte, 0, len(rawKeyIDs))
	now := time.Now()
	for _, keyID := range rawKeyIDs {
		stringKeyID := hex.EncodeToString(keyID)
		lastRequestTime := cli.appStateKeyRequests[stringKeyID]
		if lastRequestTime.IsZero() || lastRequestTime.Add(24*time.Hour).Before(now) {
			cli.appStateKeyRequests[stringKeyID] = now
			filteredKeyIDs = append(filteredKeyIDs, keyID)
		}
	}
	cli.appStateKeyRequestsLock.Unlock()
	cli.requestAppStateKeys(ctx, filteredKeyIDs)
}

func (cli *Client) requestAppStateKeys(ctx context.Context, rawKeyIDs [][]byte) {
	keyIDs := make([]*waE2E.AppStateSyncKeyId, len(rawKeyIDs))
	debugKeyIDs := make([]string, len(rawKeyIDs))
	for i, keyID := range rawKeyIDs {
		keyIDs[i] = &waE2E.AppStateSyncKeyId{KeyID: keyID}
		debugKeyIDs[i] = hex.EncodeToString(keyID)
	}
	msg := &waE2E.Message{
		ProtocolMessage: &waE2E.ProtocolMessage{
			Type: waE2E.ProtocolMessage_APP_STATE_SYNC_KEY_REQUEST.Enum(),
			AppStateSyncKeyRequest: &waE2E.AppStateSyncKeyRequest{
				KeyIDs: keyIDs,
			},
		},
	}
	if len(debugKeyIDs) == 0 {
		return
	}
	cli.Log.Infof("Sending key request for app state keys %+v", debugKeyIDs)
	_, err := cli.SendPeerMessage(ctx, msg)
	if err != nil {
		cli.Log.Warnf("Failed to send app state key request: %v", err)
	}
}

// SendAppState sends the given app state patch, then triggers a background resync of that app state type
// to update local caches and send events for the updates.
//
// You can use the Build methods in the appstate package to build the parameter for this method, e.g.
//
//	cli.SendAppState(ctx, appstate.BuildMute(targetJID, true, 24 * time.Hour))
func (cli *Client) SendAppState(ctx context.Context, patch appstate.PatchInfo) error {
	return cli.sendAppState(ctx, patch, true)
}

func (cli *Client) sendAppState(ctx context.Context, patch appstate.PatchInfo, allowRetry bool) error {
	_, err := cli.sendAppStateEvents(ctx, patch, allowRetry, true)
	return err
}

// SendRegularLowStateEvents returns the exact authenticated post-fetch product
// events from the normal send/409/reencode path. These events are not dispatched
// globally; callers can persist them under their existing ordered write ticket.
// RegularLowSendPreflightError distinguishes refusals before any mutating IQ
// from errors after submission. Neither result rolls back previous fetches.
func (cli *Client) SendRegularLowStateEvents(ctx context.Context, patch appstate.PatchInfo) ([]any, error) {
	if cli == nil {
		return nil, ErrClientIsNil
	}
	if cli.regularLow == nil || patch.Type != appstate.WAPatchRegularLow {
		return nil, fmt.Errorf("regular_low policy required for owned send events")
	}
	return cli.sendAppStateEvents(ctx, patch, true, false)
}

func (cli *Client) sendAppStateEvents(ctx context.Context, patch appstate.PatchInfo, allowRetry, dispatch bool) ([]any, error) {
	if cli == nil {
		return nil, ErrClientIsNil
	}
	if err := cli.appStateSyncLock.Acquire(ctx, 1); err != nil {
		if cli.regularLow != nil && patch.Type == appstate.WAPatchRegularLow {
			return nil, &RegularLowSendPreflightError{Err: err}
		}
		return nil, err
	}
	var syncErrors []*events.AppStateSyncError
	var eventsToDispatch []any
	var err error
	attempted := false
	func() {
		defer cli.appStateSyncLock.Release(1)
		var generation uint64
		if cli.regularLow != nil && patch.Type == appstate.WAPatchRegularLow {
			if generation, err = cli.regularLow.sendGeneration(); err != nil {
				return
			}
			if err = cli.checkRegularLowSendProjection(patch); err != nil {
				return
			}
		}
		err = cli.sendAppStateLocked(ctx, patch, allowRetry, &eventsToDispatch, &syncErrors, &attempted, generation)
	}()
	if err != nil && cli.regularLow != nil && patch.Type == appstate.WAPatchRegularLow && !attempted {
		err = &RegularLowSendPreflightError{Err: err}
	}
	for _, evt := range syncErrors {
		cli.dispatchEvent(evt)
	}
	if dispatch && len(eventsToDispatch) > 0 {
		go func() {
			for _, evt := range eventsToDispatch {
				cli.dispatchEvent(evt)
			}
		}()
	}
	return eventsToDispatch, err
}

func (cli *Client) sendAppStateLocked(ctx context.Context, patch appstate.PatchInfo, allowRetry bool, eventsToDispatch *[]any, syncErrors *[]*events.AppStateSyncError, attempted *bool, generation uint64) (returnErr error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	low := cli.regularLow != nil && patch.Type == appstate.WAPatchRegularLow
	version, hash, err := cli.Store.AppState.GetAppStateVersion(ctx, string(patch.Type))
	if err != nil {
		return err
	}
	if low && (appstate.HashState{Version: version, Hash: hash}) != cli.regularLow.state {
		cli.regularLow.invalidate()
		return ErrRegularLowSessionInvalid
	}
	// TODO create new key instead of reusing the primary client's keys
	latestKeyID, err := cli.Store.AppStateKeys.GetLatestAppStateSyncKeyID(ctx)
	if err != nil {
		return fmt.Errorf("failed to get latest app state key ID: %w", err)
	} else if latestKeyID == nil {
		return fmt.Errorf("no app state keys found, creating app state keys is not yet supported")
	}

	state := appstate.HashState{Version: version, Hash: hash}

	encodedPatch, err := cli.appStateProc.EncodePatch(ctx, latestKeyID, state, patch)
	if err != nil {
		return err
	}
	if low {
		if err := cli.checkRegularLowEncodedIndexes(patch, encodedPatch); err != nil {
			return err
		}
		if err := cli.regularLow.checkGeneration(generation); err != nil {
			return err
		}
		cli.regularLow.incomplete()
		errorCount := len(*syncErrors)
		defer func() {
			if returnErr != nil {
				cli.regularLow.invalidate()
				if len(*syncErrors) == errorCount {
					*syncErrors = append(*syncErrors, &events.AppStateSyncError{Name: patch.Type, Error: returnErr})
				}
			}
		}()
	}

	*attempted = true
	resp, err := cli.sendIQ(ctx, infoQuery{
		Namespace: "w:sync:app:state",
		NoRetry:   low,
		Type:      iqSet,
		To:        types.ServerJID,
		Content: []waBinary.Node{{
			Tag: "sync",
			Content: []waBinary.Node{{
				Tag: "collection",
				Attrs: waBinary.Attrs{
					"name":            string(patch.Type),
					"version":         version,
					"return_snapshot": false,
				},
				Content: []waBinary.Node{{
					Tag:     "patch",
					Content: encodedPatch,
				}},
			}},
		}},
	})
	if err != nil {
		return err
	}

	if low {
		if err := cli.regularLow.checkGeneration(generation); err != nil {
			return err
		}
	}
	respCollection, ok := resp.GetOptionalChildByTag("sync", "collection")
	if !ok {
		return &ElementMissingError{Tag: "collection", In: "app state send response"}
	}
	respCollectionAttr := respCollection.AttrGetter()
	if respCollectionAttr.OptionalString("type") == "error" {
		errorTag, ok := respCollection.GetOptionalChildByTag("error")

		mainErr := fmt.Errorf("%w: %s", ErrAppStateUpdate, &respCollection)
		if ok {
			mainErr = fmt.Errorf("%w (%s): %s", ErrAppStateUpdate, patch.Type, &errorTag)
		}
		if ok && errorTag.AttrGetter().Int("code") == 409 && allowRetry {
			zerolog.Ctx(ctx).Warn().Err(mainErr).Msg("Failed to update app state, trying to apply conflicts and retry")
			var conflictEvents []any
			patches, err := appstate.ParsePatchList(ctx, &respCollection, cli.downloadExternalAppStateBlob)
			if err != nil {
				return fmt.Errorf("%w (also, parsing patches in the response failed: %w)", mainErr, err)
			} else if _, err = cli.applyAppStatePatchesNoDispatch(ctx, patch.Type, state, patches, false, &conflictEvents, syncErrors); err != nil {
				return fmt.Errorf("%w (also, applying patches in the response failed: %w)", mainErr, err)
			} else {
				if low {
					if patches.HasMorePatches {
						return fmt.Errorf("incomplete regular_low conflict response")
					}
					if err := cli.checkRegularLowSendProjection(patch); err != nil {
						return err
					}
				}
				zerolog.Ctx(ctx).Debug().Msg("Retrying app state send after applying conflicting patches")
				*eventsToDispatch = append(*eventsToDispatch, conflictEvents...)
				return cli.sendAppStateLocked(ctx, patch, false, eventsToDispatch, syncErrors, attempted, generation)
			}
		}
		return mainErr
	}
	fetchedEvents, err := cli.fetchAppStateLocked(ctx, patch.Type, false, false, syncErrors, generation)
	if err != nil {
		return fmt.Errorf("failed to fetch app state after sending update: %w", err)
	}
	if low {
		if err := cli.checkRegularLowSendProjection(patch); err != nil {
			return err
		}
		if err := cli.confirmRegularLowSend(ctx, encodedPatch, version); err != nil {
			return err
		}
		if err := cli.regularLow.checkGeneration(generation); err != nil {
			return err
		}
	}
	*eventsToDispatch = append(*eventsToDispatch, fetchedEvents...)

	return nil
}

func (cli *Client) MarkNotDirty(ctx context.Context, cleanType string, ts time.Time) error {
	_, err := cli.sendIQ(ctx, infoQuery{
		Namespace: "urn:xmpp:whatsapp:dirty",
		Type:      iqSet,
		To:        types.ServerJID,
		Content: []waBinary.Node{{
			Tag: "clean",
			Attrs: waBinary.Attrs{
				"type":      cleanType,
				"timestamp": ts.Unix(),
			},
		}},
	})
	return err
}

// BuildFatalAppStateExceptionNotification builds a message to request the user's primary device
// to reset specific app state collections. This will cause all linked devices to be logged out.
//
// The built message can be sent using Client.SendPeerMessage.
// There is no response, as the client will get logged out.
func BuildFatalAppStateExceptionNotification(collections ...appstate.WAPatchName) *waE2E.Message {
	return &waE2E.Message{
		ProtocolMessage: &waE2E.ProtocolMessage{
			Type: waE2E.ProtocolMessage_APP_STATE_FATAL_EXCEPTION_NOTIFICATION.Enum(),
			AppStateFatalExceptionNotification: &waE2E.AppStateFatalExceptionNotification{
				CollectionNames: exslices.CastToString[string](collections),
				Timestamp:       new(time.Now().UnixMilli()),
			},
		},
	}
}

// BuildAppStateRecoveryRequest builds a message to request the user's primary device to send
// an unencrypted copy of the given app state collection.
//
// The built message can be sent using Client.SendPeerMessage.
// The response will come as a ProtocolMessage with type `PEER_DATA_OPERATION_RESPONSE_MESSAGE`.
func BuildAppStateRecoveryRequest(collection appstate.WAPatchName) *waE2E.Message {
	return &waE2E.Message{
		ProtocolMessage: &waE2E.ProtocolMessage{
			Type: waE2E.ProtocolMessage_PEER_DATA_OPERATION_REQUEST_MESSAGE.Enum(),
			PeerDataOperationRequestMessage: &waE2E.PeerDataOperationRequestMessage{
				PeerDataOperationRequestType: waE2E.PeerDataOperationRequestType_COMPANION_SYNCD_SNAPSHOT_FATAL_RECOVERY.Enum(),
				SyncdCollectionFatalRecoveryRequest: &waE2E.PeerDataOperationRequestMessage_SyncDCollectionFatalRecoveryRequest{
					CollectionName: (*string)(&collection),
					Timestamp:      new(time.Now().Unix()),
				},
			},
		},
	}
}
