// Copyright (c) 2026 Gabriel Alonso
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package whatsmeow

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"

	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waSyncdSnapshotRecovery"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// Peer recovery is not a cryptographic anchor for this policy. Classify with
// native gzip/protobuf semantics but bounded work; malformed/unknown refuses.
// Valid other-collection recovery retains native strict handling.
func (cli *Client) quarantineRegularLowRecovery(ctx context.Context, results []*waE2E.PeerDataOperationRequestResponseMessage_PeerDataOperationResult) bool {
	if len(results) != 1 {
		return true
	}
	r := results[0].GetSyncdSnapshotFatalRecoveryResponse()
	if r == nil {
		return true
	}
	raw := r.GetCollectionSnapshot()
	if len(raw) > 16<<20 {
		return true
	}
	if r.GetIsCompressed() {
		reader, err := gzip.NewReader(bytes.NewReader(raw))
		if err != nil {
			return true
		}
		raw, err = io.ReadAll(io.LimitReader(reader, (16<<20)+1))
		closeErr := reader.Close()
		if err != nil || closeErr != nil || len(raw) > 16<<20 {
			return true
		}
	}
	rest := raw
	fields, records := 0, 0
	for len(rest) > 0 {
		if ctx.Err() != nil {
			return true
		}
		fields++
		if fields > 100000 {
			return true
		}
		field, kind, n := protowire.ConsumeTag(rest)
		if n < 0 || kind == protowire.StartGroupType || kind == protowire.EndGroupType {
			return true
		}
		rest = rest[n:]
		if field == 3 && kind == protowire.BytesType {
			value, n := protowire.ConsumeBytes(rest)
			records++
			if n < 0 || records > 20000 || len(value) > 1<<20 {
				return true
			}
		}
		n = protowire.ConsumeFieldValue(field, kind, rest)
		if n < 0 {
			return true
		}
		rest = rest[n:]
	}
	var snapshot waSyncdSnapshotRecovery.SyncdSnapshotRecovery
	if (proto.UnmarshalOptions{RecursionLimit: 64}).Unmarshal(raw, &snapshot) != nil {
		return true
	}
	return !regularLowRecoveryCollectionAllowed(appstate.WAPatchName(snapshot.GetCollectionName()))
}

func regularLowRecoveryCollectionAllowed(name appstate.WAPatchName) bool {
	switch name {
	case appstate.WAPatchCriticalBlock, appstate.WAPatchCriticalUnblockLow, appstate.WAPatchRegularHigh, appstate.WAPatchRegular:
		return true
	default:
		return false // Low, unknown and malformed collections cannot adopt.
	}
}

func (cli *Client) dispatchRegularLowAwareMessage(ctx context.Context, evt *events.Message) bool {
	if cli.regularLow != nil {
		response := evt.Message.GetProtocolMessage().GetPeerDataOperationRequestResponseMessage()
		if response != nil && response.GetPeerDataOperationRequestType() == waE2E.PeerDataOperationRequestType_COMPANION_SYNCD_SNAPSHOT_FATAL_RECOVERY && cli.quarantineRegularLowRecovery(ctx, response.GetPeerDataOperationResult()) {
			return true
		}
	}
	return cli.dispatchEvent(evt)
}
