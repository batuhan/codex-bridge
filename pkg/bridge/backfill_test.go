package bridge

import (
	"context"
	"encoding/json"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"

	agui "github.com/beeper/ai-bridge/pkg/ag-ui"
	aistream "github.com/beeper/ai-bridge/pkg/ai-stream"
	"github.com/beeper/codex-bridge/pkg/appserver"
)

func TestBackfillAssistantMessageUsesBeeperAI(t *testing.T) {
	client := &Client{UserLogin: &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "codex"}}}
	thread := appserver.Thread{
		ID:            "thread-1",
		ModelProvider: "openai/gpt-5",
		CreatedAt:     100,
		Turns: []appserver.Turn{{
			ID:        "turn-1",
			StartedAt: 100,
			Items: []appserver.TurnItem{
				{ID: "user-1", Type: "userMessage", Content: []appserver.InputPart{{Type: "text", Text: "status"}}},
				{ID: "cmd-1", Type: "commandExecution", Command: "git status", AggregatedOutput: "clean"},
				{ID: "agent-1", Type: "agentMessage", Text: "done"},
			},
		}},
	}
	portal := &bridgev2.Portal{Portal: &database.Portal{PortalKey: networkid.PortalKey{ID: "portal", Receiver: "codex"}}}
	messages, err := client.projectBackfillMessages(context.Background(), portal, thread)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 2 {
		t.Fatalf("expected user and assistant backfill messages, got %d", len(messages))
	}
	userPart := messages[0].ConvertedMessage.Parts[0]
	userMeta, ok := userPart.DBMetadata.(*MessageMetadata)
	if !ok || userMeta.Role != "user" || userMeta.ThreadID != "thread-1" || userMeta.TurnID != "turn-1" || userMeta.StreamStatus != "done" {
		t.Fatalf("user backfill has wrong DB metadata: %#v", userPart.DBMetadata)
	}
	part := messages[1].ConvertedMessage.Parts[0]
	meta, ok := part.DBMetadata.(*MessageMetadata)
	if !ok || meta.Role != "assistant" || meta.ThreadID != "thread-1" || meta.TurnID != "turn-1" || meta.StreamStatus != "complete" {
		t.Fatalf("assistant backfill has wrong DB metadata: %#v", part.DBMetadata)
	}
	ai, ok := part.Extra[aistream.BeeperAIKey].(aistream.BeeperAI)
	if !ok {
		t.Fatalf("assistant backfill missing com.beeper.ai payload: %#v", part.Extra)
	}
	if ai.Kind != aistream.AIKindFinal || ai.Message == nil {
		t.Fatalf("unexpected AI payload: %#v", ai)
	}
	if len(ai.Message.Parts) < 2 {
		t.Fatalf("expected text and tool parts, got %#v", ai.Message.Parts)
	}
	assertNoTurnIDThinkingPart(t, ai, "turn-1")
	assertCodexProfile(t, part.Content)
	if !strings.Contains(part.Content.Body, "done") {
		t.Fatalf("assistant fallback body missing text: %q", part.Content.Body)
	}
}

func TestBackfillAssistantMessageMarksFailedTurn(t *testing.T) {
	client := &Client{UserLogin: &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "codex"}}}
	thread := appserver.Thread{
		ID:            "thread-1",
		ModelProvider: "openai/gpt-5",
		CreatedAt:     100,
		Turns: []appserver.Turn{{
			ID:        "turn-1",
			StartedAt: 100,
			Error: &struct {
				Message string `json:"message"`
			}{Message: "command failed"},
			Items: []appserver.TurnItem{
				{ID: "cmd-1", Type: "commandExecution", Command: "go test ./...", AggregatedOutput: "failed"},
			},
		}},
	}
	portal := &bridgev2.Portal{Portal: &database.Portal{PortalKey: networkid.PortalKey{ID: "portal", Receiver: "codex"}}}
	messages, err := client.projectBackfillMessages(context.Background(), portal, thread)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 {
		t.Fatalf("expected one assistant backfill message, got %d", len(messages))
	}
	part := messages[0].ConvertedMessage.Parts[0]
	meta, ok := part.DBMetadata.(*MessageMetadata)
	if !ok || meta.Role != "assistant" || meta.ThreadID != "thread-1" || meta.TurnID != "turn-1" || meta.StreamStatus != "error" {
		t.Fatalf("failed assistant backfill has wrong DB metadata: %#v", part.DBMetadata)
	}
	ai, ok := part.Extra[aistream.BeeperAIKey].(aistream.BeeperAI)
	if !ok || ai.Kind != aistream.AIKindFinal {
		t.Fatalf("failed assistant backfill missing final com.beeper.ai payload: %#v", part.Extra)
	}
	if !strings.Contains(part.Content.Body, "command failed") {
		t.Fatalf("failed assistant fallback body missing error: %q", part.Content.Body)
	}
}

func TestBackfillAssistantMessageMarksCancelledTurn(t *testing.T) {
	client := &Client{UserLogin: &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "codex"}}}
	thread := appserver.Thread{
		ID:            "thread-1",
		ModelProvider: "openai/gpt-5",
		CreatedAt:     100,
		Turns: []appserver.Turn{{
			ID:        "turn-1",
			Status:    "cancelled",
			StartedAt: 100,
			Items: []appserver.TurnItem{
				{ID: "agent-1", Type: "agentMessage", Text: "partial"},
			},
		}},
	}
	portal := &bridgev2.Portal{Portal: &database.Portal{PortalKey: networkid.PortalKey{ID: "portal", Receiver: "codex"}}}
	messages, err := client.projectBackfillMessages(context.Background(), portal, thread)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 {
		t.Fatalf("expected one assistant backfill message, got %d", len(messages))
	}
	part := messages[0].ConvertedMessage.Parts[0]
	meta, ok := part.DBMetadata.(*MessageMetadata)
	if !ok || meta.StreamStatus != "aborted" {
		t.Fatalf("cancelled assistant backfill has wrong DB metadata: %#v", part.DBMetadata)
	}
	ai, ok := part.Extra[aistream.BeeperAIKey].(aistream.BeeperAI)
	if !ok || len(ai.Events) == 0 || ai.Events[len(ai.Events)-1].Event.Type() != agui.EventRunError || ai.Events[len(ai.Events)-1].Event.Get("code") != agui.FinishReasonCancelled {
		t.Fatalf("cancelled assistant backfill missing cancelled RUN_ERROR: %#v", part.Extra)
	}
}

func TestBackfillAssistantMessageMarksEmptyInterruptedTurn(t *testing.T) {
	client := &Client{UserLogin: &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "codex"}}}
	thread := appserver.Thread{
		ID:            "thread-1",
		ModelProvider: "openai/gpt-5",
		CreatedAt:     100,
		Turns: []appserver.Turn{{
			ID:        "turn-1",
			Status:    "interrupted",
			StartedAt: 100,
		}},
	}
	portal := &bridgev2.Portal{Portal: &database.Portal{PortalKey: networkid.PortalKey{ID: "portal", Receiver: "codex"}}}
	messages, err := client.projectBackfillMessages(context.Background(), portal, thread)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 {
		t.Fatalf("expected one interrupted assistant backfill message, got %d", len(messages))
	}
	part := messages[0].ConvertedMessage.Parts[0]
	meta, ok := part.DBMetadata.(*MessageMetadata)
	if !ok || meta.StreamStatus != "aborted" {
		t.Fatalf("interrupted assistant backfill has wrong DB metadata: %#v", part.DBMetadata)
	}
	if !strings.Contains(part.Content.Body, "Codex turn was interrupted") {
		t.Fatalf("interrupted assistant fallback body missing status: %q", part.Content.Body)
	}
	ai, ok := part.Extra[aistream.BeeperAIKey].(aistream.BeeperAI)
	if !ok || len(ai.Events) == 0 || ai.Events[len(ai.Events)-1].Event.Type() != agui.EventRunError || ai.Events[len(ai.Events)-1].Event.Get("code") != agui.FinishReasonCancelled {
		t.Fatalf("interrupted assistant backfill missing cancelled RUN_ERROR: %#v", part.Extra)
	}
}

func TestBackfillAssistantMessageMarksEmptyFailedTurn(t *testing.T) {
	client := &Client{UserLogin: &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "codex"}}}
	thread := appserver.Thread{
		ID:            "thread-1",
		ModelProvider: "openai/gpt-5",
		CreatedAt:     100,
		Turns: []appserver.Turn{{
			ID:        "turn-1",
			Status:    "failed",
			StartedAt: 100,
		}},
	}
	portal := &bridgev2.Portal{Portal: &database.Portal{PortalKey: networkid.PortalKey{ID: "portal", Receiver: "codex"}}}
	messages, err := client.projectBackfillMessages(context.Background(), portal, thread)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 {
		t.Fatalf("expected one failed assistant backfill message, got %d", len(messages))
	}
	part := messages[0].ConvertedMessage.Parts[0]
	meta, ok := part.DBMetadata.(*MessageMetadata)
	if !ok || meta.StreamStatus != "error" {
		t.Fatalf("failed assistant backfill has wrong DB metadata: %#v", part.DBMetadata)
	}
	if !strings.Contains(part.Content.Body, "Codex turn failed") {
		t.Fatalf("failed assistant fallback body missing status: %q", part.Content.Body)
	}
}

func TestBackfillAssistantMessageSkipsInProgressTurn(t *testing.T) {
	client := &Client{UserLogin: &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "codex"}}}
	thread := appserver.Thread{
		ID:            "thread-1",
		ModelProvider: "openai/gpt-5",
		CreatedAt:     100,
		Turns: []appserver.Turn{{
			ID:        "turn-1",
			Status:    "inProgress",
			StartedAt: 100,
			Items: []appserver.TurnItem{
				{ID: "agent-1", Type: "agentMessage", Text: "partial"},
			},
		}},
	}
	portal := &bridgev2.Portal{Portal: &database.Portal{PortalKey: networkid.PortalKey{ID: "portal", Receiver: "codex"}}}
	messages, err := client.projectBackfillMessages(context.Background(), portal, thread)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 0 {
		t.Fatalf("in-progress assistant turn should not be backfilled as final, got %d messages", len(messages))
	}
}

func TestBackfillAssistantMessageDoesNotUseTurnIDAsThinkingStep(t *testing.T) {
	client := &Client{UserLogin: &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "codex"}}}
	thread := appserver.Thread{
		ID:            "thread-1",
		ModelProvider: "openai/gpt-5",
		CreatedAt:     100,
		Turns: []appserver.Turn{{
			ID:        "turn-1",
			StartedAt: 100,
			Items: []appserver.TurnItem{
				{ID: "agent-1", Type: "agentMessage", Text: "done"},
			},
		}},
	}
	portal := &bridgev2.Portal{Portal: &database.Portal{PortalKey: networkid.PortalKey{ID: "portal", Receiver: "codex"}}}
	messages, err := client.projectBackfillMessages(context.Background(), portal, thread)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 {
		t.Fatalf("expected assistant backfill message, got %d", len(messages))
	}
	part := messages[0].ConvertedMessage.Parts[0]
	ai, ok := part.Extra[aistream.BeeperAIKey].(aistream.BeeperAI)
	if !ok || ai.Message == nil {
		t.Fatalf("assistant backfill missing com.beeper.ai payload: %#v", part.Extra)
	}
	assertNoTurnIDThinkingPart(t, ai, "turn-1")
}

func TestBackfillAssistantMessageUploadsOversizedFinalParts(t *testing.T) {
	intent := &recordingMediaIntent{}
	client := &Client{
		Main:      &Connector{Bridge: &bridgev2.Bridge{Bot: intent}},
		UserLogin: &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "codex"}},
	}
	thread := appserver.Thread{
		ID:            "thread-1",
		ModelProvider: "openai/gpt-5",
		CreatedAt:     100,
		Turns: []appserver.Turn{{
			ID:        "turn-1",
			StartedAt: 100,
			Items: []appserver.TurnItem{
				{ID: "cmd-1", Type: "commandExecution", Command: "cat huge.txt", AggregatedOutput: strings.Repeat("x", aistream.FinalMessageBudgetBytes)},
				{ID: "agent-1", Type: "agentMessage", Text: "done"},
			},
		}},
	}
	portal := &bridgev2.Portal{Portal: &database.Portal{
		PortalKey: networkid.PortalKey{ID: "portal", Receiver: "codex"},
		MXID:      "!room:example.com",
	}}
	messages, err := client.projectBackfillMessages(context.Background(), portal, thread)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 {
		t.Fatalf("expected one assistant backfill message, got %d", len(messages))
	}
	part := messages[0].ConvertedMessage.Parts[0]
	assertCodexProfile(t, part.Content)
	if intent.roomID != portal.MXID {
		t.Fatalf("backfill upload used wrong room: %q", intent.roomID)
	}
	if intent.mimeType != aistream.FinalPartsMediaType {
		t.Fatalf("backfill upload used wrong MIME type: %q", intent.mimeType)
	}
	ai, ok := part.Extra[aistream.BeeperAIKey].(aistream.BeeperAI)
	if !ok {
		t.Fatalf("assistant backfill missing com.beeper.ai payload: %#v", part.Extra)
	}
	if ai.Final == nil || ai.Final.Delivery != "attachment" || ai.Final.PartsComplete {
		t.Fatalf("backfill final metadata should advertise attachment delivery: %#v", ai.Final)
	}
	if ai.Final.PartsRef == nil || ai.Final.PartsRef.URL != string(intent.url) {
		t.Fatalf("missing uploaded backfill parts ref: %#v", ai.Final)
	}
	if ai.Message == nil || len(ai.Message.Parts) != 0 {
		t.Fatalf("inline backfill final parts should be empty after upload: %#v", ai.Message)
	}
	var payload aistream.FinalPartsPayload
	if err := json.Unmarshal(intent.data, &payload); err != nil {
		t.Fatalf("uploaded backfill payload is not final parts JSON: %v", err)
	}
	if payload.Schema != aistream.FinalPartsPayloadSchema || payload.ThreadID != thread.ID || payload.RunID != "turn-1" || payload.MessageID == "" {
		t.Fatalf("bad uploaded backfill final parts payload: %#v", payload)
	}
}

func TestBackfillUserMessageUploadsOversizedBody(t *testing.T) {
	intent := &recordingMediaIntent{}
	client := &Client{
		Main:      &Connector{Bridge: &bridgev2.Bridge{Bot: intent}},
		UserLogin: &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "codex"}},
	}
	thread := appserver.Thread{
		ID:            "thread-1",
		ModelProvider: "openai/gpt-5",
		CreatedAt:     100,
		Turns: []appserver.Turn{{
			ID:        "turn-1",
			StartedAt: 100,
			Items: []appserver.TurnItem{{
				ID:      "user-1",
				Type:    "userMessage",
				Content: []appserver.InputPart{{Type: "text", Text: strings.Repeat("x", backfillTextEventBudgetBytes+1)}},
			}},
		}},
	}
	portal := &bridgev2.Portal{Portal: &database.Portal{
		PortalKey: networkid.PortalKey{ID: "portal", Receiver: "codex"},
		MXID:      "!room:example.com",
	}}
	messages, err := client.projectBackfillMessages(context.Background(), portal, thread)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 {
		t.Fatalf("expected one user backfill message, got %d", len(messages))
	}
	part := messages[0].ConvertedMessage.Parts[0]
	if part.Content.MsgType != event.MsgFile || part.Content.URL != intent.url || part.Content.FileName == "" {
		t.Fatalf("oversized user backfill should become a file attachment: %#v", part.Content)
	}
	if intent.roomID != portal.MXID || intent.mimeType != "text/plain" || len(intent.data) <= backfillTextEventBudgetBytes {
		t.Fatalf("backfill upload used wrong media data: room=%q mime=%q bytes=%d", intent.roomID, intent.mimeType, len(intent.data))
	}
	if part.Content.Info == nil || part.Content.Info.Size != len(intent.data) {
		t.Fatalf("backfill attachment missing file size: %#v", part.Content.Info)
	}
}

func TestBackfillUserMessageTruncatesOversizedBodyWithoutUpload(t *testing.T) {
	client := &Client{UserLogin: &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "codex"}}}
	thread := appserver.Thread{
		ID:        "thread-1",
		CreatedAt: 100,
		Turns: []appserver.Turn{{
			ID:        "turn-1",
			StartedAt: 100,
			Items: []appserver.TurnItem{{
				ID:      "user-1",
				Type:    "userMessage",
				Content: []appserver.InputPart{{Type: "text", Text: strings.Repeat("x", backfillTextEventBudgetBytes+1)}},
			}},
		}},
	}
	messages, err := client.projectBackfillMessages(context.Background(), nil, thread)
	if err != nil {
		t.Fatal(err)
	}
	part := messages[0].ConvertedMessage.Parts[0]
	if part.Content.MsgType != event.MsgText || !strings.Contains(part.Content.Body, backfillOversizedUserMessageNotice) || len([]byte(part.Content.Body)) > backfillTextEventBudgetBytes {
		t.Fatalf("oversized user backfill fallback should be truncated text: type=%s bytes=%d body=%q", part.Content.MsgType, len([]byte(part.Content.Body)), part.Content.Body)
	}
}

func TestBackfillUserMessageUsesCodexClientID(t *testing.T) {
	client := &Client{UserLogin: &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "codex"}}}
	thread := appserver.Thread{
		ID:            "thread-1",
		ModelProvider: "openai/gpt-5",
		CreatedAt:     100,
		Turns: []appserver.Turn{{
			ID:        "turn-1",
			StartedAt: 100,
			Items: []appserver.TurnItem{
				{ID: "item-1", ClientID: "$event:beeper.local", Type: "userMessage", Content: []appserver.InputPart{{Type: "text", Text: "status"}}},
			},
		}},
	}
	portal := &bridgev2.Portal{Portal: &database.Portal{PortalKey: networkid.PortalKey{ID: "portal", Receiver: "codex"}}}
	messages, err := client.projectBackfillMessages(context.Background(), portal, thread)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 {
		t.Fatalf("expected one user message, got %d", len(messages))
	}
	if messages[0].ID != "user:$event:beeper.local" || messages[0].TxnID != "user:$event:beeper.local" {
		t.Fatalf("backfilled user message did not use clientId: id=%q txn=%q", messages[0].ID, messages[0].TxnID)
	}
}

func TestBackfillContextCompactionProducesVisibleAIText(t *testing.T) {
	client := &Client{UserLogin: &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "codex"}}}
	thread := appserver.Thread{
		ID:            "thread-1",
		ModelProvider: "openai/gpt-5",
		CreatedAt:     100,
		Turns: []appserver.Turn{{
			ID:        "turn-1",
			StartedAt: 100,
			Items: []appserver.TurnItem{
				{ID: "compact-1", Type: "contextCompaction"},
			},
		}},
	}
	portal := &bridgev2.Portal{Portal: &database.Portal{PortalKey: networkid.PortalKey{ID: "portal", Receiver: "codex"}}}
	messages, err := client.projectBackfillMessages(context.Background(), portal, thread)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 {
		t.Fatalf("expected assistant compaction backfill message, got %d", len(messages))
	}
	part := messages[0].ConvertedMessage.Parts[0]
	ai, ok := part.Extra[aistream.BeeperAIKey].(aistream.BeeperAI)
	if !ok || ai.Message == nil {
		t.Fatalf("compaction backfill missing com.beeper.ai payload: %#v", part.Extra)
	}
	if !strings.Contains(part.Content.Body, codexCompactionNotice) {
		t.Fatalf("fallback body missing compaction notice: %q", part.Content.Body)
	}
	assertCodexProfile(t, part.Content)
	if !aiMessageHasText(ai, codexCompactionNotice) {
		t.Fatalf("AI parts missing compaction notice: %#v", ai.Message.Parts)
	}
}

func TestBackfillReasoningArraysFromThreadRead(t *testing.T) {
	var item appserver.TurnItem
	if err := json.Unmarshal([]byte(`{
		"id":"reason-1",
		"type":"reasoning",
		"summary":["checked status"],
		"content":["read files","picked fix"]
	}`), &item); err != nil {
		t.Fatal(err)
	}
	thread := appserver.Thread{
		ID:            "thread-1",
		ModelProvider: "openai/gpt-5",
		CreatedAt:     100,
		Turns: []appserver.Turn{{
			ID:        "turn-1",
			StartedAt: 100,
			Items:     []appserver.TurnItem{item, {ID: "agent-1", Type: "agentMessage", Text: "done"}},
		}},
	}
	client := &Client{UserLogin: &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "codex"}}}
	portal := &bridgev2.Portal{Portal: &database.Portal{PortalKey: networkid.PortalKey{ID: "portal", Receiver: "codex"}}}
	messages, err := client.projectBackfillMessages(context.Background(), portal, thread)
	if err != nil {
		t.Fatal(err)
	}
	ai := messages[0].ConvertedMessage.Parts[0].Extra[aistream.BeeperAIKey].(aistream.BeeperAI)
	for _, text := range []string{"checked status", "read files", "picked fix"} {
		if !aiMessageHasText(ai, text) {
			t.Fatalf("AI parts missing reasoning text %q: %#v", text, ai.Message.Parts)
		}
	}
}

func TestBackfillReviewModeProducesVisibleAIText(t *testing.T) {
	client := &Client{UserLogin: &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "codex"}}}
	thread := appserver.Thread{
		ID:            "thread-1",
		ModelProvider: "openai/gpt-5",
		CreatedAt:     100,
		Turns: []appserver.Turn{{
			ID:        "turn-1",
			StartedAt: 100,
			Items: []appserver.TurnItem{
				{ID: "review-1", Type: "enteredReviewMode", Review: "Inspect auth flow"},
			},
		}},
	}
	portal := &bridgev2.Portal{Portal: &database.Portal{PortalKey: networkid.PortalKey{ID: "portal", Receiver: "codex"}}}
	messages, err := client.projectBackfillMessages(context.Background(), portal, thread)
	if err != nil {
		t.Fatal(err)
	}
	part := messages[0].ConvertedMessage.Parts[0]
	if !strings.Contains(part.Content.Body, codexEnteredReviewNotice) || !strings.Contains(part.Content.Body, "Inspect auth flow") {
		t.Fatalf("review mode fallback body missing visible text: %q", part.Content.Body)
	}
}

func TestBackfillMessagesHaveMonotonicStreamOrder(t *testing.T) {
	client := &Client{UserLogin: &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "codex"}}}
	thread := appserver.Thread{
		ID:            "thread-1",
		ModelProvider: "openai/gpt-5",
		CreatedAt:     100,
		Turns: []appserver.Turn{
			{
				ID:        "turn-1",
				StartedAt: 100,
				Items: []appserver.TurnItem{
					{ID: "user-1", Type: "userMessage", Content: []appserver.InputPart{{Type: "text", Text: "first"}}},
					{ID: "agent-1", Type: "agentMessage", Text: "done"},
				},
			},
			{
				ID:        "turn-2",
				StartedAt: 100,
				Items: []appserver.TurnItem{
					{ID: "user-2", Type: "userMessage", Content: []appserver.InputPart{{Type: "text", Text: "second"}}},
					{ID: "agent-2", Type: "agentMessage", Text: "done again"},
				},
			},
		},
	}
	portal := &bridgev2.Portal{Portal: &database.Portal{PortalKey: networkid.PortalKey{ID: "portal", Receiver: "codex"}}}
	messages, err := client.projectBackfillMessages(context.Background(), portal, thread)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 4 {
		t.Fatalf("expected four backfill messages, got %d", len(messages))
	}
	for i, msg := range messages {
		if msg.StreamOrder == 0 {
			t.Fatalf("message %d missing stream order: %#v", i, msg)
		}
		if i > 0 && msg.StreamOrder <= messages[i-1].StreamOrder {
			t.Fatalf("stream order did not increase at %d: %d <= %d", i, msg.StreamOrder, messages[i-1].StreamOrder)
		}
	}
}

func TestDirectoryBackfillMessagesMergesSessionsOldestFirst(t *testing.T) {
	client := &Client{UserLogin: &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "codex"}}}
	portal := &bridgev2.Portal{Portal: &database.Portal{PortalKey: networkid.PortalKey{ID: "portal", Receiver: "codex"}}}
	threads := []appserver.Thread{
		{
			ID:            "thread-new",
			Cwd:           "/tmp/project",
			ModelProvider: "openai/gpt-5",
			CreatedAt:     200,
			Turns: []appserver.Turn{{
				ID:        "turn-new",
				Status:    "completed",
				StartedAt: 200,
				Items: []appserver.TurnItem{
					{ID: "user-new", Type: "userMessage", Content: []appserver.InputPart{{Type: "text", Text: "new prompt"}}},
					{ID: "agent-new", Type: "agentMessage", Text: "new answer"},
				},
			}},
		},
		{
			ID:            "thread-old",
			Cwd:           "/tmp/project",
			ModelProvider: "openai/gpt-5",
			CreatedAt:     100,
			Turns: []appserver.Turn{{
				ID:        "turn-old",
				Status:    "completed",
				StartedAt: 100,
				Items: []appserver.TurnItem{
					{ID: "user-old", Type: "userMessage", Content: []appserver.InputPart{{Type: "text", Text: "old prompt"}}},
					{ID: "agent-old", Type: "agentMessage", Text: "old answer"},
				},
			}},
		},
	}

	messages, err := client.directoryBackfillMessages(context.Background(), portal, threads)
	if err != nil {
		t.Fatal(err)
	}
	if got := backfillMessageIDs(messages); strings.Join(got, ",") != "codex:turn-old:user-old,codex:turn-old:assistant,codex:turn-new:user-new,codex:turn-new:assistant" {
		t.Fatalf("directory backfill did not merge sessions oldest first: %#v", got)
	}
	for i, msg := range messages {
		if i > 0 && msg.StreamOrder <= messages[i-1].StreamOrder {
			t.Fatalf("directory backfill stream order did not increase at %d: %d <= %d", i, msg.StreamOrder, messages[i-1].StreamOrder)
		}
	}
}

func TestBackfillThreadPageOnlyUploadsSelectedPage(t *testing.T) {
	intent := &recordingMediaIntent{}
	client := &Client{
		Main:      &Connector{Bridge: &bridgev2.Bridge{Bot: intent}},
		UserLogin: &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "codex"}},
	}
	portal := &bridgev2.Portal{Portal: &database.Portal{
		PortalKey: networkid.PortalKey{ID: "portal", Receiver: "codex"},
		MXID:      "!room:example.com",
	}}
	threads := []appserver.Thread{
		{
			ID:            "thread-old",
			Cwd:           "/tmp/project",
			ModelProvider: "openai/gpt-5",
			CreatedAt:     100,
			Turns: []appserver.Turn{{
				ID:        "turn-old",
				StartedAt: 100,
				Items: []appserver.TurnItem{{
					ID:      "user-old",
					Type:    "userMessage",
					Content: []appserver.InputPart{{Type: "text", Text: strings.Repeat("x", backfillTextEventBudgetBytes+1)}},
				}},
			}},
		},
		{
			ID:            "thread-new",
			Cwd:           "/tmp/project",
			ModelProvider: "openai/gpt-5",
			CreatedAt:     200,
			Turns: []appserver.Turn{{
				ID:        "turn-new",
				StartedAt: 200,
				Items: []appserver.TurnItem{{
					ID:      "user-new",
					Type:    "userMessage",
					Content: []appserver.InputPart{{Type: "text", Text: "new prompt"}},
				}},
			}},
		},
	}

	resp, err := client.backfillThreadPage(context.Background(), portal, threads, bridgev2.FetchMessagesParams{Count: 1})
	if err != nil {
		t.Fatal(err)
	}
	if ids := backfillMessageIDs(resp.Messages); strings.Join(ids, ",") != "codex:turn-new:user-new" {
		t.Fatalf("unexpected selected page IDs: %v", ids)
	}
	if len(intent.data) != 0 {
		t.Fatalf("older oversized backfill message should not be uploaded for newest page, uploaded %d bytes", len(intent.data))
	}
	if !resp.HasMore || resp.Cursor != "1" {
		t.Fatalf("page should still advertise older history: %#v", resp)
	}
}

func TestArchivedThreadDetection(t *testing.T) {
	if !isArchivedThread(appserver.Thread{Raw: map[string]any{"archived": true}}) {
		t.Fatal("archived bool should mark thread archived")
	}
	if !isArchivedThread(appserver.Thread{Raw: map[string]any{"archivedAt": "2026-06-03T00:00:00Z"}}) {
		t.Fatal("archivedAt should mark thread archived")
	}
	if isArchivedThread(appserver.Thread{Raw: map[string]any{"archived": false}}) {
		t.Fatal("non-archived thread was treated as archived")
	}
	if !isDetachedThread(appserver.Thread{Raw: map[string]any{"closed": true}}) {
		t.Fatal("closed thread should be detached")
	}
	if isDetachedThread(appserver.Thread{Raw: map[string]any{"archived": false, "closed": false}}) {
		t.Fatal("active thread was treated as detached")
	}
}

func TestBackfillSortsTurnsOldestFirst(t *testing.T) {
	client := &Client{UserLogin: &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "codex"}}}
	thread := appserver.Thread{
		ID:            "thread-1",
		ModelProvider: "openai/gpt-5",
		CreatedAt:     100,
		Turns: []appserver.Turn{
			{
				ID:        "turn-new",
				StartedAt: 200,
				Items: []appserver.TurnItem{
					{ID: "user-new", Type: "userMessage", Content: []appserver.InputPart{{Type: "text", Text: "new"}}},
					{ID: "agent-new", Type: "agentMessage", Text: "new answer"},
				},
			},
			{
				ID:        "turn-old",
				StartedAt: 100,
				Items: []appserver.TurnItem{
					{ID: "user-old", Type: "userMessage", Content: []appserver.InputPart{{Type: "text", Text: "old"}}},
					{ID: "agent-old", Type: "agentMessage", Text: "old answer"},
				},
			},
		},
	}
	portal := &bridgev2.Portal{Portal: &database.Portal{PortalKey: networkid.PortalKey{ID: "portal", Receiver: "codex"}}}
	messages, err := client.projectBackfillMessages(context.Background(), portal, thread)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 4 {
		t.Fatalf("expected four backfill messages, got %d", len(messages))
	}
	if messages[0].ID != "codex:turn-old:user-old" || messages[1].ID != "codex:turn-old:assistant" || messages[2].ID != "codex:turn-new:user-new" || messages[3].ID != "codex:turn-new:assistant" {
		t.Fatalf("messages were not oldest first: %s, %s, %s, %s", messages[0].ID, messages[1].ID, messages[2].ID, messages[3].ID)
	}
	ai, ok := messages[1].ConvertedMessage.Parts[0].Extra[aistream.BeeperAIKey].(aistream.BeeperAI)
	if !ok || ai.Kind != aistream.AIKindFinal || ai.Message == nil {
		t.Fatalf("assistant backfill should remain a Beeper AI final stream message: %#v", messages[1].ConvertedMessage.Parts[0].Extra)
	}
}

func TestPaginateBackfillMessagesBackwardsUsesCursor(t *testing.T) {
	messages := testBackfillMessages("m1", "m2", "m3", "m4", "m5")
	resp := paginateBackfillMessages(messages, bridgev2.FetchMessagesParams{Count: 2})
	if ids := backfillMessageIDs(resp.Messages); strings.Join(ids, ",") != "m4,m5" {
		t.Fatalf("unexpected first page IDs: %v", ids)
	}
	if !resp.HasMore || resp.Cursor != "3" || resp.ApproxRemainingCount != 3 || resp.ApproxTotalCount != 5 {
		t.Fatalf("unexpected first page metadata: %#v", resp)
	}
	resp = paginateBackfillMessages(messages, bridgev2.FetchMessagesParams{Count: 2, Cursor: resp.Cursor})
	if ids := backfillMessageIDs(resp.Messages); strings.Join(ids, ",") != "m2,m3" {
		t.Fatalf("unexpected second page IDs: %v", ids)
	}
	if !resp.HasMore || resp.Cursor != "1" {
		t.Fatalf("unexpected second page metadata: %#v", resp)
	}
	resp = paginateBackfillMessages(messages, bridgev2.FetchMessagesParams{Count: 2, Cursor: resp.Cursor})
	if ids := backfillMessageIDs(resp.Messages); strings.Join(ids, ",") != "m1" {
		t.Fatalf("unexpected final page IDs: %v", ids)
	}
	if resp.HasMore || resp.Cursor != "" {
		t.Fatalf("unexpected final page metadata: %#v", resp)
	}
}

func TestPaginateBackfillMessagesUsesAnchor(t *testing.T) {
	messages := testBackfillMessages("m1", "m2", "m3", "m4", "m5")
	backward := paginateBackfillMessages(messages, bridgev2.FetchMessagesParams{
		Count:         2,
		AnchorMessage: &database.Message{ID: "m4", Timestamp: messages[3].Timestamp},
	})
	if ids := backfillMessageIDs(backward.Messages); strings.Join(ids, ",") != "m2,m3" {
		t.Fatalf("unexpected backward anchor page IDs: %v", ids)
	}
	backward = paginateBackfillMessages(messages, bridgev2.FetchMessagesParams{
		Count:         2,
		AnchorMessage: &database.Message{ID: "missing", Timestamp: messages[3].Timestamp},
	})
	if ids := backfillMessageIDs(backward.Messages); strings.Join(ids, ",") != "m2,m3" {
		t.Fatalf("unexpected timestamp fallback anchor page IDs: %v", ids)
	}
	forward := paginateBackfillMessages(messages, bridgev2.FetchMessagesParams{
		Forward:       true,
		Count:         2,
		AnchorMessage: &database.Message{ID: "m2", Timestamp: messages[1].Timestamp},
	})
	if ids := backfillMessageIDs(forward.Messages); strings.Join(ids, ",") != "m3,m4" {
		t.Fatalf("unexpected forward anchor page IDs: %v", ids)
	}
	if !forward.Forward || !forward.MarkRead {
		t.Fatalf("unexpected forward metadata: %#v", forward)
	}
	initialForward := paginateBackfillMessages(messages, bridgev2.FetchMessagesParams{Forward: true, Count: 2})
	if ids := backfillMessageIDs(initialForward.Messages); strings.Join(ids, ",") != "m4,m5" {
		t.Fatalf("initial forward page should use newest messages, got %v", ids)
	}
}

func TestPaginateBackfillMessagesMarksReadOnlyForForwardOrInitialImport(t *testing.T) {
	messages := testBackfillMessages("m1", "m2", "m3", "m4", "m5")
	initial := paginateBackfillMessages(messages, bridgev2.FetchMessagesParams{Count: 2})
	if !initial.MarkRead {
		t.Fatalf("initial import should mark read: %#v", initial)
	}
	next := paginateBackfillMessages(messages, bridgev2.FetchMessagesParams{Count: 2, Cursor: initial.Cursor})
	if next.MarkRead {
		t.Fatalf("older backfill pages should not mark read: %#v", next)
	}
	anchored := paginateBackfillMessages(messages, bridgev2.FetchMessagesParams{
		Count:         2,
		AnchorMessage: &database.Message{ID: "m4", Timestamp: messages[3].Timestamp},
	})
	if anchored.MarkRead {
		t.Fatalf("anchored backward backfill should not mark read: %#v", anchored)
	}
	forward := paginateBackfillMessages(messages, bridgev2.FetchMessagesParams{Forward: true, Count: 2})
	if !forward.MarkRead {
		t.Fatalf("forward backfill should mark read: %#v", forward)
	}
}

func TestCodexBackfillMaxBatchCountIsUnlimitedForRooms(t *testing.T) {
	client := &Client{}
	portal := &bridgev2.Portal{Portal: &database.Portal{
		RoomType: database.RoomTypeDM,
		Metadata: &PortalMetadata{ThreadID: "thread-1"},
	}}
	if got := client.GetBackfillMaxBatchCount(context.Background(), portal, nil); got != -1 {
		t.Fatalf("Codex room backfill should be unlimited, got %d", got)
	}
	portal.Metadata = &PortalMetadata{}
	if got := client.GetBackfillMaxBatchCount(context.Background(), portal, nil); got != 0 {
		t.Fatalf("rooms without a Codex thread should not backfill, got %d", got)
	}
	portal.RoomType = database.RoomTypeSpace
	portal.Metadata = &PortalMetadata{ThreadID: "thread-1"}
	if got := client.GetBackfillMaxBatchCount(context.Background(), portal, nil); got != 0 {
		t.Fatalf("space backfill should be disabled, got %d", got)
	}
}

func TestBackfillAssistantUsesGhostSender(t *testing.T) {
	client := &Client{UserLogin: &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "codex"}}}
	thread := appserver.Thread{
		ID:            "thread-1",
		ModelProvider: "openai/gpt-5",
		CreatedAt:     100,
		Turns: []appserver.Turn{{
			ID:        "turn-1",
			StartedAt: 100,
			Items: []appserver.TurnItem{
				{ID: "agent-1", Type: "agentMessage", Text: "done"},
			},
		}},
	}
	portal := &bridgev2.Portal{Portal: &database.Portal{PortalKey: networkid.PortalKey{ID: "portal", Receiver: "codex"}}}
	messages, err := client.projectBackfillMessages(context.Background(), portal, thread)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 {
		t.Fatalf("expected one backfill message, got %d", len(messages))
	}
	if messages[0].Sender.Sender != codexUserID || messages[0].Sender.SenderLogin != "" || messages[0].Sender.IsFromMe {
		t.Fatalf("assistant backfill should use ghost sender only: %#v", messages[0].Sender)
	}
}

func testBackfillMessages(ids ...string) []*bridgev2.BackfillMessage {
	messages := make([]*bridgev2.BackfillMessage, len(ids))
	for i, msgID := range ids {
		messages[i] = &bridgev2.BackfillMessage{
			ID:          networkid.MessageID(msgID),
			TxnID:       networkid.TransactionID(msgID),
			Timestamp:   time.Unix(int64(i+1), 0),
			StreamOrder: int64(i + 1),
		}
	}
	return messages
}

func backfillMessageIDs(messages []*bridgev2.BackfillMessage) []string {
	ids := make([]string, len(messages))
	for i, msg := range messages {
		ids[i] = string(msg.ID)
	}
	return ids
}

func TestBackfillUserBodyIncludesNonTextInputs(t *testing.T) {
	body := backfillUserBody(appserver.TurnItem{Content: []appserver.InputPart{
		{Type: "text", Text: "look at this"},
		{Type: "image", URL: "https://example.com/image.png"},
		{Type: "localImage", Path: "/tmp/image.png"},
		{Type: "skill", Name: "go", Path: "/skills/go"},
		{Type: "mention", Name: "README", Path: "/tmp/README.md"},
	}})
	for _, want := range []string{"look at this", "Image: https://example.com/image.png", "Local image: /tmp/image.png", "Skill: go /skills/go", "Mention: README /tmp/README.md"} {
		if !strings.Contains(body, want) {
			t.Fatalf("body missing %q: %q", want, body)
		}
	}
}

func TestBackfillUserBodySkipsBlankParts(t *testing.T) {
	body := backfillUserBody(appserver.TurnItem{Content: []appserver.InputPart{
		{Type: "text", Text: "  "},
		{Type: "unknown", Text: "ignored"},
		{Type: "text", Text: "keep me"},
	}})
	if body != "keep me" {
		t.Fatalf("unexpected backfill user body: %q", body)
	}
}

func TestBackfillItemDataPreservesRawFields(t *testing.T) {
	var item appserver.TurnItem
	if err := json.Unmarshal([]byte(`{
		"id":"cmd-1",
		"type":"commandExecution",
		"command":"git status",
		"aggregatedOutput":"clean",
		"exitCode":0,
		"nested":{"ok":true}
	}`), &item); err != nil {
		t.Fatal(err)
	}
	data := backfillItemData(item)
	if data["exitCode"] != float64(0) {
		t.Fatalf("raw field was not preserved: %#v", data)
	}
	if nested, _ := data["nested"].(map[string]any); nested == nil || nested["ok"] != true {
		t.Fatalf("nested raw field was not preserved: %#v", data)
	}
}

func TestBackfillItemDataEnrichesGeneratedFieldsWithoutOverwritingRaw(t *testing.T) {
	item := appserver.TurnItem{
		ID:               "item-1",
		Type:             "reasoning",
		Text:             "generated text",
		Summary:          []string{"summary"},
		ReasoningContent: []string{"content"},
		Fragments:        []appserver.PromptFragment{{Text: "fragment"}},
		Arguments:        json.RawMessage(`{"cwd":"/tmp/project"}`),
		Raw: map[string]any{
			"text": "raw text",
		},
	}

	data := backfillItemData(item)
	if data["text"] != "raw text" {
		t.Fatalf("raw text should not be overwritten: %#v", data)
	}
	if data["id"] != "item-1" || data["type"] != "reasoning" {
		t.Fatalf("generated scalar fields were not filled: %#v", data)
	}
	if got, _ := data["summary"].([]string); len(got) != 1 || got[0] != "summary" {
		t.Fatalf("generated summary was not filled: %#v", data)
	}
	if got, _ := data["content"].([]string); len(got) != 1 || got[0] != "content" {
		t.Fatalf("generated reasoning content was not filled: %#v", data)
	}
	if got, _ := data["fragments"].([]appserver.PromptFragment); len(got) != 1 || got[0].Text != "fragment" {
		t.Fatalf("generated fragments were not filled: %#v", data)
	}
	if got, _ := data["arguments"].(json.RawMessage); string(got) != `{"cwd":"/tmp/project"}` {
		t.Fatalf("generated arguments were not filled: %#v", data)
	}
}

func isBackfilledThreadItemType(itemType string) bool {
	switch itemType {
	case "userMessage",
		"agentMessage",
		"contextCompaction",
		"reasoning",
		"hookPrompt",
		"enteredReviewMode",
		"exitedReviewMode",
		"plan",
		"commandExecution",
		"fileChange",
		"mcpToolCall",
		"dynamicToolCall",
		"collabAgentToolCall",
		"webSearch",
		"imageView",
		"imageGeneration":
		return true
	default:
		return false
	}
}

func TestGeneratedCodexThreadItemsAreBackfilled(t *testing.T) {
	raw, err := os.ReadFile(codexGeneratedSchemaPath)
	if err != nil {
		t.Fatalf("read generated Codex schema: %v", err)
	}
	itemTypes := sortedUniqueMatches(regexp.MustCompile(`(?s)type:\s*Annotated\[\s*Literal\["([^"]+)"\].{0,300}?ThreadItemType`), raw)
	if len(itemTypes) == 0 {
		t.Fatal("generated Codex schema did not contain thread item types")
	}
	for _, itemType := range itemTypes {
		if !isBackfilledThreadItemType(itemType) {
			t.Fatalf("generated Codex thread item %q is not covered by backfill", itemType)
		}
	}
}

func TestTypeScriptV2CodexThreadItemsAreBackfilled(t *testing.T) {
	for _, itemType := range generatedTypeScriptThreadItemTypes(t) {
		if !isBackfilledThreadItemType(itemType) {
			t.Fatalf("TypeScript v2 Codex thread item %q is not covered by backfill", itemType)
		}
	}
}

func TestBackfillToolNameAndResultUseRichItemData(t *testing.T) {
	data := map[string]any{
		"id":     "mcp-1",
		"type":   "mcpToolCall",
		"server": "github",
		"tool":   "list_issues",
		"result": map[string]any{"count": float64(2)},
	}
	name := (codexItem{ID: "mcp-1", Type: "mcpToolCall", Raw: data}).Name()
	if name != "github list_issues" {
		t.Fatalf("unexpected tool name: %q", name)
	}
	result := codexToolResultText(data, true)
	if !strings.Contains(result, `"count":2`) {
		t.Fatalf("unexpected tool result: %q", result)
	}

	data["result"] = map[string]any{
		"content":           []any{map[string]any{"type": "text", "text": "found two issues"}},
		"structuredContent": map[string]any{"count": float64(2)},
		"_meta":             nil,
	}
	result = codexToolResultText(data, true)
	if !strings.Contains(result, "found two issues") || !strings.Contains(result, `"structuredContent":{"count":2}`) {
		t.Fatalf("unexpected MCP structured content result: %q", result)
	}

	data = map[string]any{"type": "dynamicToolCall", "tool": "custom_tool", "contentItems": []any{map[string]any{"text": "done"}}}
	if name := (codexItem{Type: "dynamicToolCall", Raw: data}).Name(); name != "custom_tool" {
		t.Fatalf("unexpected dynamic tool name: %q", name)
	}
	if result := codexToolResultText(data, true); !strings.Contains(result, "done") {
		t.Fatalf("unexpected dynamic tool result: %q", result)
	}
}

func TestCodexToolResultMapText(t *testing.T) {
	contentOnly := map[string]any{"content": []any{map[string]any{"text": "found"}}}
	if got := codexToolResultMapText(contentOnly); got != "found" {
		t.Fatalf("unexpected content-only result: %q", got)
	}

	structured := map[string]any{
		"content":           []any{map[string]any{"text": "found"}},
		"structuredContent": map[string]any{"count": float64(2)},
	}
	got := codexToolResultMapText(structured)
	if !strings.Contains(got, "found") || !strings.Contains(got, `"structuredContent":{"count":2}`) {
		t.Fatalf("unexpected structured result: %q", got)
	}
}

func TestIsCompleteToolResultText(t *testing.T) {
	if !isCompleteToolResultText(map[string]any{"message": "done"}, "done", messageToolResultIgnoredFields()...) {
		t.Fatal("message-only result text should be complete")
	}
	if isCompleteToolResultText(map[string]any{"message": "done", "count": float64(2)}, "done", messageToolResultIgnoredFields()...) {
		t.Fatal("result text with additional fields should not be complete")
	}
	if isCompleteToolResultText(map[string]any{"message": "done"}, "", messageToolResultIgnoredFields()...) {
		t.Fatal("empty result text should not be complete")
	}
}

func TestJSONToolResultValueText(t *testing.T) {
	if got := jsonToolResultValueText(nil); got != "" {
		t.Fatalf("nil tool result should be suppressed, got %q", got)
	}
	got := jsonToolResultValueText(map[string]any{"count": float64(2)})
	if got != `{"count":2}` {
		t.Fatalf("unexpected JSON tool result: %q", got)
	}
}

func TestShouldEmitToolCompletionMetadata(t *testing.T) {
	if shouldEmitToolCompletionMetadata(agui.ToolResultStateComplete, false) {
		t.Fatal("metadata should not emit before a visible result")
	}
	if shouldEmitToolCompletionMetadata(agui.ToolResultStateStreaming, true) {
		t.Fatal("metadata should not emit for streaming tool state")
	}
	if !shouldEmitToolCompletionMetadata(agui.ToolResultStateComplete, true) {
		t.Fatal("metadata should emit for completed tool with visible result")
	}
}

func TestHasToolCompletionMetadataExtras(t *testing.T) {
	if hasToolCompletionMetadataExtras(toolStatusResult(agui.ToolResultStateComplete, "completed")) {
		t.Fatal("state/status-only metadata should not count as extra")
	}
	if !hasToolCompletionMetadataExtras(map[string]any{
		"state":    agui.ToolResultStateComplete,
		"status":   "completed",
		"exitCode": float64(0),
	}) {
		t.Fatal("terminal metadata field should count as extra")
	}
}

func TestBackfillToolItemsMapRichGeneratedInputs(t *testing.T) {
	run := aistream.NewRun("turn-1", "thread-1", "codex", "codex", "Codex", time.Unix(0, 0))
	writer := aistream.NewWriter(run, func() time.Time { return time.Unix(0, 0) })
	writer.Start()
	for _, item := range []appserver.TurnItem{
		{
			ID:   "search-1",
			Type: "webSearch",
			Raw:  map[string]any{"id": "search-1", "type": "webSearch", "query": "codex bridge streaming"},
		},
		{
			ID:   "image-1",
			Type: "imageView",
			Raw:  map[string]any{"id": "image-1", "type": "imageView", "path": "/tmp/screenshots/current.png"},
		},
		{
			ID:   "collab-1",
			Type: "collabAgentToolCall",
			Raw:  map[string]any{"id": "collab-1", "type": "collabAgentToolCall", "tool": "spawn", "prompt": "check the stream mapping"},
		},
	} {
		if !mapBackfillItem(writer, run.MessageID, item, nil) {
			t.Fatalf("expected %s item to be backfilled", item.Type)
		}
	}
	if !hasToolCallStartName(run.Events, "search-1", "web search: codex bridge streaming") || !hasToolArgsContaining(run.Events, "search-1", "codex bridge streaming") {
		t.Fatalf("expected rich web search backfill events, got %#v", run.Events)
	}
	if !hasToolCallStartName(run.Events, "image-1", "image view: current.png") || !hasToolArgsContaining(run.Events, "image-1", "/tmp/screenshots/current.png") {
		t.Fatalf("expected rich image view backfill events, got %#v", run.Events)
	}
	if !hasToolCallStartName(run.Events, "collab-1", "collab: spawn") || !hasToolArgsContaining(run.Events, "collab-1", "check the stream mapping") {
		t.Fatalf("expected rich collab backfill events, got %#v", run.Events)
	}
}

func TestBackfillToolItemsMapDynamicAndImageGenerationResults(t *testing.T) {
	run := aistream.NewRun("turn-1", "thread-1", "codex", "codex", "Codex", time.Unix(0, 0))
	writer := aistream.NewWriter(run, func() time.Time { return time.Unix(0, 0) })
	writer.Start()
	for _, item := range []appserver.TurnItem{
		{
			ID:   "dynamic-1",
			Type: "dynamicToolCall",
			Raw: map[string]any{
				"id":           "dynamic-1",
				"type":         "dynamicToolCall",
				"tool":         "custom_tool",
				"arguments":    map[string]any{"query": "codex"},
				"contentItems": []any{map[string]any{"type": "inputText", "text": "dynamic result"}},
			},
		},
		{
			ID:   "image-1",
			Type: "imageGeneration",
			Raw: map[string]any{
				"id":            "image-1",
				"type":          "imageGeneration",
				"status":        "completed",
				"revisedPrompt": "small bridge diagram",
				"result":        "generated image",
				"savedPath":     "/tmp/codex/image.png",
			},
		},
	} {
		if !mapBackfillItem(writer, run.MessageID, item, nil) {
			t.Fatalf("expected %s item to be backfilled", item.Type)
		}
	}

	if !hasToolResultStateContaining(run.Events, "dynamic-1", "dynamic result", agui.ToolResultStateComplete) {
		t.Fatalf("expected dynamic content item text as result, got %#v", run.Events)
	}
	if !hasToolArgsContaining(run.Events, "image-1", "small bridge diagram") || !hasToolResultStateContaining(run.Events, "image-1", "generated image", agui.ToolResultStateComplete) {
		t.Fatalf("expected image generation prompt/result in backfill, got %#v", run.Events)
	}
	assertAGUISequenceValid(t, run)
}

func TestBackfillAssistantRunSequenceIsValid(t *testing.T) {
	run := aistream.NewRun("turn-1", "thread-1", "openai/gpt-5", "codex", "Codex", time.Unix(0, 0))
	writer := aistream.NewWriter(run, func() time.Time { return time.Unix(0, 0) })
	writer.Start()
	writer.StepStart("turn-1")
	for _, item := range []appserver.TurnItem{
		{ID: "cmd-1", Type: "commandExecution", Raw: map[string]any{"id": "cmd-1", "type": "commandExecution", "command": "go test ./...", "status": "completed", "aggregatedOutput": "ok"}},
		{ID: "agent-1", Type: "agentMessage", Text: "done"},
	} {
		if !mapBackfillItem(writer, run.MessageID, item, nil) {
			t.Fatalf("expected %s item to be backfilled", item.Type)
		}
	}
	writer.StepFinish("turn-1")
	writer.Finish(agui.FinishReasonStop)

	assertAGUISequenceValid(t, run)
}

func TestBackfillRawResponseItemsMapAssistantReasoningAndTools(t *testing.T) {
	run := aistream.NewRun("turn-1", "thread-1", "openai/gpt-5", "codex", "Codex", time.Unix(0, 0))
	writer := aistream.NewWriter(run, func() time.Time { return time.Unix(0, 0) })
	writer.Start()
	writer.StepStart("turn-1")
	for _, item := range []appserver.TurnItem{
		{
			ID:   "msg-1",
			Type: "message",
			Raw: map[string]any{
				"id":      "msg-1",
				"type":    "message",
				"role":    "assistant",
				"content": []any{map[string]any{"type": "output_text", "text": "raw answer"}},
			},
		},
		{
			ID:   "reason-1",
			Type: "reasoning",
			Raw: map[string]any{
				"id":      "reason-1",
				"type":    "reasoning",
				"summary": []any{map[string]any{"type": "summary_text", "text": "raw checked"}},
				"content": []any{map[string]any{"type": "reasoning_text", "text": "raw thought"}},
			},
		},
		{
			ID:   "call-1",
			Type: "function_call",
			Raw: map[string]any{
				"type":      "function_call",
				"call_id":   "call-1",
				"name":      "search",
				"arguments": `{"query":"codex"}`,
			},
		},
		{
			ID:   "call-1",
			Type: "function_call_output",
			Raw: map[string]any{
				"type":    "function_call_output",
				"call_id": "call-1",
				"output":  "found",
			},
		},
	} {
		if !mapBackfillItem(writer, run.MessageID, item, nil) {
			t.Fatalf("expected raw %s item to be backfilled", item.Type)
		}
	}
	writer.StepFinish("turn-1")
	writer.Finish(agui.FinishReasonStop)

	if countTextDelta(run.Events, "raw answer") != 1 {
		t.Fatalf("expected raw assistant message in backfill, got %#v", run.Events)
	}
	if countReasoningDelta(run.Events, "raw checked") != 1 || countReasoningDelta(run.Events, "raw thought") != 1 {
		t.Fatalf("expected raw reasoning in backfill, got %#v", run.Events)
	}
	if !hasEventType(run.Events, agui.EventToolCallStart) || !hasToolResult(run.Events, "call-1", "found") {
		t.Fatalf("expected raw tool call/result backfill events, got %#v", run.Events)
	}
	assertAGUISequenceValid(t, run)
}

func TestBackfillReasoningDoesNotHashCollidingItemIDsTogether(t *testing.T) {
	run := aistream.NewRun("turn-1", "thread-1", "openai/gpt-5", "codex", "Codex", time.Unix(0, 0))
	writer := aistream.NewWriter(run, func() time.Time { return time.Unix(0, 0) })
	writer.Start()
	var reasoningSections reasoningSectionState
	for _, item := range []appserver.TurnItem{
		{ID: "a", Type: "reasoning", Raw: map[string]any{"content": []any{"first"}}},
		{ID: "A", Type: "reasoning", Raw: map[string]any{"content": []any{"second"}}},
	} {
		if !mapBackfillItem(writer, run.MessageID, item, &reasoningSections) {
			t.Fatalf("expected reasoning item %q to be backfilled", item.ID)
		}
	}
	writer.Finish(agui.FinishReasonStop)

	ids := distinctReasoningMessageIDs(run.Events)
	if len(ids) != 2 {
		t.Fatalf("distinct backfilled reasoning items should create distinct thinking parts, got %#v events=%#v", ids, run.Events)
	}
	assertAGUISequenceValid(t, run)
}

func TestBackfillRawToolResultSynthesizesMissingStart(t *testing.T) {
	run := aistream.NewRun("turn-1", "thread-1", "openai/gpt-5", "codex", "Codex", time.Unix(0, 0))
	writer := aistream.NewWriter(run, func() time.Time { return time.Unix(0, 0) })
	writer.Start()
	writer.StepStart("turn-1")
	if !mapBackfillItem(writer, run.MessageID, appserver.TurnItem{
		ID:   "call-1",
		Type: "function_call_output",
		Raw:  map[string]any{"type": "function_call_output", "call_id": "call-1", "output": "found"},
	}, nil) {
		t.Fatal("expected output-only raw tool item to be backfilled")
	}
	writer.StepFinish("turn-1")
	writer.Finish(agui.FinishReasonStop)

	if !toolEventsInOrder(run.Events, "call-1", agui.EventToolCallStart, agui.EventToolCallResult) {
		t.Fatalf("expected raw tool result backfill to synthesize start first, got %#v", run.Events)
	}
	assertAGUISequenceValid(t, run)
}

func TestBackfillRawToolResultPreservesStructuredOutput(t *testing.T) {
	run := aistream.NewRun("turn-1", "thread-1", "openai/gpt-5", "codex", "Codex", time.Unix(0, 0))
	writer := aistream.NewWriter(run, func() time.Time { return time.Unix(0, 0) })
	writer.Start()
	writer.StepStart("turn-1")
	if !mapBackfillItem(writer, run.MessageID, appserver.TurnItem{
		ID:   "call-1",
		Type: "function_call_output",
		Raw: map[string]any{
			"type":    "function_call_output",
			"call_id": "call-1",
			"output": map[string]any{
				"content":           []any{map[string]any{"type": "text", "text": "found"}},
				"structuredContent": map[string]any{"count": float64(2)},
			},
		},
	}, nil) {
		t.Fatal("expected structured raw tool output to be backfilled")
	}
	writer.StepFinish("turn-1")
	writer.Finish(agui.FinishReasonStop)

	if !hasToolResultStateContaining(run.Events, "call-1", "found", agui.ToolResultStateComplete) {
		t.Fatalf("expected structured raw tool output text, got %#v", run.Events)
	}
	if !hasToolResultStateContaining(run.Events, "call-1", `"structuredContent":{"count":2}`, agui.ToolResultStateComplete) {
		t.Fatalf("expected structured raw tool output data, got %#v", run.Events)
	}
	assertAGUISequenceValid(t, run)
}

func TestBackfillRawToolResultWithoutOutputOnlySynthesizesStart(t *testing.T) {
	run := aistream.NewRun("turn-1", "thread-1", "openai/gpt-5", "codex", "Codex", time.Unix(0, 0))
	writer := aistream.NewWriter(run, func() time.Time { return time.Unix(0, 0) })
	writer.Start()
	writer.StepStart("turn-1")
	if !mapBackfillItem(writer, run.MessageID, appserver.TurnItem{
		ID:   "call-1",
		Type: "function_call_output",
		Raw:  map[string]any{"type": "function_call_output", "call_id": "call-1"},
	}, nil) {
		t.Fatal("expected output-only raw tool item to be backfilled")
	}
	writer.StepFinish("turn-1")
	writer.Finish(agui.FinishReasonStop)

	if !hasToolCallStart(run.Events, "call-1") {
		t.Fatalf("expected raw tool output without content to synthesize start, got %#v", run.Events)
	}
	if hasEventType(run.Events, agui.EventToolCallResult) || hasToolResult(run.Events, "call-1", "null") {
		t.Fatalf("missing raw tool output should not create a result, got %#v", run.Events)
	}
	assertAGUISequenceValid(t, run)
}

func TestBackfillRawToolSearchOutputMapsToolsResult(t *testing.T) {
	run := aistream.NewRun("turn-1", "thread-1", "openai/gpt-5", "codex", "Codex", time.Unix(0, 0))
	writer := aistream.NewWriter(run, func() time.Time { return time.Unix(0, 0) })
	writer.Start()
	writer.StepStart("turn-1")
	if !mapBackfillItem(writer, run.MessageID, appserver.TurnItem{
		ID:   "search-1",
		Type: "tool_search_output",
		Raw: map[string]any{
			"type":      "tool_search_output",
			"call_id":   "search-1",
			"status":    "completed",
			"execution": "tool_search",
			"tools":     []any{map[string]any{"name": "github", "description": "GitHub connector"}},
		},
	}, nil) {
		t.Fatal("expected tool search output to be backfilled")
	}
	writer.StepFinish("turn-1")
	writer.Finish(agui.FinishReasonStop)

	if !hasToolResultStateContaining(run.Events, "search-1", "GitHub connector", agui.ToolResultStateComplete) {
		t.Fatalf("expected tool_search_output tools to map to a tool result, got %#v", run.Events)
	}
	assertAGUISequenceValid(t, run)
}

func TestBackfillRawToolEndMapsResultBeforeEnd(t *testing.T) {
	run := aistream.NewRun("turn-1", "thread-1", "openai/gpt-5", "codex", "Codex", time.Unix(0, 0))
	writer := aistream.NewWriter(run, func() time.Time { return time.Unix(0, 0) })
	writer.Start()
	writer.StepStart("turn-1")
	if !mapBackfillItem(writer, run.MessageID, appserver.TurnItem{
		ID:   "image-1",
		Type: "image_generation_call",
		Raw: map[string]any{
			"type":           "image_generation_call",
			"id":             "image-1",
			"status":         "completed",
			"revised_prompt": "a clean bridge diagram",
			"result":         "mxc://example.com/image",
		},
	}, nil) {
		t.Fatal("expected raw image generation item to be backfilled")
	}
	writer.StepFinish("turn-1")
	writer.Finish(agui.FinishReasonStop)

	if !toolEventsInOrder(run.Events, "image-1", agui.EventToolCallStart, agui.EventToolCallResult, agui.EventToolCallEnd) {
		t.Fatalf("expected raw image result before tool end, got %#v", run.Events)
	}
	if !hasToolResultStateContaining(run.Events, "image-1", "mxc://example.com/image", agui.ToolResultStateComplete) {
		t.Fatalf("expected raw image result to map to tool result, got %#v", run.Events)
	}
	if got := countToolResults(run.Events, "image-1"); got != 1 {
		t.Fatalf("expected raw image close to preserve the real output only, got %d events=%#v", got, run.Events)
	}
	if !hasToolArgsContaining(run.Events, "image-1", "a clean bridge diagram") {
		t.Fatalf("expected raw image prompt to map to tool args, got %#v", run.Events)
	}
	if hasToolArgsContaining(run.Events, "image-1", "mxc://example.com/image") || hasToolArgsContaining(run.Events, "image-1", `"status"`) {
		t.Fatalf("raw image tool args should not include result/status fields, got %#v", run.Events)
	}
	assertAGUISequenceValid(t, run)
}

func TestBackfillRawToolEndPreservesStreamingStatus(t *testing.T) {
	run := aistream.NewRun("turn-1", "thread-1", "openai/gpt-5", "codex", "Codex", time.Unix(0, 0))
	writer := aistream.NewWriter(run, func() time.Time { return time.Unix(0, 0) })
	writer.Start()
	writer.StepStart("turn-1")
	if !mapBackfillItem(writer, run.MessageID, appserver.TurnItem{
		ID:   "search-1",
		Type: "web_search_call",
		Raw: map[string]any{
			"type":   "web_search_call",
			"id":     "search-1",
			"status": "in_progress",
			"action": map[string]any{"query": "codex bridge"},
			"result": "searching",
		},
	}, nil) {
		t.Fatal("expected raw in-progress web search to be backfilled")
	}
	writer.StepFinish("turn-1")
	writer.Finish(agui.FinishReasonStop)

	if !hasToolResultState(run.Events, "search-1", "searching", agui.ToolResultStateStreaming) {
		t.Fatalf("expected raw in-progress tool status to stay streaming, got %#v", run.Events)
	}
	if hasToolCallEnd(run.Events, "search-1") {
		t.Fatalf("streaming raw tool should not be closed in backfill, got %#v", run.Events)
	}
	assertAGUISequenceValid(t, run)
}

func TestBackfillRawToolEndSynthesizesIDWhenCodexOmitsCallID(t *testing.T) {
	run := aistream.NewRun("turn-1", "thread-1", "openai/gpt-5", "codex", "Codex", time.Unix(0, 0))
	writer := aistream.NewWriter(run, func() time.Time { return time.Unix(0, 0) })
	writer.Start()
	writer.StepStart("turn-1")
	item := map[string]any{
		"type":   "web_search_call",
		"status": "completed",
		"action": map[string]any{"query": "codex bridge streaming"},
	}
	callID := rawToolCallID(item)
	if !mapBackfillItem(writer, run.MessageID, appserver.TurnItem{Type: "web_search_call", Raw: item}, nil) {
		t.Fatal("expected raw web search item without call_id to be backfilled")
	}
	writer.StepFinish("turn-1")
	writer.Finish(agui.FinishReasonStop)

	if !strings.HasPrefix(callID, "raw_web_search_call_") {
		t.Fatalf("unexpected synthetic raw tool ID: %q", callID)
	}
	if !toolEventsInOrder(run.Events, callID, agui.EventToolCallStart, agui.EventToolCallEnd) {
		t.Fatalf("expected raw web search backfill to synthesize a stable tool ID, got %#v", run.Events)
	}
	if !hasToolArgsContaining(run.Events, callID, "codex bridge streaming") {
		t.Fatalf("expected raw web search action to be preserved as tool input, got %#v", run.Events)
	}
	if hasToolResult(run.Events, callID, "null") {
		t.Fatalf("missing raw web search output should not be backfilled as a null tool result: %#v", run.Events)
	}
	assertAGUISequenceValid(t, run)
}

func TestBackfillRawResponseItemCompactionTriggerNotice(t *testing.T) {
	run := aistream.NewRun("turn-1", "thread-1", "openai/gpt-5", "codex", "Codex", time.Unix(0, 0))
	writer := aistream.NewWriter(run, func() time.Time { return time.Unix(0, 0) })
	writer.Start()
	writer.StepStart("turn-1")
	if !mapBackfillItem(writer, run.MessageID, appserver.TurnItem{
		Type: "compaction_trigger",
		Raw:  map[string]any{"type": "compaction_trigger"},
	}, nil) {
		t.Fatal("expected compaction trigger to be backfilled")
	}
	writer.StepFinish("turn-1")
	writer.Finish(agui.FinishReasonStop)

	if countTextDelta(run.Events, codexCompactionNotice) != 1 {
		t.Fatalf("expected compaction trigger notice in backfill, got %#v", run.Events)
	}
	assertAGUISequenceValid(t, run)
}

func TestBackfillToolItemPreservesFailureState(t *testing.T) {
	run := aistream.NewRun("turn-1", "thread-1", "codex", "codex", "Codex", time.Unix(0, 0))
	writer := aistream.NewWriter(run, func() time.Time { return time.Unix(0, 0) })
	item := appserver.TurnItem{
		ID:               "cmd-1",
		Type:             "commandExecution",
		Command:          "go test ./...",
		AggregatedOutput: "failed",
		Raw: map[string]any{
			"id":               "cmd-1",
			"type":             "commandExecution",
			"command":          "go test ./...",
			"status":           "completed",
			"exitCode":         float64(1),
			"aggregatedOutput": "failed",
		},
	}
	if !mapBackfillItem(writer, run.MessageID, item, nil) {
		t.Fatal("expected failed command item to be backfilled")
	}
	if !hasToolResultState(run.Events, "cmd-1", "failed", agui.ToolResultStateError) {
		t.Fatalf("expected failed backfill command to map to error tool result, got %#v", run.Events)
	}
}

func TestBackfillStreamingToolItemDoesNotCloseAsComplete(t *testing.T) {
	run := aistream.NewRun("turn-1", "thread-1", "codex", "codex", "Codex", time.Unix(0, 0))
	writer := aistream.NewWriter(run, func() time.Time { return time.Unix(0, 0) })
	item := appserver.TurnItem{
		ID:               "cmd-1",
		Type:             "commandExecution",
		Command:          "go test ./...",
		AggregatedOutput: "still running",
		Raw: map[string]any{
			"id":               "cmd-1",
			"type":             "commandExecution",
			"command":          "go test ./...",
			"status":           "inProgress",
			"aggregatedOutput": "still running",
		},
	}
	if !mapBackfillItem(writer, run.MessageID, item, nil) {
		t.Fatal("expected in-progress command item to be backfilled")
	}
	if !hasToolResultState(run.Events, "cmd-1", "still running", agui.ToolResultStateStreaming) {
		t.Fatalf("expected in-progress backfill command to map to streaming tool result, got %#v", run.Events)
	}
	if hasToolCallEnd(run.Events, "cmd-1") {
		t.Fatalf("in-progress backfill command should not close the tool, got %#v", run.Events)
	}
}

func TestBackfillCommandExecutionUsesRawAggregatedOutput(t *testing.T) {
	run := aistream.NewRun("turn-1", "thread-1", "codex", "codex", "Codex", time.Unix(0, 0))
	writer := aistream.NewWriter(run, func() time.Time { return time.Unix(0, 0) })
	item := appserver.TurnItem{
		ID:   "cmd-1",
		Type: "commandExecution",
		Raw: map[string]any{
			"id":               "cmd-1",
			"type":             "commandExecution",
			"command":          "go test ./...",
			"status":           "completed",
			"aggregatedOutput": "ok",
		},
	}

	if !mapBackfillItem(writer, run.MessageID, item, nil) {
		t.Fatal("expected command item to be backfilled")
	}
	if !hasToolResultState(run.Events, "cmd-1", "ok", agui.ToolResultStateComplete) {
		t.Fatalf("expected raw aggregated output to map to tool result, got %#v", run.Events)
	}
	if got := countToolResult(run.Events, "cmd-1", "ok"); got != 1 {
		t.Fatalf("expected one backfilled tool result, got %d events=%#v", got, run.Events)
	}
	if got := countToolResults(run.Events, "cmd-1"); got != 1 {
		t.Fatalf("expected backfilled command close to preserve the real output only, got %d events=%#v", got, run.Events)
	}
	if !hasToolArgsContaining(run.Events, "cmd-1", "go test ./...") {
		t.Fatalf("expected command input to map to tool args, got %#v", run.Events)
	}
}

func TestBackfillCommandExecutionDoesNotEmitTerminalMetadataAsOutput(t *testing.T) {
	run := aistream.NewRun("turn-1", "thread-1", "codex", "codex", "Codex", time.Unix(0, 0))
	writer := aistream.NewWriter(run, func() time.Time { return time.Unix(0, 0) })
	item := appserver.TurnItem{
		ID:   "cmd-1",
		Type: "commandExecution",
		Raw: map[string]any{
			"id":               "cmd-1",
			"type":             "commandExecution",
			"command":          "go test ./...",
			"status":           "completed",
			"aggregatedOutput": "ok",
			"exitCode":         float64(0),
			"durationMs":       float64(12),
		},
	}

	if !mapBackfillItem(writer, run.MessageID, item, nil) {
		t.Fatal("expected command item to be backfilled")
	}
	if countToolResult(run.Events, "cmd-1", "ok") != 1 {
		t.Fatalf("expected backfilled command output exactly once, got %#v", run.Events)
	}
	if got := countToolResults(run.Events, "cmd-1"); got != 1 {
		t.Fatalf("expected terminal command metadata not to emit as output, got %d events=%#v", got, run.Events)
	}
	assertAGUISequenceValid(t, run)
}

func TestBackfillToolInputPreservesGeneratedMetadata(t *testing.T) {
	run := aistream.NewRun("turn-1", "thread-1", "codex", "codex", "Codex", time.Unix(0, 0))
	writer := aistream.NewWriter(run, func() time.Time { return time.Unix(0, 0) })
	for _, item := range []appserver.TurnItem{
		{
			ID:   "cmd-1",
			Type: "commandExecution",
			Raw: map[string]any{
				"id":               "cmd-1",
				"type":             "commandExecution",
				"command":          "go test ./...",
				"cwd":              "/tmp/project",
				"processId":        "proc-1",
				"source":           "user",
				"status":           "completed",
				"commandActions":   []any{map[string]any{"type": "exec", "program": "go", "argv": []any{"go", "test", "./..."}}},
				"aggregatedOutput": "ok",
				"exitCode":         float64(0),
				"durationMs":       float64(12),
			},
		},
		{
			ID:   "mcp-1",
			Type: "mcpToolCall",
			Raw: map[string]any{
				"id":                "mcp-1",
				"type":              "mcpToolCall",
				"server":            "github",
				"tool":              "list_issues",
				"arguments":         map[string]any{"state": "open"},
				"mcpAppResourceUri": "app://github",
				"pluginId":          "github",
				"status":            "completed",
			},
		},
		{
			ID:   "collab-1",
			Type: "collabAgentToolCall",
			Raw: map[string]any{
				"id":                "collab-1",
				"type":              "collabAgentToolCall",
				"tool":              "spawn",
				"senderThreadId":    "thread-1",
				"receiverThreadIds": []any{"thread-2"},
				"prompt":            "audit mapping",
				"model":             "gpt-5.5",
				"reasoningEffort":   "high",
				"agentsStates":      map[string]any{"thread-2": map[string]any{"status": "running"}},
				"status":            "completed",
			},
		},
	} {
		if !mapBackfillItem(writer, run.MessageID, item, nil) {
			t.Fatalf("expected %s item to be backfilled", item.Type)
		}
	}

	for _, want := range []string{`"processId":"proc-1"`, `"source":"user"`, `"commandActions"`, `"program":"go"`} {
		if !hasToolArgsContaining(run.Events, "cmd-1", want) {
			t.Fatalf("expected backfilled command input to preserve %s, got %#v", want, run.Events)
		}
	}
	for _, unwanted := range []string{`"status"`, `"aggregatedOutput"`, `"exitCode"`, `"durationMs"`} {
		if hasToolArgsContaining(run.Events, "cmd-1", unwanted) {
			t.Fatalf("backfilled command args should not include output/status field %s: %#v", unwanted, run.Events)
		}
	}
	for _, want := range []string{`"mcpAppResourceUri":"app://github"`, `"pluginId":"github"`} {
		if !hasToolArgsContaining(run.Events, "mcp-1", want) {
			t.Fatalf("expected backfilled MCP input to preserve %s, got %#v", want, run.Events)
		}
	}
	for _, want := range []string{`"senderThreadId":"thread-1"`, `"receiverThreadIds":["thread-2"]`, `"model":"gpt-5.5"`, `"reasoningEffort":"high"`, `"agentsStates"`} {
		if !hasToolArgsContaining(run.Events, "collab-1", want) {
			t.Fatalf("expected backfilled collab input to preserve %s, got %#v", want, run.Events)
		}
	}
	for _, want := range []string{`"subagents"`, `"threadId":"thread-2"`, `"portalId":"subagent:thread-2"`, `"readOnly":true`, `"status":"running"`} {
		if !hasToolStartMetadataContaining(run.Events, "collab-1", want) {
			t.Fatalf("expected backfilled collab metadata to preserve subagent %s, got %#v", want, run.Events)
		}
	}
	for _, want := range []string{`"type":"commandExecution"`, `"status":"completed"`, `"processId":"proc-1"`} {
		if !hasToolStartMetadataContaining(run.Events, "cmd-1", want) {
			t.Fatalf("expected backfilled command start metadata to preserve %s, got %#v", want, run.Events)
		}
	}
	for _, unwanted := range []string{`"aggregatedOutput"`, `"exitCode"`, `"durationMs"`} {
		if hasToolStartMetadataContaining(run.Events, "cmd-1", unwanted) {
			t.Fatalf("backfilled command start metadata should not include output field %s: %#v", unwanted, run.Events)
		}
	}
}

func TestBackfillMaterializesReadOnlySubagentPortal(t *testing.T) {
	ctx := context.Background()
	oldPortalEventBuffer := bridgev2.PortalEventBuffer
	bridgev2.PortalEventBuffer = 0
	t.Cleanup(func() { bridgev2.PortalEventBuffer = oldPortalEventBuffer })

	connector, br := testBridgeWithDB(t, &fakeMatrixConnector{})
	user, err := br.GetUserByMXID(ctx, "@alice:example.com")
	if err != nil {
		t.Fatal(err)
	}
	login, err := connector.ensureLoginID(ctx, user, "sh-codex", "alice@example.com")
	if err != nil {
		t.Fatal(err)
	}
	client := login.Client.(*Client)
	parentKey := projectPortalKey("/tmp/project", login.ID)
	parentPortal := &bridgev2.Portal{Portal: &database.Portal{
		PortalKey: parentKey,
		MXID:      "!parent:example.com",
		RoomType:  database.RoomTypeDM,
		Metadata:  &PortalMetadata{Kind: portalKindProject, ThreadID: "thread-1", Cwd: "/tmp/project"},
	}}
	connector.rememberThreadRoom("thread-1", client, parentKey, "/tmp/project", nil)

	_, err = client.projectBackfillMessages(ctx, parentPortal, appserver.Thread{
		ID:  "thread-1",
		Cwd: "/tmp/project",
		Turns: []appserver.Turn{{
			ID: "turn-1",
			Items: []appserver.TurnItem{{
				ID:   "collab-1",
				Type: "collabAgentToolCall",
				Raw: map[string]any{
					"id":                "collab-1",
					"type":              "collabAgentToolCall",
					"receiverThreadIds": []any{"thread-2"},
					"agentsStates":      map[string]any{"thread-2": map[string]any{"status": "running"}},
					"status":            "completed",
				},
			}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}

	key := subagentPortalKey("thread-2", login.ID)
	requireEventually(t, time.Second, func() bool {
		portal, err := br.GetExistingPortalByKey(ctx, key)
		if err != nil || portal == nil {
			return false
		}
		meta := portalMetadata(portal.Metadata)
		return portal.MXID != "" && meta.Kind == portalKindSubagent && meta.ThreadID == "thread-2" && meta.ParentThreadID == "thread-1" && meta.Cwd == "/tmp/project" && meta.ReadOnly
	})
}

func TestBackfillFileChangeIncludesPatchDiff(t *testing.T) {
	run := aistream.NewRun("turn-1", "thread-1", "codex", "codex", "Codex", time.Unix(0, 0))
	writer := aistream.NewWriter(run, func() time.Time { return time.Unix(0, 0) })
	item := appserver.TurnItem{
		ID:   "patch-1",
		Type: "fileChange",
		Raw: map[string]any{
			"id":     "patch-1",
			"type":   "fileChange",
			"status": "completed",
			"changes": []any{map[string]any{
				"path": "pkg/bridge/stream.go",
				"kind": "modify",
				"diff": "@@\n-old\n+new",
			}},
		},
	}

	if !mapBackfillItem(writer, run.MessageID, item, nil) {
		t.Fatal("expected file change item to be backfilled")
	}
	if !hasToolResultStateContaining(run.Events, "patch-1", "pkg/bridge/stream.go", agui.ToolResultStateComplete) {
		t.Fatalf("expected backfill patch result to include path, got %#v", run.Events)
	}
	if !hasToolResultStateContaining(run.Events, "patch-1", "+new", agui.ToolResultStateComplete) {
		t.Fatalf("expected backfill patch result to include diff, got %#v", run.Events)
	}
}

func TestBackfillPlanItemMapsToActivitySnapshot(t *testing.T) {
	run := aistream.NewRun("turn-1", "thread-1", "codex", "codex", "Codex", time.Unix(0, 0))
	writer := aistream.NewWriter(run, func() time.Time { return time.Unix(0, 0) })
	item := appserver.TurnItem{ID: "plan-1", Type: "plan", Text: "Run tests"}

	if !mapBackfillItem(writer, run.MessageID, item, nil) {
		t.Fatal("expected plan item to be backfilled")
	}
	if !hasActivitySnapshot(run.Events, "codex_plan", "explanation", "Run tests") {
		t.Fatalf("expected backfill plan activity snapshot, got %#v", run.Events)
	}
}

func TestBackfillHookPromptMapsTextState(t *testing.T) {
	run := aistream.NewRun("turn-1", "thread-1", "codex", "codex", "Codex", time.Unix(0, 0))
	writer := aistream.NewWriter(run, func() time.Time { return time.Unix(0, 0) })
	item := appserver.TurnItem{
		ID:        "hook-prompt-1",
		Type:      "hookPrompt",
		Fragments: []appserver.PromptFragment{{Text: "Preserve approval context."}},
	}

	if !mapBackfillItem(writer, run.MessageID, item, nil) {
		t.Fatal("expected hook prompt item to be backfilled")
	}
	if !hasCodexRunStateDelta(run.Events, "item/hookPrompt", "text", "Preserve approval context.") ||
		!hasCodexRunStateDelta(run.Events, "item/hookPrompt", "itemId", "hook-prompt-1") {
		t.Fatalf("expected hook prompt text state, got %#v", run.Events)
	}
}

func TestBackfillHookPromptMapsRawTextState(t *testing.T) {
	for _, item := range []appserver.TurnItem{
		{
			ID:   "hook-prompt-raw-fragments",
			Type: "hookPrompt",
			Raw: map[string]any{
				"id":        "hook-prompt-raw-fragments",
				"type":      "hookPrompt",
				"fragments": []any{map[string]any{"text": "Raw fragment approval context."}},
			},
		},
		{
			ID:   "hook-prompt-raw-text",
			Type: "hookPrompt",
			Raw: map[string]any{
				"id":   "hook-prompt-raw-text",
				"type": "hookPrompt",
				"text": "Raw text approval context.",
			},
		},
	} {
		run := aistream.NewRun("turn-1", "thread-1", "codex", "codex", "Codex", time.Unix(0, 0))
		writer := aistream.NewWriter(run, func() time.Time { return time.Unix(0, 0) })
		if !mapBackfillItem(writer, run.MessageID, item, nil) {
			t.Fatalf("expected raw hook prompt item %s to be backfilled", item.ID)
		}
		want := "Raw fragment approval context."
		if item.ID == "hook-prompt-raw-text" {
			want = "Raw text approval context."
		}
		if !hasCodexRunStateDelta(run.Events, "item/hookPrompt", "text", want) ||
			!hasCodexRunStateDelta(run.Events, "item/hookPrompt", "itemId", item.ID) {
			t.Fatalf("expected raw hook prompt text state for %s, got %#v", item.ID, run.Events)
		}
	}
}

func generatedTypeScriptThreadItemTypes(t *testing.T) []string {
	t.Helper()
	raw, err := os.ReadFile(codexTypeScriptV2ThreadItemPath)
	if err != nil {
		t.Fatalf("read generated Codex TypeScript thread item schema: %v", err)
	}
	return sortedUniqueMatches(regexp.MustCompile(`"type":\s*"([^"]+)"`), raw)
}

func aiMessageHasText(ai aistream.BeeperAI, text string) bool {
	if ai.Message == nil {
		return false
	}
	for _, part := range ai.Message.Parts {
		if content, _ := part["content"].(string); strings.Contains(content, text) {
			return true
		}
	}
	return false
}

func assertNoTurnIDThinkingPart(t *testing.T, ai aistream.BeeperAI, turnID string) {
	t.Helper()
	if ai.Message == nil {
		t.Fatalf("AI message missing")
	}
	for _, part := range ai.Message.Parts {
		if part["type"] != "thinking" {
			continue
		}
		if part["content"] == turnID || part["stepId"] == turnID {
			t.Fatalf("turn ID leaked as visible thinking part: %#v", ai.Message.Parts)
		}
	}
}
