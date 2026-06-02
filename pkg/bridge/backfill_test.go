package bridge

import (
	"context"
	"encoding/json"
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"

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
	part := messages[1].ConvertedMessage.Parts[0]
	meta, ok := part.DBMetadata.(*MessageMetadata)
	if !ok || meta.Role != "assistant" || meta.ThreadID != "thread-1" || meta.TurnID != "turn-1" {
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
	if !strings.Contains(part.Content.Body, "done") {
		t.Fatalf("assistant fallback body missing text: %q", part.Content.Body)
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

func TestGeneratedCodexThreadItemsAreBackfilled(t *testing.T) {
	for _, itemType := range generatedCodexThreadItemTypes(t) {
		if !isBackfilledThreadItemType(itemType) {
			t.Fatalf("generated Codex thread item %q is not covered by backfill", itemType)
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
	name := backfillToolName(appserver.TurnItem{ID: "mcp-1", Type: "mcpToolCall"}, data)
	if name != "github list_issues" {
		t.Fatalf("unexpected tool name: %q", name)
	}
	result := backfillToolResultText(data)
	if !strings.Contains(result, `"count":2`) {
		t.Fatalf("unexpected tool result: %q", result)
	}

	data = map[string]any{"type": "dynamicToolCall", "tool": "custom_tool", "contentItems": []any{map[string]any{"text": "done"}}}
	if name := backfillToolName(appserver.TurnItem{Type: "dynamicToolCall"}, data); name != "custom_tool" {
		t.Fatalf("unexpected dynamic tool name: %q", name)
	}
	if result := backfillToolResultText(data); !strings.Contains(result, "done") {
		t.Fatalf("unexpected dynamic tool result: %q", result)
	}
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
	if !mapBackfillItem(writer, run.MessageID, item) {
		t.Fatal("expected failed command item to be backfilled")
	}
	if !hasToolResultState(run.Events, "cmd-1", "failed", agui.ToolResultStateError) {
		t.Fatalf("expected failed backfill command to map to error tool result, got %#v", run.Events)
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

	if !mapBackfillItem(writer, run.MessageID, item) {
		t.Fatal("expected command item to be backfilled")
	}
	if !hasToolResultState(run.Events, "cmd-1", "ok", agui.ToolResultStateComplete) {
		t.Fatalf("expected raw aggregated output to map to tool result, got %#v", run.Events)
	}
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

	if !mapBackfillItem(writer, run.MessageID, item) {
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

	if !mapBackfillItem(writer, run.MessageID, item) {
		t.Fatal("expected plan item to be backfilled")
	}
	if !hasActivitySnapshot(run.Events, "codex_plan", "explanation", "Run tests") {
		t.Fatalf("expected backfill plan activity snapshot, got %#v", run.Events)
	}
}

func generatedCodexThreadItemTypes(t *testing.T) []string {
	t.Helper()
	raw, err := os.ReadFile(codexGeneratedSchemaPath)
	if err != nil {
		t.Fatalf("read generated Codex schema: %v", err)
	}
	re := regexp.MustCompile(`(?s)type:\s*Annotated\[\s*Literal\["([^"]+)"\].{0,300}?ThreadItemType`)
	seen := map[string]bool{}
	for _, match := range re.FindAllSubmatch(raw, -1) {
		seen[string(match[1])] = true
	}
	types := make([]string, 0, len(seen))
	for itemType := range seen {
		types = append(types, itemType)
	}
	sort.Strings(types)
	return types
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
