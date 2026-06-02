package bridge

import (
	"context"
	"strings"
	"testing"

	agui "github.com/beeper/ai-bridge/pkg/ag-ui"
	aistream "github.com/beeper/ai-bridge/pkg/ai-stream"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

func TestNewActiveRunAdvertisesCodexAGUICapabilities(t *testing.T) {
	run := newActiveRun(nil, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1")
	caps, ok := run.run.Data["capabilities"].(agui.AgentCapabilities)
	if !ok {
		t.Fatalf("missing capabilities: %#v", run.run.Data)
	}
	if caps.Transport == nil || caps.Transport.Streaming == nil || !*caps.Transport.Streaming {
		t.Fatalf("streaming capability not advertised: %#v", caps.Transport)
	}
	if caps.HumanInTheLoop == nil || caps.HumanInTheLoop.Approvals == nil || !*caps.HumanInTheLoop.Approvals {
		t.Fatalf("approval capability not advertised: %#v", caps.HumanInTheLoop)
	}
	if caps.Execution == nil || caps.Execution.CodeExecution == nil || !*caps.Execution.CodeExecution {
		t.Fatalf("execution capability not advertised: %#v", caps.Execution)
	}
}

func TestActiveRunStartRequiresBridgeContext(t *testing.T) {
	client := &Client{UserLogin: testUserLogin("codex")}
	run := newActiveRun(client, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1")
	if err := run.start(context.Background()); err == nil {
		t.Fatal("expected missing bridge context error")
	}
}

func TestOutputDelta(t *testing.T) {
	itemID, delta := outputDelta([]byte(`{"itemId":"item-1","delta":"hello"}`))
	if itemID != "item-1" || delta != "hello" {
		t.Fatalf("unexpected item delta: itemID=%q delta=%q", itemID, delta)
	}

	itemID, delta = outputDelta([]byte(`{"processId":"proc-1","output":"line"}`))
	if itemID != "proc-1" || delta != "line" {
		t.Fatalf("unexpected process delta: itemID=%q delta=%q", itemID, delta)
	}

	itemID, delta = outputDelta([]byte(`{"processHandle":"proc-2","deltaBase64":"bGluZQo="}`))
	if itemID != "proc-2" || delta != "line\n" {
		t.Fatalf("unexpected base64 process delta: itemID=%q delta=%q", itemID, delta)
	}
}

func TestReasoningTextDeltaUsesContentIndex(t *testing.T) {
	run := newActiveRun(nil, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1")
	run.handle("item/reasoning/textDelta", []byte(`{"threadId":"thread-1","turnId":"turn-1","itemId":"reason-1","contentIndex":0,"delta":"first"}`))
	run.handle("item/reasoning/textDelta", []byte(`{"threadId":"thread-1","turnId":"turn-1","itemId":"reason-1","contentIndex":1,"delta":"second"}`))

	ids := map[string]bool{}
	for _, event := range run.run.Events {
		if event.Type() == agui.EventReasoningMsgStart {
			ids[event.String("messageId")] = true
		}
	}
	if len(ids) != 2 {
		t.Fatalf("expected two reasoning messages for two content indexes, got %#v", ids)
	}
}

func TestStreamMapsRichCodexNotifications(t *testing.T) {
	run := newActiveRun(nil, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1")
	run.handle("model/rerouted", []byte(`{"threadId":"thread-1","turnId":"turn-1","fromModel":"a","toModel":"b","reason":"policy"}`))
	run.handle("item/commandExecution/terminalInteraction", []byte(`{"threadId":"thread-1","turnId":"turn-1","itemId":"cmd-1","processId":"p","stdin":"y\n"}`))
	run.handle("item/autoApprovalReview/completed", []byte(`{"threadId":"thread-1","turnId":"turn-1","targetItemId":"cmd-1","review":{"status":"approved","rationale":"ok"}}`))
	run.handle("item/reasoning/summaryPartAdded", []byte(`{"threadId":"thread-1","turnId":"turn-1","itemId":"reason-1","summaryIndex":1}`))
	run.handle("hook/started", []byte(`{"threadId":"thread-1","turnId":"turn-1","run":{"id":"hook-1","eventName":"preCompact","status":"running"}}`))
	run.handle("hook/completed", []byte(`{"threadId":"thread-1","turnId":"turn-1","run":{"id":"hook-1","eventName":"preCompact","status":"succeeded"}}`))
	run.handle("thread/settings/updated", []byte(`{"threadId":"thread-1","turnId":"turn-1","threadSettings":{"cwd":"/tmp/project","model":"gpt-5","modelProvider":"openai","approvalPolicy":"on-request","collaborationMode":"default"}}`))
	run.handle("thread/realtime/transcript/delta", []byte(`{"threadId":"thread-1","role":"assistant","delta":"hello"}`))

	if !hasEventType(run.run.Events, agui.EventStateDelta) {
		t.Fatalf("expected state delta event, got %#v", run.run.Events)
	}
	if !hasEventType(run.run.Events, agui.EventToolCallStart) {
		t.Fatalf("expected tool start event, got %#v", run.run.Events)
	}
	if !hasEventType(run.run.Events, agui.EventToolCallEnd) {
		t.Fatalf("expected tool end event, got %#v", run.run.Events)
	}
	if !hasEventType(run.run.Events, agui.EventToolCallResult) {
		t.Fatalf("expected tool result event, got %#v", run.run.Events)
	}
	if !hasEventType(run.run.Events, agui.EventReasoningMsgStart) {
		t.Fatalf("expected reasoning message start event, got %#v", run.run.Events)
	}
	if !hasEventType(run.run.Events, agui.EventTextMessageContent) {
		t.Fatalf("expected text content event, got %#v", run.run.Events)
	}
}

func TestStreamPublisherPublishesCarriers(t *testing.T) {
	run := newActiveRun(nil, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1")
	run.anchorMXID = "$anchor:example.com"
	run.roomID = "!room:example.com"
	publisher := &recordingBeeperStreamPublisher{}
	run.publisher = publisher
	run.writer.Start()
	run.writer.Text("hello")

	run.started = true
	run.publishLocked()
	if len(publisher.updates) != 1 {
		t.Fatalf("expected one Beeper stream update, got %#v", publisher.updates)
	}
	ai, ok := publisher.updates[0][aistream.BeeperAIKey].(aistream.BeeperAI)
	if !ok || ai.Kind != aistream.AIKindStream || len(ai.Events) == 0 {
		t.Fatalf("unexpected stream payload: %#v", publisher.updates[0])
	}
	if publisher.roomID != "!room:example.com" || publisher.eventID != "$anchor:example.com" {
		t.Fatalf("published to wrong target: room=%s event=%s", publisher.roomID, publisher.eventID)
	}
}

func TestFinalMessageMetadataMarksStreamStatus(t *testing.T) {
	run := aistream.Run{Status: aistream.Status{State: "complete"}}
	msg := &database.Message{Metadata: &MessageMetadata{Role: "assistant", ThreadID: "thread-1", TurnID: "turn-1"}}
	applyFinalMessageMetadata(msg, run, "thread-1", "turn-1")
	meta, ok := msg.Metadata.(*MessageMetadata)
	if !ok || meta.StreamStatus != "complete" || meta.Role != "assistant" || meta.ThreadID != "thread-1" || meta.TurnID != "turn-1" {
		t.Fatalf("unexpected complete metadata: %#v", msg.Metadata)
	}

	run.Status.State = "error"
	msg = &database.Message{}
	applyFinalMessageMetadata(msg, run, "thread-2", "turn-2")
	meta, ok = msg.Metadata.(*MessageMetadata)
	if !ok || meta.StreamStatus != "error" || meta.ThreadID != "thread-2" || meta.TurnID != "turn-2" {
		t.Fatalf("unexpected error metadata: %#v", msg.Metadata)
	}
}

func TestProcessOutputUsesRegisteredCommandItem(t *testing.T) {
	run := newActiveRun(nil, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1")
	run.handle("item/started", []byte(`{"threadId":"thread-1","turnId":"turn-1","item":{"type":"commandExecution","id":"cmd-1","command":"go test ./...","processId":"proc-1"}}`))
	run.handle("command/exec/outputDelta", []byte(`{"processId":"proc-1","deltaBase64":"b2sK","stream":"stdout"}`))

	if !hasToolResult(run.run.Events, "cmd-1", "ok\n") {
		t.Fatalf("expected process output to map to command item, got %#v", run.run.Events)
	}
}

func TestProcessExitUsesRegisteredCommandItem(t *testing.T) {
	run := newActiveRun(nil, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1")
	run.handle("item/started", []byte(`{"threadId":"thread-1","turnId":"turn-1","item":{"type":"commandExecution","id":"cmd-1","command":"go test ./...","processId":"proc-1"}}`))
	run.handle("process/exited", []byte(`{"processHandle":"proc-1","exitCode":1,"stdout":"out\n","stderr":"err\n","stdoutCapReached":true}`))

	if !hasToolResultState(run.run.Events, "cmd-1", "out\nerr\n[stdout truncated]", agui.ToolResultStateError) {
		t.Fatalf("expected process exit to map to failed command item, got %#v", run.run.Events)
	}
}

func TestConnectorRoutesProcessNotificationWithoutThreadID(t *testing.T) {
	connector := &Connector{}
	client := &Client{Main: connector}
	run := newActiveRun(client, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1")
	run.rememberProcessTool("proc-1", "cmd-1")

	connector.handleNotification("command/exec/outputDelta", []byte(`{"processId":"proc-1","deltaBase64":"b2sK","stream":"stdout"}`))

	if !hasToolResult(run.run.Events, "cmd-1", "ok\n") {
		t.Fatalf("expected connector-routed process output to map to command item, got %#v", run.run.Events)
	}
}

func TestConnectorRoutesProcessExitWithoutThreadID(t *testing.T) {
	connector := &Connector{}
	client := &Client{Main: connector}
	run := newActiveRun(client, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1")
	run.rememberProcessTool("proc-1", "cmd-1")

	connector.handleNotification("process/exited", []byte(`{"processHandle":"proc-1","exitCode":2}`))

	if !hasToolResultState(run.run.Events, "cmd-1", "Process exited with code 2.", agui.ToolResultStateError) {
		t.Fatalf("expected connector-routed process exit to map to command item, got %#v", run.run.Events)
	}
}

func TestProcessExitResult(t *testing.T) {
	itemID, result, state := processExitResult(map[string]any{"processHandle": "proc-1", "exitCode": float64(2)})
	if itemID != "proc-1" || result != "Process exited with code 2." || state != agui.ToolResultStateError {
		t.Fatalf("unexpected failed exit result: itemID=%q result=%q state=%q", itemID, result, state)
	}

	_, result, state = processExitResult(map[string]any{"processHandle": "proc-1", "exitCode": float64(0), "stdout": "ok\n"})
	if result != "ok" || state != agui.ToolResultStateComplete {
		t.Fatalf("unexpected successful exit result: result=%q state=%q", result, state)
	}
}

func TestContextCompactionItemProducesVisibleText(t *testing.T) {
	run := newActiveRun(nil, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1")
	run.handle("item/completed", []byte(`{"threadId":"thread-1","turnId":"turn-1","item":{"type":"contextCompaction","id":"compact-1"}}`))

	if !hasTextDelta(run.run.Events, codexCompactionNotice) {
		t.Fatalf("expected compaction notice text event, got %#v", run.run.Events)
	}
}

func TestReviewModeItemsProduceVisibleText(t *testing.T) {
	run := newActiveRun(nil, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1")
	run.handle("item/completed", []byte(`{"threadId":"thread-1","turnId":"turn-1","item":{"type":"enteredReviewMode","id":"review-1","review":"Inspect auth flow"}}`))
	run.handle("item/completed", []byte(`{"threadId":"thread-1","turnId":"turn-1","item":{"type":"exitedReviewMode","id":"review-2"}}`))

	if !hasTextDelta(run.run.Events, codexEnteredReviewNotice+"\n\nInspect auth flow") {
		t.Fatalf("expected entered review notice text event, got %#v", run.run.Events)
	}
	if !hasTextDelta(run.run.Events, codexExitedReviewNotice) {
		t.Fatalf("expected exited review notice text event, got %#v", run.run.Events)
	}
}

func TestCompletedAgentMessageRecoversTextWithoutDuplicatingDelta(t *testing.T) {
	run := newActiveRun(nil, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1")
	run.handle("item/completed", []byte(`{"threadId":"thread-1","turnId":"turn-1","item":{"type":"agentMessage","id":"agent-1","text":"done"}}`))
	if countTextDelta(run.run.Events, "done") != 1 {
		t.Fatalf("expected completed agent text fallback once, got %#v", run.run.Events)
	}

	run = newActiveRun(nil, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1")
	run.handle("item/agentMessage/delta", []byte(`{"threadId":"thread-1","turnId":"turn-1","itemId":"agent-1","delta":"done"}`))
	run.handle("item/completed", []byte(`{"threadId":"thread-1","turnId":"turn-1","item":{"type":"agentMessage","id":"agent-1","text":"done"}}`))
	if countTextDelta(run.run.Events, "done") != 1 {
		t.Fatalf("expected streamed agent text not to duplicate on completion, got %#v", run.run.Events)
	}
}

func TestCompletedReasoningRecoversTextWithoutDuplicatingDelta(t *testing.T) {
	run := newActiveRun(nil, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1")
	run.handle("item/completed", []byte(`{"threadId":"thread-1","turnId":"turn-1","item":{"type":"reasoning","id":"reason-1","summary":["checked"],"content":["read files"]}}`))
	if countReasoningDelta(run.run.Events, "checked") != 1 || countReasoningDelta(run.run.Events, "read files") != 1 {
		t.Fatalf("expected completed reasoning fallback, got %#v", run.run.Events)
	}

	run = newActiveRun(nil, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1")
	run.handle("item/reasoning/textDelta", []byte(`{"threadId":"thread-1","turnId":"turn-1","itemId":"reason-1","contentIndex":0,"delta":"read files"}`))
	run.handle("item/completed", []byte(`{"threadId":"thread-1","turnId":"turn-1","item":{"type":"reasoning","id":"reason-1","content":["read files"]}}`))
	if countReasoningDelta(run.run.Events, "read files") != 1 {
		t.Fatalf("expected streamed reasoning not to duplicate on completion, got %#v", run.run.Events)
	}
}

func TestRawResponseItemCompletedMapsAssistantReasoningAndTools(t *testing.T) {
	run := newActiveRun(nil, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1")
	run.handle("rawResponseItem/completed", []byte(`{"threadId":"thread-1","turnId":"turn-1","item":{"type":"message","role":"assistant","id":"msg-1","content":[{"type":"output_text","text":"raw answer"}]}}`))
	run.handle("rawResponseItem/completed", []byte(`{"threadId":"thread-1","turnId":"turn-1","item":{"type":"reasoning","summary":[{"type":"summary_text","text":"raw checked"}],"content":[{"type":"reasoning_text","text":"raw thought"}]}}`))
	run.handle("rawResponseItem/completed", []byte(`{"threadId":"thread-1","turnId":"turn-1","item":{"type":"function_call","call_id":"call-1","name":"search","arguments":"{\"query\":\"codex\"}"}}`))
	run.handle("rawResponseItem/completed", []byte(`{"threadId":"thread-1","turnId":"turn-1","item":{"type":"function_call_output","call_id":"call-1","output":"found"}}`))

	if countTextDelta(run.run.Events, "raw answer") != 1 {
		t.Fatalf("expected raw assistant message fallback, got %#v", run.run.Events)
	}
	if countReasoningDelta(run.run.Events, "raw checked") != 1 || countReasoningDelta(run.run.Events, "raw thought") != 1 {
		t.Fatalf("expected raw reasoning fallback, got %#v", run.run.Events)
	}
	if !hasEventType(run.run.Events, agui.EventToolCallStart) || !hasToolResult(run.run.Events, "call-1", "found") {
		t.Fatalf("expected raw tool call/result events, got %#v", run.run.Events)
	}
}

func TestRawResponseAssistantMessageDoesNotDuplicateStreamedDelta(t *testing.T) {
	run := newActiveRun(nil, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1")
	run.handle("item/agentMessage/delta", []byte(`{"threadId":"thread-1","turnId":"turn-1","itemId":"msg-1","delta":"raw answer"}`))
	run.handle("rawResponseItem/completed", []byte(`{"threadId":"thread-1","turnId":"turn-1","item":{"type":"message","role":"assistant","id":"msg-1","content":[{"type":"output_text","text":"raw answer"}]}}`))

	if countTextDelta(run.run.Events, "raw answer") != 1 {
		t.Fatalf("expected raw assistant message not to duplicate streamed delta, got %#v", run.run.Events)
	}
}

func TestCompletedToolItemPreservesFailureState(t *testing.T) {
	run := newActiveRun(nil, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1")
	run.handle("item/started", []byte(`{"threadId":"thread-1","turnId":"turn-1","item":{"type":"mcpToolCall","id":"mcp-1","server":"github","tool":"list_issues","status":"inProgress"}}`))
	run.handle("item/completed", []byte(`{"threadId":"thread-1","turnId":"turn-1","item":{"type":"mcpToolCall","id":"mcp-1","server":"github","tool":"list_issues","status":"failed","error":{"message":"token expired"}}}`))

	if !hasToolResultStateContaining(run.run.Events, "mcp-1", `"status":"failed"`, agui.ToolResultStateError) {
		t.Fatalf("expected failed MCP item to map to error tool result, got %#v", run.run.Events)
	}
}

func TestCompletedToolItemMapsRichResult(t *testing.T) {
	run := newActiveRun(nil, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1")
	run.handle("item/started", []byte(`{"threadId":"thread-1","turnId":"turn-1","item":{"type":"mcpToolCall","id":"mcp-1","server":"github","tool":"list_issues","status":"inProgress"}}`))
	run.handle("item/completed", []byte(`{"threadId":"thread-1","turnId":"turn-1","item":{"type":"mcpToolCall","id":"mcp-1","server":"github","tool":"list_issues","status":"completed","result":{"count":2}}}`))

	if !hasToolResultStateContaining(run.run.Events, "mcp-1", `"count":2`, agui.ToolResultStateComplete) {
		t.Fatalf("expected completed tool result to map to AG-UI tool result, got %#v", run.run.Events)
	}
}

func TestCompletedCommandDoesNotReplayStreamedAggregateOutput(t *testing.T) {
	run := newActiveRun(nil, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1")
	run.handle("item/started", []byte(`{"threadId":"thread-1","turnId":"turn-1","item":{"type":"commandExecution","id":"cmd-1","command":"go test ./...","status":"inProgress"}}`))
	run.handle("item/commandExecution/outputDelta", []byte(`{"threadId":"thread-1","turnId":"turn-1","itemId":"cmd-1","delta":"ok\n"}`))
	run.handle("item/completed", []byte(`{"threadId":"thread-1","turnId":"turn-1","item":{"type":"commandExecution","id":"cmd-1","command":"go test ./...","status":"completed","aggregatedOutput":"ok\n"}}`))

	if countToolResult(run.run.Events, "cmd-1", "ok\n") != 1 {
		t.Fatalf("expected streamed aggregate output not to be replayed, got %#v", run.run.Events)
	}
}

func TestMCPToolProgressMapsToStreamingToolResult(t *testing.T) {
	run := newActiveRun(nil, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1")
	run.handle("item/started", []byte(`{"threadId":"thread-1","turnId":"turn-1","item":{"type":"mcpToolCall","id":"mcp-1","server":"github","tool":"list_issues","status":"inProgress"}}`))
	run.handle("item/mcpToolCall/progress", []byte(`{"threadId":"thread-1","turnId":"turn-1","itemId":"mcp-1","message":"Fetching issues"}`))

	if !hasToolResultState(run.run.Events, "mcp-1", "Fetching issues", agui.ToolResultStateStreaming) {
		t.Fatalf("expected MCP progress to map to streaming tool result, got %#v", run.run.Events)
	}
}

func TestFilePatchUpdateMapsToStreamingToolResult(t *testing.T) {
	run := newActiveRun(nil, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1")
	run.handle("item/started", []byte(`{"threadId":"thread-1","turnId":"turn-1","item":{"type":"fileChange","id":"patch-1","status":"inProgress"}}`))
	run.handle("item/fileChange/patchUpdated", []byte(`{
		"threadId":"thread-1",
		"turnId":"turn-1",
		"itemId":"patch-1",
		"changes":[{"path":"pkg/bridge/stream.go","kind":"update","diff":"@@\n-old\n+new"}]
	}`))

	if !hasToolResultStateContaining(run.run.Events, "patch-1", "pkg/bridge/stream.go", agui.ToolResultStateStreaming) {
		t.Fatalf("expected patch update to map to streaming tool result, got %#v", run.run.Events)
	}
	if !hasToolResultStateContaining(run.run.Events, "patch-1", "+new", agui.ToolResultStateStreaming) {
		t.Fatalf("expected patch diff in streaming tool result, got %#v", run.run.Events)
	}
}

func TestPlanAndDiffUpdatesMapToActivitySnapshots(t *testing.T) {
	run := newActiveRun(nil, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1")
	run.handle("item/plan/delta", []byte(`{"threadId":"thread-1","turnId":"turn-1","itemId":"plan-1","delta":"Check tests"}`))
	run.handle("turn/plan/updated", []byte(`{"threadId":"thread-1","turnId":"turn-1","explanation":"Plan","plan":[{"text":"Run tests","status":"pending"}]}`))
	run.handle("turn/diff/updated", []byte(`{"threadId":"thread-1","turnId":"turn-1","diff":"@@\n-old\n+new"}`))

	if !hasActivitySnapshot(run.run.Events, "codex_plan", "delta", "Check tests") {
		t.Fatalf("expected plan delta activity snapshot, got %#v", run.run.Events)
	}
	if !hasActivitySnapshot(run.run.Events, "codex_plan", "explanation", "Plan") {
		t.Fatalf("expected plan snapshot activity, got %#v", run.run.Events)
	}
	if !hasActivitySnapshot(run.run.Events, "codex_diff", "diff", "+new") {
		t.Fatalf("expected diff snapshot activity, got %#v", run.run.Events)
	}
}

func TestCompletedPlanItemMapsToActivitySnapshot(t *testing.T) {
	run := newActiveRun(nil, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1")
	run.handle("item/completed", []byte(`{"threadId":"thread-1","turnId":"turn-1","item":{"type":"plan","id":"plan-1","text":"Run tests"}}`))

	if !hasActivitySnapshot(run.run.Events, "codex_plan", "explanation", "Run tests") {
		t.Fatalf("expected completed plan activity snapshot, got %#v", run.run.Events)
	}
}

func TestCodexItemToolState(t *testing.T) {
	tests := []struct {
		name string
		data map[string]any
		want string
	}{
		{name: "failed status", data: map[string]any{"status": "failed"}, want: agui.ToolResultStateError},
		{name: "declined status", data: map[string]any{"status": "declined"}, want: agui.ToolResultStateError},
		{name: "nonzero exit", data: map[string]any{"status": "completed", "exitCode": float64(2)}, want: agui.ToolResultStateError},
		{name: "success false", data: map[string]any{"success": false}, want: agui.ToolResultStateError},
		{name: "in progress", data: map[string]any{"status": "inProgress"}, want: agui.ToolResultStateStreaming},
		{name: "complete", data: map[string]any{"status": "completed"}, want: agui.ToolResultStateComplete},
	}
	for _, tt := range tests {
		if got := codexItemToolState(tt.data); got != tt.want {
			t.Fatalf("%s: got %q want %q", tt.name, got, tt.want)
		}
	}
}

func hasEventType(events []agui.Event, eventType string) bool {
	for _, event := range events {
		if event.Type() == eventType {
			return true
		}
	}
	return false
}

func hasTextDelta(events []agui.Event, delta string) bool {
	for _, event := range events {
		if event.Type() == agui.EventTextMessageContent && event.String("delta") == delta {
			return true
		}
	}
	return false
}

func countTextDelta(events []agui.Event, delta string) int {
	count := 0
	for _, event := range events {
		if event.Type() == agui.EventTextMessageContent && event.String("delta") == delta {
			count++
		}
	}
	return count
}

func countReasoningDelta(events []agui.Event, delta string) int {
	count := 0
	for _, event := range events {
		if event.Type() == agui.EventReasoningMsgCont && event.String("delta") == delta {
			count++
		}
	}
	return count
}

func hasToolResult(events []agui.Event, toolCallID, delta string) bool {
	for _, event := range events {
		if event.Type() == agui.EventToolCallResult && event.String("toolCallId") == toolCallID && event.String("content") == delta {
			return true
		}
	}
	return false
}

func countToolResult(events []agui.Event, toolCallID, delta string) int {
	count := 0
	for _, event := range events {
		if event.Type() == agui.EventToolCallResult && event.String("toolCallId") == toolCallID && event.String("content") == delta {
			count++
		}
	}
	return count
}

func hasToolResultState(events []agui.Event, toolCallID, delta, state string) bool {
	for _, event := range events {
		if event.Type() == agui.EventToolCallResult && event.String("toolCallId") == toolCallID && event.String("content") == delta && event.String("state") == state {
			return true
		}
	}
	return false
}

func hasToolResultStateContaining(events []agui.Event, toolCallID, content, state string) bool {
	for _, event := range events {
		if event.Type() == agui.EventToolCallResult && event.String("toolCallId") == toolCallID && strings.Contains(event.String("content"), content) && event.String("state") == state {
			return true
		}
	}
	return false
}

func hasActivitySnapshot(events []agui.Event, activityType, key, text string) bool {
	for _, event := range events {
		if event.Type() != agui.EventActivitySnapshot || event.String("activityType") != activityType {
			continue
		}
		content, _ := event.Get("content").(map[string]any)
		if strings.Contains(anyString(content[key]), text) {
			return true
		}
	}
	return false
}

func anyString(value any) string {
	if text, _ := value.(string); text != "" {
		return text
	}
	return ""
}

type recordingBeeperStreamPublisher struct {
	roomID  id.RoomID
	eventID id.EventID
	updates []map[string]any
}

func (p *recordingBeeperStreamPublisher) NewDescriptor(ctx context.Context, roomID id.RoomID, streamType string) (*event.BeeperStreamInfo, error) {
	return &event.BeeperStreamInfo{UserID: "@bot:example.com", Type: streamType}, nil
}

func (p *recordingBeeperStreamPublisher) Register(ctx context.Context, roomID id.RoomID, eventID id.EventID, descriptor *event.BeeperStreamInfo) error {
	p.roomID = roomID
	p.eventID = eventID
	return nil
}

func (p *recordingBeeperStreamPublisher) Publish(ctx context.Context, roomID id.RoomID, eventID id.EventID, delta map[string]any) error {
	p.roomID = roomID
	p.eventID = eventID
	p.updates = append(p.updates, delta)
	return nil
}

func (p *recordingBeeperStreamPublisher) Unregister(roomID id.RoomID, eventID id.EventID) {}

var _ bridgev2.BeeperStreamPublisher = (*recordingBeeperStreamPublisher)(nil)
