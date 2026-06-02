package bridge

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	agui "github.com/beeper/ai-bridge/pkg/ag-ui"
	aistream "github.com/beeper/ai-bridge/pkg/ai-stream"
	"github.com/beeper/ai-bridge/pkg/aidb"
	"github.com/beeper/ai-bridge/pkg/aiid"
	"github.com/rs/zerolog"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/beeperstream"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/bridgeconfig"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
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

func TestNewActiveRunStartsAGUIRunBeforeBufferedNotifications(t *testing.T) {
	run := newActiveRun(nil, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1")
	run.handle("item/agentMessage/delta", []byte(`{"threadId":"thread-1","turnId":"turn-1","itemId":"agent-1","delta":"hello"}`))

	if len(run.run.Events) < 2 {
		t.Fatalf("expected run start and text events, got %#v", run.run.Events)
	}
	if run.run.Events[0].Type() != agui.EventRunStarted {
		t.Fatalf("first buffered AG-UI event should start the run, got %#v", run.run.Events[0])
	}
	if !hasEventType(run.run.Events[1:], agui.EventTextMessageContent) {
		t.Fatalf("expected text notification after run start, got %#v", run.run.Events)
	}
	assertAGUISequenceValid(t, run.run)
}

func TestActiveRunStartRequiresBridgeContext(t *testing.T) {
	client := &Client{UserLogin: testUserLogin("codex")}
	run := newActiveRun(client, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1")
	if err := run.start(context.Background()); err == nil {
		t.Fatal("expected missing bridge context error")
	}
}

func TestAnchorEventIDDoesNotPredictOutgoingID(t *testing.T) {
	run := newActiveRun(nil, projectPortalKey("/tmp/project", "sh-codex"), "thread-1", "turn-1")
	portal := &bridgev2.Portal{
		Portal: &database.Portal{
			PortalKey: run.portalKey,
			MXID:      "!room:example.com",
		},
		Bridge: &bridgev2.Bridge{
			Config: &bridgeconfig.BridgeConfig{OutgoingMessageReID: true},
			Matrix: &deterministicMatrix{eventID: "$deterministic:example.com"},
		},
	}

	got := run.anchorEventID(context.Background(), portal)
	if got != "" || run.anchorMXID != "" {
		t.Fatalf("anchor event ID should come from the sent event or DB, got %q cached %q", got, run.anchorMXID)
	}
}

func TestResolveAnchorEventIDPrefersBridgeMessageStore(t *testing.T) {
	ctx := context.Background()
	connector, br := testBridgeWithDB(t, &deterministicMatrix{eventID: "$deterministic:example.com"})
	key := projectPortalKey("/tmp/project", "sh-codex")
	if err := br.DB.Portal.Insert(ctx, &database.Portal{
		PortalKey: key,
		MXID:      "!room:example.com",
		Name:      "project",
		RoomType:  database.RoomTypeDM,
		Metadata:  &PortalMetadata{ThreadID: "thread-1", Cwd: "/tmp/project"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := br.DB.Ghost.Insert(ctx, &database.Ghost{ID: codexUserID, Name: "Codex"}); err != nil {
		t.Fatal(err)
	}
	client := &Client{Main: connector, UserLogin: testUserLogin("sh-codex")}
	run := newActiveRun(client, key, "thread-1", "turn-1")
	if err := br.DB.Message.Insert(ctx, &database.Message{
		ID:         run.messageID,
		PartID:     partID("text"),
		MXID:       "$saved:example.com",
		Room:       key,
		SenderID:   codexUserID,
		SenderMXID: "@sh-codex_codex:example.com",
		Timestamp:  time.Unix(10, 0),
		Metadata:   &MessageMetadata{Role: "assistant", ThreadID: "thread-1", TurnID: "turn-1"},
	}); err != nil {
		t.Fatal(err)
	}

	run.resolveAnchorEventIDLocked(ctx)
	if run.anchorMXID != "$saved:example.com" {
		t.Fatalf("expected saved anchor event ID, got %q", run.anchorMXID)
	}
}

func TestAnchorMessageCarriesBeeperStreamAndStreamingMetadata(t *testing.T) {
	run := newActiveRun(nil, projectPortalKey("/tmp/project", "sh-codex"), "thread-1", "turn-1")
	descriptor := &event.BeeperStreamInfo{UserID: "@sh-codexbot:example.com", Type: aiid.StreamType}
	msg := run.anchorMessage(descriptor, time.Unix(10, 0))
	if msg.ID != run.messageID || msg.EventMeta.Sender.Sender != codexUserID {
		t.Fatalf("unexpected anchor routing: %#v", msg)
	}
	if len(msg.Data.Parts) != 1 {
		t.Fatalf("expected one anchor part, got %#v", msg.Data.Parts)
	}
	part := msg.Data.Parts[0]
	if part.ID != partID("text") || part.Content == nil || part.Content.BeeperStream != descriptor {
		t.Fatalf("anchor part is not a Beeper stream anchor: %#v", part)
	}
	assertCodexProfile(t, part.Content)
	ai, ok := part.Extra[aistream.BeeperAIKey].(aistream.BeeperAI)
	if !ok || ai.Kind != aistream.AIKindAnchor || ai.Message == nil {
		t.Fatalf("anchor missing Beeper AI metadata: %#v", part.Extra)
	}
	meta, ok := part.DBMetadata.(*MessageMetadata)
	if !ok || meta.Role != "assistant" || meta.ThreadID != "thread-1" || meta.TurnID != "turn-1" || meta.StreamStatus != "streaming" {
		t.Fatalf("anchor DB metadata should mark a streaming assistant run: %#v", part.DBMetadata)
	}
}

func TestMatrixFinalContentUploadsOversizedParts(t *testing.T) {
	run := aistream.NewRun("run-1", "thread-1", aistream.DefaultModel, "codex", "Codex", time.Unix(10, 0))
	run.MessageID = "msg-run-1"
	writer := aistream.NewWriter(run, func() time.Time { return time.Unix(10, 0) })
	writer.Start()
	writer.ToolStart("tool-1", "read", 0, nil)
	writer.ToolEnd("tool-1", "read", nil, map[string]any{"content": strings.Repeat("x", aistream.FinalMessageBudgetBytes)})
	writer.Text("done")
	writer.Finish(agui.FinishReasonStop)
	intent := &recordingMediaIntent{}
	portal := &bridgev2.Portal{Portal: &database.Portal{MXID: "!room:example.com"}}

	content, extra, err := matrixFinalContentWithAttachment(context.Background(), portal, intent, *run)
	if err != nil {
		t.Fatalf("final content upload failed: %v", err)
	}
	if content == nil || !strings.Contains(content.Body, "done") {
		t.Fatalf("final content lost visible text: %#v", content)
	}
	assertCodexProfile(t, content)
	if intent.roomID != portal.MXID {
		t.Fatalf("upload used wrong room: %q", intent.roomID)
	}
	if intent.mimeType != aistream.FinalPartsMediaType {
		t.Fatalf("upload used wrong MIME type: %q", intent.mimeType)
	}
	if !strings.Contains(intent.fileName, run.RunID) {
		t.Fatalf("upload filename should include run ID, got %q", intent.fileName)
	}
	ai, ok := extra[aistream.BeeperAIKey].(aistream.BeeperAI)
	if !ok {
		t.Fatalf("missing com.beeper.ai final metadata: %#v", extra[aistream.BeeperAIKey])
	}
	if ai.Final == nil || ai.Final.Delivery != "attachment" || ai.Final.PartsComplete {
		t.Fatalf("final metadata should advertise attachment delivery: %#v", ai.Final)
	}
	if ai.Final.PartsRef == nil || ai.Final.PartsRef.URL != string(intent.url) {
		t.Fatalf("missing uploaded parts ref: %#v", ai.Final)
	}
	if ai.Message == nil || len(ai.Message.Parts) != 0 {
		t.Fatalf("inline final parts should be empty after attachment upload: %#v", ai.Message)
	}
	var payload aistream.FinalPartsPayload
	if err := json.Unmarshal(intent.data, &payload); err != nil {
		t.Fatalf("uploaded payload is not final parts JSON: %v", err)
	}
	if payload.Schema != aistream.FinalPartsPayloadSchema || payload.ThreadID != run.ThreadID || payload.RunID != run.RunID || payload.MessageID != run.MessageID {
		t.Fatalf("bad uploaded final parts payload: %#v", payload)
	}
	if len(payload.Message.Parts) == 0 || ai.Final.PartsRef.PartsCount != len(payload.Message.Parts) {
		t.Fatalf("uploaded parts count mismatch: ref=%#v payload=%#v", ai.Final.PartsRef, payload.Message.Parts)
	}
}

func TestCodexFinalStreamEditClearsBeeperStream(t *testing.T) {
	run := aistream.NewRun("run-1", "thread-1", aistream.DefaultModel, "codex", "Codex", time.Unix(10, 0))
	run.MessageID = "msg-run-1"
	writer := aistream.NewWriter(run, func() time.Time { return time.Unix(10, 0) })
	writer.Start()
	writer.Text("done")
	writer.Finish(agui.FinishReasonStop)

	edit := codexFinalStreamEdit(
		networkid.PortalKey{ID: "portal-1"},
		networkid.MessageID(run.MessageID),
		*run,
		run.ThreadID,
		run.RunID,
		time.Unix(11, 0),
	)
	converted, err := edit.ConvertEditFunc(
		context.Background(),
		&bridgev2.Portal{Portal: &database.Portal{MXID: "!room:example.com"}},
		&recordingMediaIntent{},
		[]*database.Message{{}},
		edit.Data,
	)
	if err != nil {
		t.Fatal(err)
	}
	part := converted.ModifiedParts[0]
	if stream, ok := part.Extra["com.beeper.stream"]; !ok || stream != nil {
		t.Fatalf("final edit must clear stream in m.new_content: %#v", part.Extra)
	}
	if stream, ok := part.TopLevelExtra["com.beeper.stream"]; !ok || stream != nil {
		t.Fatalf("final edit must clear stream at top level: %#v", part.TopLevelExtra)
	}
}

func TestCodexFinalStreamEditUploadsOversizedPartsAndClearsStream(t *testing.T) {
	run := aistream.NewRun("run-1", "thread-1", aistream.DefaultModel, "codex", "Codex", time.Unix(10, 0))
	run.MessageID = "msg-run-1"
	writer := aistream.NewWriter(run, func() time.Time { return time.Unix(10, 0) })
	writer.Start()
	writer.ToolStart("tool-1", "read", 0, nil)
	writer.ToolEnd("tool-1", "read", nil, map[string]any{"content": strings.Repeat("x", aistream.FinalMessageBudgetBytes)})
	writer.Text("done")
	writer.Finish(agui.FinishReasonStop)
	intent := &recordingMediaIntent{}
	existing := &database.Message{Metadata: &MessageMetadata{Role: "assistant", ThreadID: "thread-1", TurnID: "turn-1", StreamStatus: "streaming"}}
	edit := codexFinalStreamEdit(
		networkid.PortalKey{ID: "portal-1"},
		networkid.MessageID(run.MessageID),
		*run,
		run.ThreadID,
		run.RunID,
		time.Unix(11, 0),
	)

	converted, err := edit.ConvertEditFunc(
		context.Background(),
		&bridgev2.Portal{Portal: &database.Portal{MXID: "!room:example.com"}},
		intent,
		[]*database.Message{existing},
		edit.Data,
	)
	if err != nil {
		t.Fatal(err)
	}
	part := converted.ModifiedParts[0]
	assertCodexProfile(t, part.Content)
	if stream, ok := part.Extra["com.beeper.stream"]; !ok || stream != nil {
		t.Fatalf("final edit must clear stream in m.new_content: %#v", part.Extra)
	}
	if stream, ok := part.TopLevelExtra["com.beeper.stream"]; !ok || stream != nil {
		t.Fatalf("final edit must clear stream at top level: %#v", part.TopLevelExtra)
	}
	if intent.roomID != "!room:example.com" || intent.mimeType != aistream.FinalPartsMediaType || len(intent.data) == 0 {
		t.Fatalf("final edit did not upload oversized parts correctly: room=%q mime=%q bytes=%d", intent.roomID, intent.mimeType, len(intent.data))
	}
	ai, ok := part.Extra[aistream.BeeperAIKey].(aistream.BeeperAI)
	if !ok || ai.Final == nil || ai.Final.Delivery != "attachment" || ai.Final.PartsComplete || ai.Final.PartsRef == nil || ai.Final.PartsRef.URL != string(intent.url) {
		t.Fatalf("final edit did not advertise uploaded final parts: %#v", part.Extra[aistream.BeeperAIKey])
	}
	if ai.Message == nil || len(ai.Message.Parts) != 0 {
		t.Fatalf("final edit should remove oversized inline parts: %#v", ai.Message)
	}
	meta, ok := existing.Metadata.(*MessageMetadata)
	if !ok || meta.StreamStatus != "complete" || meta.ThreadID != run.ThreadID || meta.TurnID != run.RunID {
		t.Fatalf("final edit did not update DB metadata: %#v", existing.Metadata)
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

	ids := distinctReasoningMessageIDs(run.run.Events)
	if len(ids) != 2 {
		t.Fatalf("expected two reasoning messages for two content indexes, got %#v", ids)
	}
}

func TestReasoningTextDeltaDoesNotHashCollidingItemIDsTogether(t *testing.T) {
	run := newActiveRun(nil, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1")
	run.handle("item/reasoning/textDelta", []byte(`{"threadId":"thread-1","turnId":"turn-1","itemId":"a","contentIndex":0,"delta":"first"}`))
	run.handle("item/reasoning/textDelta", []byte(`{"threadId":"thread-1","turnId":"turn-1","itemId":"A","contentIndex":0,"delta":"second"}`))

	ids := distinctReasoningMessageIDs(run.run.Events)
	if len(ids) != 2 {
		t.Fatalf("distinct reasoning item IDs should create distinct thinking parts, got %#v events=%#v", ids, run.run.Events)
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

func TestSemanticDeltasMapToChatWithoutRawCodexCustomEvents(t *testing.T) {
	run := newActiveRun(nil, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1")
	run.handle("item/agentMessage/delta", []byte(`{"threadId":"thread-1","turnId":"turn-1","itemId":"msg-1","delta":"hello"}`))
	run.handle("item/reasoning/textDelta", []byte(`{"threadId":"thread-1","turnId":"turn-1","itemId":"reason-1","contentIndex":0,"delta":"thinking"}`))
	run.handle("thread/realtime/transcript/delta", []byte(`{"threadId":"thread-1","turnId":"turn-1","role":"assistant","delta":"live"}`))

	if !hasTextDelta(run.run.Events, "hello") {
		t.Fatalf("agent message delta did not map to AG-UI text: %#v", run.run.Events)
	}
	if !hasTextDelta(run.run.Events, "live") {
		t.Fatalf("realtime transcript delta did not map to AG-UI text: %#v", run.run.Events)
	}
	if countReasoningDelta(run.run.Events, "thinking") != 1 {
		t.Fatalf("reasoning delta did not map to AG-UI reasoning: %#v", run.run.Events)
	}
	if hasCustomPayloadText(run.run.Events, "item/agentMessage/delta", "delta", "hello") {
		t.Fatalf("agent message raw delta should not be bridged as custom event: %#v", run.run.Events)
	}
	if hasCustomPayloadText(run.run.Events, "item/reasoning/textDelta", "delta", "thinking") {
		t.Fatalf("reasoning raw delta should not be bridged as custom event: %#v", run.run.Events)
	}
	if hasCustomPayloadText(run.run.Events, "thread/realtime/transcript/delta", "delta", "live") {
		t.Fatalf("realtime raw delta should not be bridged as custom event: %#v", run.run.Events)
	}
}

func TestTurnStartedDoesNotCreateVisibleThinkingPart(t *testing.T) {
	run := newActiveRun(nil, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1")
	run.handle("turn/started", []byte(`{"threadId":"thread-1","turn":{"id":"turn-1"}}`))
	run.writer.Text("done")
	run.writer.Finish(agui.FinishReasonStop)

	message := run.run.FinalBeeperAIMessage(0, true)
	for _, part := range message.Parts {
		if part["type"] == "thinking" {
			t.Fatalf("turn/started should not become a visible thinking part: %#v", message.Parts)
		}
	}
}

func TestActiveRunNotificationsDoNotBridgeRawCodexCustomEvents(t *testing.T) {
	for _, method := range generatedTypeScriptMethods(t, codexTypeScriptNotificationPath) {
		if !isActiveRunNotification(method) || method == "turn/completed" {
			continue
		}
		t.Run(method, func(t *testing.T) {
			run := newActiveRun(nil, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1")
			payload := map[string]any{
				"threadId": "thread-1",
				"turnId":   "turn-1",
				"marker":   "raw-" + method,
			}
			raw, err := json.Marshal(payload)
			if err != nil {
				t.Fatal(err)
			}

			run.handle(method, raw)
			if hasCustomPayloadText(run.run.Events, method, "marker", "raw-"+method) {
				t.Fatalf("%s should not bridge raw Codex payload as AG-UI custom event: %#v", method, run.run.Events)
			}
		})
	}
}

func TestTurnCompletedDoesNotBridgeRawCodexCustomEvent(t *testing.T) {
	ctx := context.Background()
	publisher := &recordingBeeperStreamPublisher{}
	connector, br := testBridgeWithDB(t, &streamStartMatrix{publisher: publisher, eventID: "$anchor:example.com"})
	user, err := br.GetUserByMXID(ctx, "@alice:example.com")
	if err != nil {
		t.Fatal(err)
	}
	login, err := connector.ensureLoginID(ctx, user, "sh-codex", "alice@example.com")
	if err != nil {
		t.Fatal(err)
	}
	key := projectPortalKey("/tmp/project", login.ID)
	if err = br.DB.Portal.Insert(ctx, &database.Portal{
		PortalKey: key,
		MXID:      "!room:example.com",
		Name:      "project",
		RoomType:  database.RoomTypeDM,
		Metadata:  &PortalMetadata{ThreadID: "thread-1", Cwd: "/tmp/project"},
	}); err != nil {
		t.Fatal(err)
	}
	client := &Client{Main: connector, UserLogin: login, loggedIn: true}
	login.Client = client
	run := newActiveRun(client, key, "thread-1", "turn-1")
	run.started = true
	run.roomID = "!room:example.com"
	run.anchorMXID = "$anchor:example.com"
	run.publisher = publisher
	connector.setActive("thread-1", run)
	if err = br.DB.Message.Insert(ctx, &database.Message{
		ID:         run.messageID,
		PartID:     partID("text"),
		MXID:       run.anchorMXID,
		Room:       key,
		SenderID:   codexUserID,
		SenderMXID: "@sh-codex_codex:example.com",
		Timestamp:  time.Unix(10, 0),
		Metadata:   &MessageMetadata{Role: "assistant", ThreadID: "thread-1", TurnID: "turn-1", StreamStatus: "streaming"},
	}); err != nil {
		t.Fatal(err)
	}

	run.handle("turn/completed", []byte(`{"threadId":"thread-1","turnId":"turn-1","marker":"raw-turn/completed","turn":{"id":"turn-1","status":"completed"}}`))

	if hasCustomPayloadText(run.run.Events, "turn/completed", "marker", "raw-turn/completed") {
		t.Fatalf("turn/completed should not bridge raw Codex payload as AG-UI custom event: %#v", run.run.Events)
	}
	message := run.run.FinalBeeperAIMessage(0, true)
	for _, part := range message.Parts {
		if part["type"] == "thinking" && (part["content"] == "turn-1" || part["stepId"] == "turn-1") {
			t.Fatalf("turn/completed should not become a visible thinking part: %#v", message.Parts)
		}
	}
	if connector.activeRun("thread-1") != nil {
		t.Fatal("turn/completed should clear the active run")
	}
}

func TestRealtimeTranscriptDoneRecoversAssistantText(t *testing.T) {
	run := newActiveRun(nil, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1")
	run.handle("thread/realtime/transcript/done", []byte(`{"threadId":"thread-1","role":"assistant","text":"final transcript"}`))
	if got := run.run.Text(); got != "final transcript" {
		t.Fatalf("transcript done did not recover assistant text: %q", got)
	}
	if hasRealtimeStateDeltaText(run.run.Events, "thread/realtime/transcript/done", "text", "final transcript") {
		t.Fatalf("transcript done raw payload should not be bridged as state: %#v", run.run.Events)
	}

	run = newActiveRun(nil, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1")
	run.handle("thread/realtime/transcript/delta", []byte(`{"threadId":"thread-1","role":"assistant","delta":"delta"}`))
	run.handle("thread/realtime/transcript/done", []byte(`{"threadId":"thread-1","role":"assistant","text":"delta"}`))
	if got := run.run.Text(); got != "delta" {
		t.Fatalf("transcript done duplicated streamed delta text: %q", got)
	}

	run = newActiveRun(nil, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1")
	run.handle("thread/compacted", []byte(`{"threadId":"thread-1"}`))
	run.handle("thread/realtime/transcript/done", []byte(`{"threadId":"thread-1","role":"assistant","text":"after notice"}`))
	if got := run.run.Text(); !strings.Contains(got, "after notice") {
		t.Fatalf("transcript done was hidden by unrelated visible text: %q", got)
	}

	run = newActiveRun(nil, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1")
	run.handle("thread/realtime/transcript/delta", []byte(`{"threadId":"thread-1","role":"assistant","delta":"hel"}`))
	run.handle("thread/realtime/transcript/done", []byte(`{"threadId":"thread-1","role":"assistant","text":"hello"}`))
	if got := run.run.Text(); got != "hello" {
		t.Fatalf("transcript done did not append missing suffix: %q", got)
	}
}

func TestStreamRunModelTracksThreadSettingsAndReroutes(t *testing.T) {
	connector := &Connector{threadRooms: map[string]threadRoom{}}
	client := &Client{Main: connector, UserLogin: testUserLogin("sh-codex")}
	key := projectPortalKey("/tmp/project", "sh-codex")
	connector.rememberThreadRoom("thread-1", client, key, "/tmp/project", "openai", "gpt-5")
	run := newActiveRun(client, key, "thread-1", "turn-1")
	if run.run.Model != "openai/gpt-5" {
		t.Fatalf("new run did not use cached model: %q", run.run.Model)
	}

	run.handle("model/rerouted", []byte(`{"threadId":"thread-1","turnId":"turn-1","fromModel":"gpt-5","toModel":"gpt-5-mini","reason":"policy"}`))
	if run.run.Model != "openai/gpt-5-mini" {
		t.Fatalf("reroute did not update run model: %q", run.run.Model)
	}
	if !hasCodexThreadStateDelta(run.run.Events, "model", "gpt-5-mini") || !hasCodexThreadStateDelta(run.run.Events, "lastNotification", "model/rerouted") {
		t.Fatalf("reroute did not emit normalized codexThread state delta: %#v", run.run.Events)
	}
	run.writer.Text("after reroute")
	if got := run.run.Events[len(run.run.Events)-1].Get("model"); got != "openai/gpt-5-mini" {
		t.Fatalf("future event kept stale reroute model: %#v", got)
	}

	run.handle("thread/settings/updated", []byte(`{"threadId":"thread-1","turnId":"turn-1","threadSettings":{"model":"gpt-5.1","modelProvider":"openai","effort":"high"}}`))
	if run.run.Model != "openai/gpt-5.1" {
		t.Fatalf("settings update did not update run model: %q", run.run.Model)
	}
	run.writer.Text("after settings")
	if got := run.run.Events[len(run.run.Events)-1].Get("model"); got != "openai/gpt-5.1" {
		t.Fatalf("future event kept stale settings model: %#v", got)
	}
}

func TestThreadStartedMapsToCodexThreadStateDelta(t *testing.T) {
	run := newActiveRun(nil, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1")
	run.handle("thread/started", []byte(`{"thread":{"id":"thread-1","sessionId":"session-1","cwd":"/tmp/project","model":"gpt-5","modelProvider":"openai"}}`))

	if !hasCodexThreadStateDelta(run.run.Events, "threadId", "thread-1") {
		t.Fatalf("expected thread/started state delta, got %#v", run.run.Events)
	}
	if run.run.Model != "openai/gpt-5" {
		t.Fatalf("thread/started did not update run model: %q", run.run.Model)
	}
}

func TestModelVerificationMapsToCodexThreadStateDelta(t *testing.T) {
	run := newActiveRun(nil, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1")
	run.handle("model/verification", []byte(`{"threadId":"thread-1","turnId":"turn-1","verifications":[{"type":"trustedAccessForCyber"}]}`))

	if !hasCodexThreadStateDelta(run.run.Events, "lastNotification", "model/verification") {
		t.Fatalf("model verification did not emit codexThread state delta: %#v", run.run.Events)
	}
}

func TestActiveRunNoticeNotificationsAreUserVisible(t *testing.T) {
	tests := []struct {
		name   string
		method string
		params string
		want   string
	}{
		{
			name:   "compaction",
			method: "thread/compacted",
			params: `{"threadId":"thread-1"}`,
			want:   codexCompactionNotice,
		},
		{
			name:   "model rerouted",
			method: "model/rerouted",
			params: `{"threadId":"thread-1","turnId":"turn-1","fromModel":"gpt-5","toModel":"gpt-5-mini","reason":"policy"}`,
			want:   "Codex switched models from gpt-5 to gpt-5-mini.",
		},
		{
			name:   "model verification",
			method: "model/verification",
			params: `{"threadId":"thread-1","turnId":"turn-1","verifications":["trustedAccessForCyber"]}`,
			want:   "Codex is running extra safety checks for possible cybersecurity risk.",
		},
		{
			name:   "legacy model verification object",
			method: "model/verification",
			params: `{"threadId":"thread-1","turnId":"turn-1","verifications":[{"type":"trustedAccessForCyber"}]}`,
			want:   "Trusted Access for Cyber: https://chatgpt.com/cyber",
		},
		{
			name:   "config warning",
			method: "configWarning",
			params: `{"threadId":"thread-1","summary":"bad config","details":"line 4"}`,
			want:   "Codex configWarning:\n\nbad config\n\nline 4",
		},
		{
			name:   "realtime closed",
			method: "thread/realtime/closed",
			params: `{"threadId":"thread-1","reason":"microphone disconnected"}`,
			want:   "Codex realtime closed:\n\nmicrophone disconnected",
		},
		{
			name:   "realtime closed without reason",
			method: "thread/realtime/closed",
			params: `{"threadId":"thread-1"}`,
			want:   "Codex realtime closed.",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			run := newActiveRun(nil, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1")

			run.handle(tt.method, []byte(tt.params))

			if !strings.Contains(run.run.Text(), tt.want) {
				t.Fatalf("active notice %q was not visible in stream text: %q", tt.method, run.run.Text())
			}
		})
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
	if ai.Protocol != "ag-ui" || ai.ThreadID != "thread-1" || ai.RunID != "turn-1" || ai.MessageID != string(run.messageID) {
		t.Fatalf("stream payload lost run identity: %#v", ai)
	}
	if publisher.roomID != "!room:example.com" || publisher.eventID != "$anchor:example.com" {
		t.Fatalf("published to wrong target: room=%s event=%s", publisher.roomID, publisher.eventID)
	}
}

func TestActiveRunStartUsesBridgeV2QueueAndRegistersBeeperStream(t *testing.T) {
	ctx := context.Background()
	publisher := &recordingBeeperStreamPublisher{}
	matrix := &streamStartMatrix{publisher: publisher, eventID: "$deterministic:example.com"}
	connector, br := testBridgeWithDB(t, matrix)
	br.Config.OutgoingMessageReID = true

	user, err := br.GetUserByMXID(ctx, "@alice:example.com")
	if err != nil {
		t.Fatal(err)
	}
	login, err := connector.ensureLoginID(ctx, user, "sh-codex", "alice@example.com")
	if err != nil {
		t.Fatal(err)
	}
	key := projectPortalKey("/tmp/project", login.ID)
	if err = br.DB.Portal.Insert(ctx, &database.Portal{
		PortalKey: key,
		MXID:      "!room:example.com",
		Name:      "project",
		RoomType:  database.RoomTypeDM,
		Metadata:  &PortalMetadata{ThreadID: "thread-1", Cwd: "/tmp/project"},
	}); err != nil {
		t.Fatal(err)
	}

	client := &Client{Main: connector, UserLogin: login, loggedIn: true}
	login.Client = client
	connector.rememberThreadRoom("thread-1", client, key, "/tmp/project", "openai", "gpt-5")
	run := newActiveRun(client, key, "thread-1", "turn-1")
	if err = run.start(ctx); err != nil {
		t.Fatal(err)
	}

	if !run.started {
		t.Fatal("active run was not marked started")
	}
	if publisher.roomID != "!room:example.com" || publisher.eventID == "" || publisher.eventID != run.anchorMXID {
		t.Fatalf("stream registered on wrong target: room=%s event=%s", publisher.roomID, publisher.eventID)
	}
	if publisher.descriptor == nil || publisher.descriptor.UserID != "@bot:example.com" || publisher.descriptor.Type != aiid.StreamType {
		t.Fatalf("stream registered with wrong descriptor: %#v", publisher.descriptor)
	}
	if len(publisher.updates) != 1 {
		t.Fatalf("expected initial stream carrier, got %#v", publisher.updates)
	}
	ai, ok := publisher.updates[0][aistream.BeeperAIKey].(aistream.BeeperAI)
	if !ok || ai.Kind != aistream.AIKindStream || len(ai.Events) == 0 {
		t.Fatalf("initial carrier missing Beeper AI stream payload: %#v", publisher.updates[0])
	}
	if ai.Events[0].Seq != 1 {
		t.Fatalf("initial stream sequence should start at 1, got %#v", ai.Events)
	}
	var lastTyping fakeTypingEvent
	requireEventually(t, time.Second, func() bool {
		typings := matrix.intent().typings
		if len(typings) == 0 {
			return false
		}
		lastTyping = typings[len(typings)-1]
		return lastTyping.RoomID == "!room:example.com" &&
			lastTyping.Type == bridgev2.TypingTypeText &&
			lastTyping.Timeout == 30*time.Second
	})
	if lastTyping.RoomID != "!room:example.com" || lastTyping.Type != bridgev2.TypingTypeText || lastTyping.Timeout != 30*time.Second {
		t.Fatalf("last typing state should keep Codex typing after stream anchor, got %#v", lastTyping)
	}
}

func TestActiveRunPreservesEncryptedBeeperStreamDescriptor(t *testing.T) {
	ctx := context.Background()
	keyBytes := []byte("0123456789abcdef0123456789abcdef")
	publisher := &recordingBeeperStreamPublisher{
		deviceID:      "PUBLISHER",
		encryptionKey: keyBytes,
	}
	matrix := &streamStartMatrix{publisher: publisher, eventID: "$deterministic:example.com"}
	connector, br := testBridgeWithDB(t, matrix)
	br.Config.OutgoingMessageReID = true

	user, err := br.GetUserByMXID(ctx, "@alice:example.com")
	if err != nil {
		t.Fatal(err)
	}
	login, err := connector.ensureLoginID(ctx, user, "sh-codex", "alice@example.com")
	if err != nil {
		t.Fatal(err)
	}
	key := projectPortalKey("/tmp/project", login.ID)
	if err = br.DB.Portal.Insert(ctx, &database.Portal{
		PortalKey: key,
		MXID:      "!room:example.com",
		Name:      "project",
		RoomType:  database.RoomTypeDM,
		Metadata:  &PortalMetadata{ThreadID: "thread-1", Cwd: "/tmp/project"},
	}); err != nil {
		t.Fatal(err)
	}

	client := &Client{Main: connector, UserLogin: login, loggedIn: true}
	login.Client = client
	connector.rememberThreadRoom("thread-1", client, key, "/tmp/project", "openai", "gpt-5")
	run := newActiveRun(client, key, "thread-1", "turn-1")
	if err = run.start(ctx); err != nil {
		t.Fatal(err)
	}

	if publisher.descriptor == nil || publisher.descriptor.DeviceID != "PUBLISHER" {
		t.Fatalf("stream descriptor device was not preserved: %#v", publisher.descriptor)
	}
	if publisher.descriptor.Encryption == nil || publisher.descriptor.Encryption.Algorithm != id.AlgorithmBeeperStreamV1 || string(publisher.descriptor.Encryption.Key) != string(keyBytes) {
		t.Fatalf("stream descriptor encryption was not preserved: %#v", publisher.descriptor.Encryption)
	}
}

func TestActiveRunReplaysPendingEncryptedBeeperStreamSubscribe(t *testing.T) {
	ctx := context.Background()
	roomID := id.RoomID("!room:example.com")
	anchorEventID := id.EventID("$message")
	publisherClient, publisherStreams, publisherRecorder := newCodexStreamHelper(t, "@sh-codexbot:example.com", "PUBDEVICE")
	subscriberClient, subscriberStreams, subscriberRecorder := newCodexStreamHelper(t, "@alice:example.com", "SUBDEVICE")
	if err := publisherClient.StateStore.SetEncryptionEvent(ctx, roomID, &event.EncryptionEventContent{Algorithm: id.AlgorithmMegolmV1}); err != nil {
		t.Fatal(err)
	}
	if err := subscriberClient.StateStore.SetEncryptionEvent(ctx, roomID, &event.EncryptionEventContent{Algorithm: id.AlgorithmMegolmV1}); err != nil {
		t.Fatal(err)
	}
	descriptor, err := publisherStreams.NewDescriptor(ctx, roomID, aiid.StreamType)
	if err != nil {
		t.Fatal(err)
	}
	if descriptor.Encryption == nil {
		t.Fatal("expected encrypted beeper stream descriptor")
	}
	if err = subscriberStreams.Subscribe(ctx, roomID, anchorEventID, descriptor); err != nil {
		t.Fatal(err)
	}
	subscribeReq := subscriberRecorder.next(t)
	if !strings.Contains(subscribeReq.path, "/sendToDevice/m.room.encrypted/") {
		t.Fatalf("expected encrypted subscribe request, got %s", subscribeReq.path)
	}
	publisherStreams.HandleSyncResponse(ctx, &mautrix.RespSync{
		ToDevice: mautrix.SyncEventsList{Events: []*event.Event{
			codexStreamToDeviceEvent(t, event.ToDeviceEncrypted, subscriberClient.UserID, descriptor.UserID, descriptor.DeviceID, subscriberRecorder.rawContent(t, subscribeReq, descriptor.UserID, descriptor.DeviceID)),
		}},
	})

	matrix := &streamStartMatrix{
		fakeMatrixConnector: fakeMatrixConnector{api: &fakeMatrixAPI{}},
		publisher:           &fixedBeeperStreamPublisher{helper: publisherStreams, descriptor: descriptor},
		eventID:             "$deterministic:example.com",
	}
	connector, br := testBridgeWithDB(t, matrix)
	br.Config.OutgoingMessageReID = true
	user, err := br.GetUserByMXID(ctx, "@alice:example.com")
	if err != nil {
		t.Fatal(err)
	}
	login, err := connector.ensureLoginID(ctx, user, "sh-codex", "alice@example.com")
	if err != nil {
		t.Fatal(err)
	}
	key := projectPortalKey("/tmp/project", login.ID)
	if err = br.DB.Portal.Insert(ctx, &database.Portal{
		PortalKey: key,
		MXID:      roomID,
		Name:      "project",
		RoomType:  database.RoomTypeDM,
		Metadata:  &PortalMetadata{ThreadID: "thread-1", Cwd: "/tmp/project"},
	}); err != nil {
		t.Fatal(err)
	}
	client := &Client{Main: connector, UserLogin: login, loggedIn: true}
	login.Client = client
	connector.rememberThreadRoom("thread-1", client, key, "/tmp/project", "openai", "gpt-5")
	run := newActiveRun(client, key, "thread-1", "turn-1")
	if err = run.start(ctx); err != nil {
		t.Fatal(err)
	}

	updateReq := publisherRecorder.next(t)
	if !strings.Contains(updateReq.path, "/sendToDevice/m.room.encrypted/") {
		t.Fatalf("expected encrypted stream update request, got %s", updateReq.path)
	}
	normalized := subscriberStreams.HandleSyncResponse(ctx, &mautrix.RespSync{
		ToDevice: mautrix.SyncEventsList{Events: []*event.Event{
			codexStreamToDeviceEvent(t, event.ToDeviceEncrypted, descriptor.UserID, subscriberClient.UserID, subscriberClient.DeviceID, publisherRecorder.rawContent(t, updateReq, subscriberClient.UserID, subscriberClient.DeviceID)),
		}},
	})
	if len(normalized) != 1 {
		t.Fatalf("expected one normalized stream update, got %#v", normalized)
	}
	ai, ok := normalized[0].Content.Raw[aistream.BeeperAIKey].(map[string]any)
	if !ok || ai["kind"] != string(aistream.AIKindStream) {
		t.Fatalf("normalized update did not contain Codex Beeper AI stream payload: %#v", normalized[0].Content.Raw)
	}
	if normalized[0].RoomID != roomID || normalized[0].Content.AsBeeperStreamUpdate().EventID != anchorEventID {
		t.Fatalf("normalized update targeted wrong stream: room=%s event=%s", normalized[0].RoomID, normalized[0].Content.AsBeeperStreamUpdate().EventID)
	}
}

func TestActiveRunRegistersStreamOnActualAnchorEventID(t *testing.T) {
	ctx := context.Background()
	oldPortalEventBuffer := bridgev2.PortalEventBuffer
	bridgev2.PortalEventBuffer = 0
	t.Cleanup(func() { bridgev2.PortalEventBuffer = oldPortalEventBuffer })

	publisher := &recordingBeeperStreamPublisher{}
	matrixAPI := &fakeMatrixAPI{}
	matrix := &streamStartMatrix{
		fakeMatrixConnector: fakeMatrixConnector{api: matrixAPI},
		publisher:           publisher,
		eventID:             "$deterministic:example.com",
	}
	connector, br := testBridgeWithDB(t, matrix)
	br.Config.OutgoingMessageReID = true

	user, err := br.GetUserByMXID(ctx, "@alice:example.com")
	if err != nil {
		t.Fatal(err)
	}
	login, err := connector.ensureLoginID(ctx, user, "sh-codex", "alice@example.com")
	if err != nil {
		t.Fatal(err)
	}
	key := projectPortalKey("/tmp/project", login.ID)
	if err = br.DB.Portal.Insert(ctx, &database.Portal{
		PortalKey: key,
		MXID:      "!room:example.com",
		Name:      "project",
		RoomType:  database.RoomTypeDM,
		Metadata:  &PortalMetadata{ThreadID: "thread-1", Cwd: "/tmp/project"},
	}); err != nil {
		t.Fatal(err)
	}

	checkedAnchorSend := false
	matrixAPI.onSendMessage = func(roomID id.RoomID, eventType event.Type, content *event.Content, extra *bridgev2.MatrixSendExtra) {
		if eventType != event.EventMessage {
			return
		}
		checkedAnchorSend = true
		if publisher.eventID != "" {
			t.Fatalf("Beeper stream publisher registered before the actual anchor event ID was known: %s", publisher.eventID)
		}
	}

	client := &Client{Main: connector, UserLogin: login, loggedIn: true}
	login.Client = client
	connector.rememberThreadRoom("thread-1", client, key, "/tmp/project", "openai", "gpt-5")
	run := newActiveRun(client, key, "thread-1", "turn-1")
	if err = run.start(ctx); err != nil {
		t.Fatal(err)
	}
	if !checkedAnchorSend {
		t.Fatal("anchor send hook was not called")
	}
	if run.anchorMXID != "$message" || publisher.eventID != "$message" {
		t.Fatalf("stream should register on actual anchor event, run=%s publisher=%s", run.anchorMXID, publisher.eventID)
	}
	if publisher.eventID == "$deterministic:example.com" {
		t.Fatal("stream registered on predicted deterministic event ID")
	}
}

func TestTurnStartedNotificationUsesPendingMatrixStartContext(t *testing.T) {
	ctx := context.Background()
	publisher := &recordingBeeperStreamPublisher{}
	matrix := &streamStartMatrix{publisher: publisher, eventID: "$deterministic:example.com"}
	connector, br := testBridgeWithDB(t, matrix)
	br.Config.OutgoingMessageReID = true

	user, err := br.GetUserByMXID(ctx, "@alice:example.com")
	if err != nil {
		t.Fatal(err)
	}
	login, err := connector.ensureLoginID(ctx, user, "sh-codex", "alice@example.com")
	if err != nil {
		t.Fatal(err)
	}
	key := projectPortalKey("/tmp/project", login.ID)
	if err = br.DB.Portal.Insert(ctx, &database.Portal{
		PortalKey: key,
		MXID:      "!room:example.com",
		Name:      "project",
		RoomType:  database.RoomTypeDM,
		Metadata:  &PortalMetadata{ThreadID: "thread-1", Cwd: "/tmp/project"},
	}); err != nil {
		t.Fatal(err)
	}

	client := &Client{Main: connector, UserLogin: login, loggedIn: true}
	login.Client = client
	connector.setPendingTurnStart("thread-1", client, key)
	defer connector.clearPendingTurnStart("thread-1")

	connector.handleNotification("turn/started", []byte(`{"threadId":"thread-1","turn":{"id":"turn-1"}}`))

	run := connector.activeRun("thread-1")
	if run == nil || run.turnID != "turn-1" || !run.started {
		t.Fatalf("turn/started did not synchronously start active run: %#v", run)
	}
	if run.portalKey != key {
		t.Fatalf("active run used wrong portal key: %s", run.portalKey)
	}
	if publisher.roomID != "!room:example.com" || publisher.eventID == "" || len(publisher.updates) == 0 {
		t.Fatalf("pending-start stream was not registered and published: room=%s event=%s updates=%#v", publisher.roomID, publisher.eventID, publisher.updates)
	}
}

func TestActiveRunFinalizeUnregistersPublisher(t *testing.T) {
	ctx := context.Background()
	oldDelay := activeStreamUnregisterDelay
	activeStreamUnregisterDelay = 20 * time.Millisecond
	t.Cleanup(func() { activeStreamUnregisterDelay = oldDelay })

	publisher := &recordingBeeperStreamPublisher{}
	matrix := &streamStartMatrix{publisher: publisher, eventID: "$deterministic:example.com"}
	connector, br := testBridgeWithDB(t, matrix)
	br.Config.OutgoingMessageReID = true

	user, err := br.GetUserByMXID(ctx, "@alice:example.com")
	if err != nil {
		t.Fatal(err)
	}
	login, err := connector.ensureLoginID(ctx, user, "sh-codex", "alice@example.com")
	if err != nil {
		t.Fatal(err)
	}
	key := projectPortalKey("/tmp/project", login.ID)
	if err = br.DB.Portal.Insert(ctx, &database.Portal{
		PortalKey: key,
		MXID:      "!room:example.com",
		Name:      "project",
		RoomType:  database.RoomTypeDM,
		Metadata:  &PortalMetadata{ThreadID: "thread-1", Cwd: "/tmp/project"},
	}); err != nil {
		t.Fatal(err)
	}

	client := &Client{Main: connector, UserLogin: login, loggedIn: true}
	login.Client = client
	connector.rememberThreadRoom("thread-1", client, key, "/tmp/project", "openai", "gpt-5")
	run := newActiveRun(client, key, "thread-1", "turn-1")
	if err = run.start(ctx); err != nil {
		t.Fatal(err)
	}

	run.finalize(time.Now())

	if count := publisher.unregisterCount(); count != 0 {
		t.Fatalf("finalization should keep stream publisher registered until final edit is sent, got %d unregisters", count)
	}
	requireEventually(t, time.Second, func() bool { return publisher.unregisterCount() == 1 })
	if count := publisher.unregisterCount(); count != 1 {
		t.Fatalf("finalization should unregister stream publisher once, got %d unregisters", count)
	}
}

func TestActiveRunPersistsAndDeletesActiveStreamRecord(t *testing.T) {
	ctx := context.Background()
	publisher := &recordingBeeperStreamPublisher{}
	matrix := &streamStartMatrix{publisher: publisher, eventID: "$deterministic:example.com"}
	connector, br := testBridgeWithDB(t, matrix)
	br.Config.OutgoingMessageReID = true
	if err := connector.Store.Upgrade(ctx); err != nil {
		t.Fatal(err)
	}

	user, err := br.GetUserByMXID(ctx, "@alice:example.com")
	if err != nil {
		t.Fatal(err)
	}
	login, err := connector.ensureLoginID(ctx, user, "sh-codex", "alice@example.com")
	if err != nil {
		t.Fatal(err)
	}
	key := projectPortalKey("/tmp/project", login.ID)
	if err = br.DB.Portal.Insert(ctx, &database.Portal{
		PortalKey: key,
		MXID:      "!room:example.com",
		Name:      "project",
		RoomType:  database.RoomTypeDM,
		Metadata:  &PortalMetadata{ThreadID: "thread-1", Cwd: "/tmp/project"},
	}); err != nil {
		t.Fatal(err)
	}

	client := &Client{Main: connector, UserLogin: login, loggedIn: true}
	login.Client = client
	connector.rememberThreadRoom("thread-1", client, key, "/tmp/project", "openai", "gpt-5")
	run := newActiveRun(client, key, "thread-1", "turn-1")
	if err = run.start(ctx); err != nil {
		t.Fatal(err)
	}

	records, err := connector.Store.ListActiveStreams(ctx, login.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatalf("expected one active stream record, got %#v", records)
	}
	record := records[0]
	if record.RunID != "turn-1" || record.PortalKey != key || record.RoomID != "!room:example.com" || record.EventID != run.anchorMXID || record.MessageID != run.messageID {
		t.Fatalf("unexpected active stream record: %#v", record)
	}
	if record.ProviderID != "openai" || record.ModelID != "gpt-5" || record.Run.Model != "openai/gpt-5" {
		t.Fatalf("unexpected active stream model metadata: %#v", record)
	}

	run.finalize(time.Now())
	records, err = connector.Store.ListActiveStreams(ctx, login.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatalf("expected active stream record to be deleted, got %#v", records)
	}
}

func TestConnectFinalizesPersistedActiveStreams(t *testing.T) {
	ctx := context.Background()
	connector, login, _ := seedPersistedCodexActiveStream(t, ctx, time.Unix(10, 0))

	client := &Client{Main: connector, UserLogin: login}
	login.Client = client
	client.Connect(ctx)

	records, err := connector.Store.ListActiveStreams(ctx, login.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatalf("expected persisted stream to be finalized and deleted, got %#v", records)
	}
}

func TestStaleActiveStreamJanitorFinalizesOrphanedStream(t *testing.T) {
	ctx := context.Background()
	oldTimeout := activeStreamIdleTimeout
	activeStreamIdleTimeout = time.Minute
	t.Cleanup(func() { activeStreamIdleTimeout = oldTimeout })

	connector, login, _ := seedPersistedCodexActiveStream(t, ctx, time.Now().Add(-2*time.Minute))

	client := &Client{Main: connector, UserLogin: login}
	login.Client = client
	client.failStaleActiveStreams(ctx)

	records, err := connector.Store.ListActiveStreams(ctx, login.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatalf("expected stale stream to be finalized and deleted, got %#v", records)
	}
}

func TestStaleActiveStreamJanitorKeepsLiveActiveRun(t *testing.T) {
	ctx := context.Background()
	oldTimeout := activeStreamIdleTimeout
	activeStreamIdleTimeout = time.Minute
	t.Cleanup(func() { activeStreamIdleTimeout = oldTimeout })

	connector, login, key := seedPersistedCodexActiveStream(t, ctx, time.Now().Add(-2*time.Minute))
	client := &Client{Main: connector, UserLogin: login}
	login.Client = client
	connector.setActive("thread-1", newActiveRun(client, key, "thread-1", "turn-1"))

	client.failStaleActiveStreams(ctx)

	records, err := connector.Store.ListActiveStreams(ctx, login.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatalf("expected live active stream record to remain, got %#v", records)
	}
}

func seedPersistedCodexActiveStream(t *testing.T, ctx context.Context, updatedAt time.Time) (*Connector, *bridgev2.UserLogin, networkid.PortalKey) {
	t.Helper()
	connector, br := testBridgeWithDB(t, &streamStartMatrix{publisher: &recordingBeeperStreamPublisher{}, eventID: "$deterministic:example.com"})
	if err := connector.Store.Upgrade(ctx); err != nil {
		t.Fatal(err)
	}
	user, err := br.GetUserByMXID(ctx, "@alice:example.com")
	if err != nil {
		t.Fatal(err)
	}
	login, err := connector.ensureLoginID(ctx, user, "sh-codex", "alice@example.com")
	if err != nil {
		t.Fatal(err)
	}
	key := projectPortalKey("/tmp/project", login.ID)
	if err = br.DB.Portal.Insert(ctx, &database.Portal{
		PortalKey: key,
		MXID:      "!room:example.com",
		Name:      "project",
		RoomType:  database.RoomTypeDM,
		Metadata:  &PortalMetadata{ThreadID: "thread-1", Cwd: "/tmp/project"},
	}); err != nil {
		t.Fatal(err)
	}
	run := aistream.NewRun("turn-1", "thread-1", "openai/gpt-5", "codex", "Codex", time.Unix(10, 0))
	run.MessageID = "msg-turn-1"
	writer := aistream.NewWriter(run, func() time.Time { return time.Unix(10, 0) })
	writer.Start()
	writer.Text("partial")
	if err := br.DB.Message.Insert(ctx, &database.Message{
		ID:         networkid.MessageID(run.MessageID),
		PartID:     partID("text"),
		MXID:       "$anchor:example.com",
		Room:       key,
		SenderID:   codexUserID,
		SenderMXID: "@sh-codex_codex:example.com",
		Timestamp:  time.Unix(10, 0),
		Metadata:   &MessageMetadata{Role: "assistant", ThreadID: "thread-1", TurnID: "turn-1", StreamStatus: "streaming"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := connector.Store.UpsertActiveStream(ctx, aidb.ActiveStreamRecord{
		RunID:      run.RunID,
		LoginID:    login.ID,
		PortalKey:  key,
		RoomID:     "!room:example.com",
		EventID:    "$anchor:example.com",
		MessageID:  networkid.MessageID(run.MessageID),
		ProviderID: "openai",
		ModelID:    "gpt-5",
		Run:        *run,
		CreatedAt:  time.Unix(10, 0),
		UpdatedAt:  updatedAt,
	}); err != nil {
		t.Fatal(err)
	}
	return connector, login, key
}

func TestStreamPublisherPublishesOnlyNewCarriers(t *testing.T) {
	run := newActiveRun(nil, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1")
	run.anchorMXID = "$anchor:example.com"
	run.roomID = "!room:example.com"
	publisher := &recordingBeeperStreamPublisher{}
	run.publisher = publisher
	run.started = true

	run.writer.Text("one")
	run.publishLocked()
	run.writer.Text("two")
	run.publishLocked()

	if len(publisher.updates) != 2 {
		t.Fatalf("expected two incremental updates, got %#v", publisher.updates)
	}
	first := publisher.updates[0][aistream.BeeperAIKey].(aistream.BeeperAI)
	second := publisher.updates[1][aistream.BeeperAIKey].(aistream.BeeperAI)
	if len(first.Events) == 0 || len(second.Events) == 0 {
		t.Fatalf("expected both updates to contain envelopes: first=%#v second=%#v", first, second)
	}
	if second.Events[0].Seq <= first.Events[len(first.Events)-1].Seq {
		t.Fatalf("second update replayed old sequence: first=%#v second=%#v", first.Events, second.Events)
	}
}

func TestSuppressStreamCarrierRequestLogsRaisesContextLogLevel(t *testing.T) {
	ctx := zerolog.New(nil).Level(zerolog.DebugLevel).WithContext(context.Background())
	suppressed := suppressStreamCarrierRequestLogs(ctx)
	if got := zerolog.Ctx(suppressed).GetLevel(); got != zerolog.FatalLevel {
		t.Fatalf("stream carrier context log level = %v, want fatal", got)
	}
}

func TestStreamPublisherSeparatesPublishedIndexFromStreamSequence(t *testing.T) {
	run := newActiveRun(nil, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1")
	run.anchorMXID = "$anchor:example.com"
	run.roomID = "!room:example.com"
	publisher := &recordingBeeperStreamPublisher{}
	run.publisher = publisher
	run.started = true
	run.writer.Text("already published")
	run.published = len(run.run.Events)
	run.nextSeq = 10
	run.writer.Text("new")

	run.publishLocked()

	if len(publisher.updates) != 1 {
		t.Fatalf("expected one incremental update, got %#v", publisher.updates)
	}
	ai := publisher.updates[0][aistream.BeeperAIKey].(aistream.BeeperAI)
	if len(ai.Events) != 1 || ai.Events[0].Seq != 10 {
		t.Fatalf("expected only unpublished event with seq 10, got %#v", ai.Events)
	}
	if run.published != len(run.run.Events) || run.nextSeq != 11 {
		t.Fatalf("cursor not advanced: published=%d nextSeq=%d events=%d", run.published, run.nextSeq, len(run.run.Events))
	}
}

func TestStreamPublisherRetriesFailedPublishWithoutAdvancingCursor(t *testing.T) {
	run := newActiveRun(nil, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1")
	run.anchorMXID = "$anchor:example.com"
	run.roomID = "!room:example.com"
	publisher := &recordingBeeperStreamPublisher{failOnPublish: 1, failErr: errors.New("send failed")}
	run.publisher = publisher
	run.started = true
	run.writer.Text("retry me")

	run.publishLocked()

	if len(publisher.updates) != 0 {
		t.Fatalf("failed publish should not record an update: %#v", publisher.updates)
	}
	if run.published != 0 || run.nextSeq != 1 {
		t.Fatalf("failed publish advanced cursor: published=%d nextSeq=%d", run.published, run.nextSeq)
	}

	run.publishLocked()

	if len(publisher.updates) != 1 {
		t.Fatalf("retry should publish one update, got %#v", publisher.updates)
	}
	ai := publisher.updates[0][aistream.BeeperAIKey].(aistream.BeeperAI)
	if len(ai.Events) == 0 || ai.Events[0].Seq != 1 {
		t.Fatalf("retry should start at original seq 1, got %#v", ai.Events)
	}
	if run.published != len(run.run.Events) || run.nextSeq != aistream.NextSeq([]aistream.Carrier{{Envelopes: ai.Events}}) {
		t.Fatalf("retry cursor not advanced correctly: published=%d nextSeq=%d events=%d", run.published, run.nextSeq, len(run.run.Events))
	}
}

func TestTurnCompletedBeforeStreamStartDoesNotFinalizeWithoutAnchor(t *testing.T) {
	run := newActiveRun(&Client{}, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1")

	run.handle("turn/completed", []byte(`{"threadId":"thread-1","turn":{"id":"turn-1","status":"completed"}}`))

	if run.started || run.anchorMXID != "" {
		t.Fatalf("run without bridge context should not start or finalize: started=%v anchor=%q", run.started, run.anchorMXID)
	}
	if hasEventType(run.run.Events, agui.EventRunFinished) {
		t.Fatalf("completion should wait for a stream anchor before finishing, got %#v", run.run.Events)
	}
}

func TestFinishTurnMapsCodexTerminalStatuses(t *testing.T) {
	tests := []struct {
		status    string
		message   string
		wantState string
	}{
		{status: "completed", wantState: "complete"},
		{status: "failed", wantState: "error"},
		{status: "timed_out", wantState: "error"},
		{status: "cancelled", wantState: "aborted"},
		{status: "interrupted", wantState: "aborted"},
		{status: "inProgress", wantState: "aborted"},
		{status: "completed", message: "runtime failed", wantState: "error"},
	}
	for _, tc := range tests {
		t.Run(tc.status+"/"+tc.message, func(t *testing.T) {
			run := newActiveRun(nil, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1")
			run.finishTurnLocked(tc.status, tc.message)
			if run.run.Status.State != tc.wantState {
				t.Fatalf("status %q message %q mapped to %q, want %q", tc.status, tc.message, run.run.Status.State, tc.wantState)
			}
			if tc.wantState == "aborted" && finalStreamStatus(*run.run) != "aborted" {
				t.Fatalf("aborted turn should persist aborted stream status, got %#v", run.run.Status)
			}
		})
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

func TestThreadSystemErrorStatusIsUserVisible(t *testing.T) {
	run := newActiveRun(nil, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1")

	run.handle("thread/status/changed", []byte(`{"threadId":"thread-1","model":"openai/gpt-5.5","status":{"type":"systemError"}}`))

	if run.run.Status.State != "error" {
		t.Fatalf("expected error status, got %#v", run.run.Status)
	}
	statusError, _ := run.run.Status.Error.(map[string]any)
	message, _ := statusError["message"].(string)
	if !strings.Contains(message, "openai/gpt-5.5") {
		t.Fatalf("expected visible model context in error message, got %q", message)
	}
	if !hasEventType(run.run.Events, agui.EventRunError) {
		t.Fatalf("expected run error event, got %#v", run.run.Events)
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

func TestProcessOutputBeforeCommandItemKeepsOneToolIdentity(t *testing.T) {
	run := newActiveRun(nil, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1")
	run.handle("command/exec/outputDelta", []byte(`{"processId":"proc-1","deltaBase64":"b2sK","stream":"stdout"}`))
	run.handle("item/started", []byte(`{"threadId":"thread-1","turnId":"turn-1","item":{"type":"commandExecution","id":"cmd-1","command":"go test ./...","processId":"proc-1"}}`))
	run.handle("item/completed", []byte(`{"threadId":"thread-1","turnId":"turn-1","item":{"type":"commandExecution","id":"cmd-1","command":"go test ./...","processId":"proc-1","status":"completed","aggregatedOutput":"ok\n"}}`))

	if !hasToolResult(run.run.Events, "proc-1", "ok\n") {
		t.Fatalf("expected early process output to stay on process tool, got %#v", run.run.Events)
	}
	if hasToolCallStart(run.run.Events, "cmd-1") || hasToolResult(run.run.Events, "cmd-1", "ok\n") {
		t.Fatalf("command item should reuse existing process tool instead of fragmenting: %#v", run.run.Events)
	}
	if !hasToolArgsContaining(run.run.Events, "proc-1", "go test ./...") || !hasToolCallEnd(run.run.Events, "proc-1") {
		t.Fatalf("command args/end should be attached to existing process tool: %#v", run.run.Events)
	}
	if got := countToolResult(run.run.Events, "proc-1", "ok\n"); got != 1 {
		t.Fatalf("streamed process output should not replay aggregate output, got %d events=%#v", got, run.run.Events)
	}
	assertAGUISequenceValid(t, run.run)
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
	assertAGUISequenceValid(t, run.run)
}

func TestRawResponseReasoningCompletionFillsMissingSectionsOnly(t *testing.T) {
	run := newActiveRun(nil, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1")
	run.handle("item/reasoning/textDelta", []byte(`{"threadId":"thread-1","turnId":"turn-1","itemId":"reason-1","contentIndex":0,"delta":"streamed thought"}`))
	run.handle("rawResponseItem/completed", []byte(`{"threadId":"thread-1","turnId":"turn-1","item":{"type":"reasoning","id":"reason-1","summary":[{"type":"summary_text","text":"completed summary"}],"content":[{"type":"reasoning_text","text":"streamed thought"}]}}`))

	if countReasoningDelta(run.run.Events, "completed summary") != 1 {
		t.Fatalf("expected raw reasoning completion to fill missing summary, got %#v", run.run.Events)
	}
	if countReasoningDelta(run.run.Events, "streamed thought") != 1 {
		t.Fatalf("expected raw reasoning completion not to duplicate streamed content, got %#v", run.run.Events)
	}
	assertAGUISequenceValid(t, run.run)
}

func TestRawToolResultSynthesizesMissingStart(t *testing.T) {
	run := newActiveRun(nil, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1")
	run.handle("rawResponseItem/completed", []byte(`{"threadId":"thread-1","turnId":"turn-1","item":{"type":"function_call_output","call_id":"call-1","output":"found"}}`))

	if !toolEventsInOrder(run.run.Events, "call-1", agui.EventToolCallStart, agui.EventToolCallResult) {
		t.Fatalf("expected raw tool result to synthesize a start first, got %#v", run.run.Events)
	}
	assertAGUISequenceValid(t, run.run)
}

func TestRawToolResultWithoutOutputOnlySynthesizesStart(t *testing.T) {
	run := newActiveRun(nil, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1")
	run.handle("rawResponseItem/completed", []byte(`{"threadId":"thread-1","turnId":"turn-1","item":{"type":"function_call_output","call_id":"call-1"}}`))

	if !hasToolCallStart(run.run.Events, "call-1") {
		t.Fatalf("expected raw tool output without content to synthesize start, got %#v", run.run.Events)
	}
	if hasEventType(run.run.Events, agui.EventToolCallResult) || hasToolResult(run.run.Events, "call-1", "null") {
		t.Fatalf("missing raw tool output should not create a result, got %#v", run.run.Events)
	}
	assertAGUISequenceValid(t, run.run)
}

func TestRawToolSearchOutputMapsToolsResult(t *testing.T) {
	run := newActiveRun(nil, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1")
	run.handle("rawResponseItem/completed", []byte(`{"threadId":"thread-1","turnId":"turn-1","item":{"type":"tool_search_output","call_id":"search-1","status":"completed","execution":"tool_search","tools":[{"name":"github","description":"GitHub connector"}]}}`))

	if !hasToolResultStateContaining(run.run.Events, "search-1", "GitHub connector", agui.ToolResultStateComplete) {
		t.Fatalf("expected tool_search_output tools to map to a tool result, got %#v", run.run.Events)
	}
	assertAGUISequenceValid(t, run.run)
}

func TestRawToolEndMapsOutputBeforeEnd(t *testing.T) {
	run := newActiveRun(nil, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1")
	run.handle("rawResponseItem/completed", []byte(`{"threadId":"thread-1","turnId":"turn-1","item":{"type":"local_shell_call","call_id":"shell-1","status":"completed","output":"ok\n"}}`))

	if !toolEventsInOrder(run.run.Events, "shell-1", agui.EventToolCallStart, agui.EventToolCallResult, agui.EventToolCallEnd) {
		t.Fatalf("expected raw shell output before tool end, got %#v", run.run.Events)
	}
	if !hasToolResultStateContaining(run.run.Events, "shell-1", "ok", agui.ToolResultStateComplete) {
		t.Fatalf("expected raw shell output to map to tool result, got %#v", run.run.Events)
	}
	assertAGUISequenceValid(t, run.run)
}

func TestRawToolEndMapsResultBeforeEnd(t *testing.T) {
	run := newActiveRun(nil, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1")
	run.handle("rawResponseItem/completed", []byte(`{"threadId":"thread-1","turnId":"turn-1","item":{"type":"image_generation_call","id":"image-1","status":"completed","revised_prompt":"a clean bridge diagram","result":"mxc://example.com/image"}}`))

	if !toolEventsInOrder(run.run.Events, "image-1", agui.EventToolCallStart, agui.EventToolCallResult, agui.EventToolCallEnd) {
		t.Fatalf("expected raw image result before tool end, got %#v", run.run.Events)
	}
	if !hasToolResultStateContaining(run.run.Events, "image-1", "mxc://example.com/image", agui.ToolResultStateComplete) {
		t.Fatalf("expected raw image result to map to tool result, got %#v", run.run.Events)
	}
	assertAGUISequenceValid(t, run.run)
}

func TestRawToolEndPreservesStreamingStatus(t *testing.T) {
	run := newActiveRun(nil, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1")
	run.handle("rawResponseItem/completed", []byte(`{"threadId":"thread-1","turnId":"turn-1","item":{"type":"web_search_call","id":"search-1","status":"in_progress","action":{"query":"codex bridge"},"result":"searching"}}`))

	if !hasToolResultState(run.run.Events, "search-1", "searching", agui.ToolResultStateStreaming) {
		t.Fatalf("expected raw in-progress tool status to stay streaming, got %#v", run.run.Events)
	}
	if hasToolCallEnd(run.run.Events, "search-1") {
		t.Fatalf("streaming raw tool should not be closed, got %#v", run.run.Events)
	}
	assertAGUISequenceValid(t, run.run)
}

func TestRawToolEndSynthesizesIDWhenCodexOmitsCallID(t *testing.T) {
	run := newActiveRun(nil, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1")
	item := map[string]any{
		"type":   "web_search_call",
		"status": "completed",
		"action": map[string]any{"query": "codex bridge streaming"},
	}
	callID := rawToolCallID(item)
	payload, err := json.Marshal(map[string]any{
		"threadId": "thread-1",
		"turnId":   "turn-1",
		"item":     item,
	})
	if err != nil {
		t.Fatal(err)
	}
	run.handle("rawResponseItem/completed", payload)

	if !strings.HasPrefix(callID, "raw_web_search_call_") {
		t.Fatalf("unexpected synthetic raw tool ID: %q", callID)
	}
	if !toolEventsInOrder(run.run.Events, callID, agui.EventToolCallStart, agui.EventToolCallEnd) {
		t.Fatalf("expected raw web search to synthesize a stable tool ID, got %#v", run.run.Events)
	}
	if !hasToolArgsContaining(run.run.Events, callID, "codex bridge streaming") {
		t.Fatalf("expected raw web search action to be preserved as tool input, got %#v", run.run.Events)
	}
	if hasToolResult(run.run.Events, callID, "null") {
		t.Fatalf("missing raw web search output should not be bridged as a null tool result: %#v", run.run.Events)
	}
	assertAGUISequenceValid(t, run.run)
}

func TestRawResponseItemCompletedMapsCompactionTriggerNotice(t *testing.T) {
	run := newActiveRun(nil, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1")
	run.handle("rawResponseItem/completed", []byte(`{"threadId":"thread-1","turnId":"turn-1","item":{"type":"compaction_trigger"}}`))

	if countTextDelta(run.run.Events, codexCompactionNotice) != 1 {
		t.Fatalf("expected compaction trigger notice, got %#v", run.run.Events)
	}
	assertAGUISequenceValid(t, run.run)
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

func TestCompletedMCPToolItemMapsContentText(t *testing.T) {
	run := newActiveRun(nil, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1")
	run.handle("item/started", []byte(`{"threadId":"thread-1","turnId":"turn-1","item":{"type":"mcpToolCall","id":"mcp-1","server":"github","tool":"list_issues","status":"inProgress"}}`))
	run.handle("item/completed", []byte(`{"threadId":"thread-1","turnId":"turn-1","item":{"type":"mcpToolCall","id":"mcp-1","server":"github","tool":"list_issues","status":"completed","result":{"content":[{"type":"text","text":"found two issues"}],"structuredContent":{"count":2},"_meta":null}}}`))

	if !hasToolResultState(run.run.Events, "mcp-1", "found two issues", agui.ToolResultStateComplete) {
		t.Fatalf("expected MCP content text to map to AG-UI tool result, got %#v", run.run.Events)
	}
}

func TestCompletedToolItemSynthesizesMissingStart(t *testing.T) {
	run := newActiveRun(nil, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1")
	run.handle("item/completed", []byte(`{"threadId":"thread-1","turnId":"turn-1","item":{"type":"mcpToolCall","id":"mcp-1","server":"github","tool":"list_issues","status":"completed","result":{"count":2}}}`))

	if !hasToolCallStart(run.run.Events, "mcp-1") {
		t.Fatalf("expected completed tool item to synthesize AG-UI tool start, got %#v", run.run.Events)
	}
	if !hasToolResultStateContaining(run.run.Events, "mcp-1", `"count":2`, agui.ToolResultStateComplete) {
		t.Fatalf("expected completed tool result, got %#v", run.run.Events)
	}
	if !hasToolCallEnd(run.run.Events, "mcp-1") {
		t.Fatalf("expected completed tool item to emit AG-UI tool end, got %#v", run.run.Events)
	}
	if !toolEventsInOrder(run.run.Events, "mcp-1", agui.EventToolCallStart, agui.EventToolCallResult, agui.EventToolCallEnd) {
		t.Fatalf("expected start/result/end order for completed tool item, got %#v", run.run.Events)
	}
	assertAGUISequenceValid(t, run.run)
}

func TestToolItemStartMapsInputArgs(t *testing.T) {
	run := newActiveRun(nil, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1")
	run.handle("item/started", []byte(`{"threadId":"thread-1","turnId":"turn-1","item":{"type":"mcpToolCall","id":"mcp-1","server":"github","tool":"list_issues","arguments":{"state":"open"}}}`))

	if !hasToolArgsContaining(run.run.Events, "mcp-1", `"state":"open"`) {
		t.Fatalf("expected tool arguments to be mapped to AG-UI, got %#v", run.run.Events)
	}
}

func TestToolItemStartMapsRichGeneratedItemInputs(t *testing.T) {
	tests := []struct {
		name     string
		payload  string
		toolID   string
		toolName string
		argText  string
	}{
		{
			name:     "web search",
			payload:  `{"threadId":"thread-1","turnId":"turn-1","item":{"type":"webSearch","id":"search-1","query":"codex bridge streaming"}}`,
			toolID:   "search-1",
			toolName: "web search: codex bridge streaming",
			argText:  "codex bridge streaming",
		},
		{
			name:     "image view",
			payload:  `{"threadId":"thread-1","turnId":"turn-1","item":{"type":"imageView","id":"image-1","path":"/tmp/screenshots/current.png"}}`,
			toolID:   "image-1",
			toolName: "image view: current.png",
			argText:  "/tmp/screenshots/current.png",
		},
		{
			name:     "collab agent",
			payload:  `{"threadId":"thread-1","turnId":"turn-1","item":{"type":"collabAgentToolCall","id":"collab-1","tool":"spawn","prompt":"check the stream mapping"}}`,
			toolID:   "collab-1",
			toolName: "collab: spawn",
			argText:  "check the stream mapping",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			run := newActiveRun(nil, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1")
			run.handle("item/started", []byte(tt.payload))
			if !hasToolCallStartName(run.run.Events, tt.toolID, tt.toolName) {
				t.Fatalf("expected rich tool start name %q for %s, got %#v", tt.toolName, tt.toolID, run.run.Events)
			}
			if !hasToolArgsContaining(run.run.Events, tt.toolID, tt.argText) {
				t.Fatalf("expected rich tool args %q for %s, got %#v", tt.argText, tt.toolID, run.run.Events)
			}
		})
	}
}

func TestCompletedToolItemsMapDynamicAndImageGenerationResults(t *testing.T) {
	run := newActiveRun(nil, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1")
	run.handle("item/completed", []byte(`{"threadId":"thread-1","turnId":"turn-1","item":{"type":"dynamicToolCall","id":"dynamic-1","tool":"custom_tool","arguments":{"query":"codex"},"status":"completed","contentItems":[{"type":"inputText","text":"dynamic result"}]}}`))
	run.handle("item/completed", []byte(`{"threadId":"thread-1","turnId":"turn-1","item":{"type":"imageGeneration","id":"image-1","status":"completed","revisedPrompt":"small bridge diagram","result":"generated image","savedPath":"/tmp/codex/image.png"}}`))

	if !hasToolResultStateContaining(run.run.Events, "dynamic-1", "dynamic result", agui.ToolResultStateComplete) {
		t.Fatalf("expected dynamic content item text as result, got %#v", run.run.Events)
	}
	if !hasToolArgsContaining(run.run.Events, "image-1", "small bridge diagram") || !hasToolResultStateContaining(run.run.Events, "image-1", "generated image", agui.ToolResultStateComplete) {
		t.Fatalf("expected image generation prompt/result in live stream, got %#v", run.run.Events)
	}
	assertAGUISequenceValid(t, run.run)
}

func TestTypeScriptV2CodexThreadItemsHaveLiveMapping(t *testing.T) {
	for _, itemType := range generatedTypeScriptThreadItemTypes(t) {
		if !isLiveMappedThreadItemType(itemType) {
			t.Fatalf("TypeScript v2 Codex thread item %q is not covered by live stream mapping", itemType)
		}
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

func TestCompletedStreamingToolItemDoesNotCloseAsComplete(t *testing.T) {
	run := newActiveRun(nil, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1")
	run.handle("item/completed", []byte(`{"threadId":"thread-1","turnId":"turn-1","item":{"type":"commandExecution","id":"cmd-1","command":"go test ./...","status":"inProgress","aggregatedOutput":"still running"}}`))

	if !hasToolResultState(run.run.Events, "cmd-1", "still running", agui.ToolResultStateStreaming) {
		t.Fatalf("expected completed in-progress item to remain streaming, got %#v", run.run.Events)
	}
	if hasToolCallEnd(run.run.Events, "cmd-1") {
		t.Fatalf("completed in-progress item should not close the tool, got %#v", run.run.Events)
	}
	assertAGUISequenceValid(t, run.run)
}

func TestMCPToolProgressMapsToStreamingToolResult(t *testing.T) {
	run := newActiveRun(nil, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1")
	run.handle("item/started", []byte(`{"threadId":"thread-1","turnId":"turn-1","item":{"type":"mcpToolCall","id":"mcp-1","server":"github","tool":"list_issues","status":"inProgress"}}`))
	run.handle("item/mcpToolCall/progress", []byte(`{"threadId":"thread-1","turnId":"turn-1","itemId":"mcp-1","message":"Fetching issues"}`))

	if !hasToolResultState(run.run.Events, "mcp-1", "Fetching issues", agui.ToolResultStateStreaming) {
		t.Fatalf("expected MCP progress to map to streaming tool result, got %#v", run.run.Events)
	}
	assertAGUISequenceValid(t, run.run)
}

func TestToolDeltasSynthesizeMissingStart(t *testing.T) {
	run := newActiveRun(nil, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1")
	run.handle("command/exec/outputDelta", []byte(`{"processId":"proc-1","deltaBase64":"b2sK","stream":"stdout"}`))
	run.handle("process/exited", []byte(`{"processHandle":"proc-2","exitCode":2}`))
	run.handle("item/mcpToolCall/progress", []byte(`{"threadId":"thread-1","turnId":"turn-1","itemId":"mcp-1","message":"Fetching issues"}`))
	run.handle("item/fileChange/patchUpdated", []byte(`{"threadId":"thread-1","turnId":"turn-1","itemId":"patch-1","changes":[{"path":"pkg/bridge/stream.go","kind":"update","diff":"@@\n-old\n+new"}]}`))
	run.handle("item/autoApprovalReview/completed", []byte(`{"threadId":"thread-1","turnId":"turn-1","targetItemId":"cmd-1","review":{"status":"approved","rationale":"ok"}}`))

	for _, toolID := range []string{"proc-1", "proc-2", "mcp-1", "patch-1", "cmd-1"} {
		if !hasToolCallStart(run.run.Events, toolID) {
			t.Fatalf("expected synthetic tool start for %s, got %#v", toolID, run.run.Events)
		}
	}
	assertAGUISequenceValid(t, run.run)
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
	assertAGUISequenceValid(t, run.run)
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

func TestCompletedHookPromptItemMapsToStateDelta(t *testing.T) {
	run := newActiveRun(nil, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1")
	run.handle("item/completed", []byte(`{"threadId":"thread-1","turnId":"turn-1","item":{"type":"hookPrompt","id":"hook-prompt-1","fragments":[{"text":"Before compacting, preserve approvals."}]}}`))

	if !hasStateDeltaText(run.run.Events, "hookPromptText", "Before compacting, preserve approvals.") {
		t.Fatalf("expected completed hook prompt state delta, got %#v", run.run.Events)
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

func assertAGUISequenceValid(t *testing.T, run *aistream.Run) {
	t.Helper()
	if err := run.Validate(); err != nil {
		t.Fatalf("invalid AG-UI event: %v\n%#v", err, run.Events)
	}
	if err := agui.ValidateEventSequence(run.Events); err != nil {
		t.Fatalf("invalid AG-UI sequence: %v\n%#v", err, run.Events)
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

func distinctReasoningMessageIDs(events []agui.Event) map[string]bool {
	ids := map[string]bool{}
	for _, event := range events {
		if event.Type() == agui.EventReasoningMsgStart {
			ids[event.String("messageId")] = true
		}
	}
	return ids
}

func hasToolResult(events []agui.Event, toolCallID, delta string) bool {
	for _, event := range events {
		if event.Type() == agui.EventToolCallResult && event.String("toolCallId") == toolCallID && event.String("content") == delta {
			return true
		}
	}
	return false
}

func hasToolCallStart(events []agui.Event, toolCallID string) bool {
	for _, event := range events {
		if event.Type() == agui.EventToolCallStart && event.String("toolCallId") == toolCallID {
			return true
		}
	}
	return false
}

func hasToolCallStartName(events []agui.Event, toolCallID, name string) bool {
	for _, event := range events {
		if event.Type() == agui.EventToolCallStart && event.String("toolCallId") == toolCallID && event.String("toolCallName") == name {
			return true
		}
	}
	return false
}

func isLiveMappedThreadItemType(itemType string) bool {
	switch itemType {
	case "userMessage",
		"agentMessage",
		"contextCompaction",
		"reasoning",
		"hookPrompt",
		"enteredReviewMode",
		"exitedReviewMode",
		"plan":
		return true
	default:
		return (codexItem{Type: itemType}).IsToolLike()
	}
}

func hasToolCallEnd(events []agui.Event, toolCallID string) bool {
	for _, event := range events {
		if event.Type() == agui.EventToolCallEnd && event.String("toolCallId") == toolCallID {
			return true
		}
	}
	return false
}

func hasToolArgsContaining(events []agui.Event, toolCallID, delta string) bool {
	for _, event := range events {
		if event.Type() == agui.EventToolCallArgs && event.String("toolCallId") == toolCallID && strings.Contains(event.String("delta"), delta) {
			return true
		}
	}
	return false
}

func toolEventsInOrder(events []agui.Event, toolCallID string, eventTypes ...string) bool {
	next := 0
	for _, event := range events {
		if next >= len(eventTypes) || event.String("toolCallId") != toolCallID {
			continue
		}
		if event.Type() == eventTypes[next] {
			next++
		}
	}
	return next == len(eventTypes)
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

func hasStateDeltaText(events []agui.Event, key, text string) bool {
	for _, event := range events {
		if event.Type() != agui.EventStateDelta {
			continue
		}
		delta, _ := event.Get("delta").(map[string]any)
		codex, _ := delta["codex"].(map[string]any)
		if strings.Contains(anyString(codex[key]), text) {
			return true
		}
	}
	return false
}

func hasRealtimeStateDeltaText(events []agui.Event, method, key, text string) bool {
	for _, event := range events {
		if event.Type() != agui.EventStateDelta {
			continue
		}
		delta, _ := event.Get("delta").(map[string]any)
		realtime, _ := delta["codexRealtime"].(map[string]any)
		payload, _ := realtime[method].(map[string]any)
		if strings.Contains(anyString(payload[key]), text) {
			return true
		}
	}
	return false
}

func hasCodexThreadStateDelta(events []agui.Event, key, text string) bool {
	for _, event := range events {
		if event.Type() != agui.EventStateDelta {
			continue
		}
		delta, _ := event.Get("delta").(map[string]any)
		thread, _ := delta["codexThread"].(map[string]any)
		if strings.Contains(anyString(thread[key]), text) {
			return true
		}
	}
	return false
}

func hasCustomPayloadText(events []agui.Event, name, key, text string) bool {
	for _, event := range events {
		if event.Type() != agui.EventCustom || event.String("name") != name {
			continue
		}
		value, _ := event.Get("value").(map[string]any)
		if strings.Contains(anyString(value[key]), text) {
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

func assertCodexProfile(t *testing.T, content *event.MessageEventContent) {
	t.Helper()
	if content == nil || content.BeeperPerMessageProfile == nil {
		t.Fatalf("missing Codex per-message profile: %#v", content)
	}
	profile := content.BeeperPerMessageProfile
	if profile.ID != string(codexUserID) || profile.Displayname != "Codex" || profile.AvatarURL == nil || string(*profile.AvatarURL) != defaultCodexAvatarMXC || !profile.HasFallback {
		t.Fatalf("unexpected Codex per-message profile: %#v", profile)
	}
}

func requireEventually(t *testing.T, timeout time.Duration, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !ok() {
		t.Fatalf("condition was not met within %s", timeout)
	}
}

type recordingBeeperStreamPublisher struct {
	mu            sync.Mutex
	roomID        id.RoomID
	eventID       id.EventID
	descriptor    *event.BeeperStreamInfo
	updates       []map[string]any
	unregisters   int
	publishes     int
	failOnPublish int
	failErr       error

	deviceID      id.DeviceID
	encryptionKey []byte
}

type deterministicMatrix struct {
	fakeMatrixConnector
	eventID id.EventID
}

func (m deterministicMatrix) GenerateDeterministicEventID(id.RoomID, networkid.PortalKey, networkid.MessageID, networkid.PartID) id.EventID {
	return m.eventID
}

type streamStartMatrix struct {
	fakeMatrixConnector
	publisher bridgev2.BeeperStreamPublisher
	eventID   id.EventID
}

func (m *streamStartMatrix) GetBeeperStreamPublisher() bridgev2.BeeperStreamPublisher {
	return m.publisher
}

func (m *streamStartMatrix) GenerateDeterministicEventID(id.RoomID, networkid.PortalKey, networkid.MessageID, networkid.PartID) id.EventID {
	return m.eventID
}

type recordingMediaIntent struct {
	bridgev2.MatrixAPI
	roomID   id.RoomID
	data     []byte
	fileName string
	mimeType string
	url      id.ContentURIString
}

func (i *recordingMediaIntent) UploadMedia(ctx context.Context, roomID id.RoomID, data []byte, fileName, mimeType string) (id.ContentURIString, *event.EncryptedFileInfo, error) {
	i.roomID = roomID
	i.data = append([]byte(nil), data...)
	i.fileName = fileName
	i.mimeType = mimeType
	i.url = "mxc://example/final-parts"
	return i.url, nil, nil
}

type codexSendToDeviceRequest struct {
	path string
	body []byte
}

type codexSendToDeviceRecorder struct {
	requests chan codexSendToDeviceRequest
}

func newCodexStreamHelper(t *testing.T, userID id.UserID, deviceID id.DeviceID) (*mautrix.Client, *beeperstream.Helper, *codexSendToDeviceRecorder) {
	t.Helper()
	recorder := &codexSendToDeviceRecorder{requests: make(chan codexSendToDeviceRequest, 8)}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Errorf("unexpected sendToDevice method %s", r.Method)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("failed to read sendToDevice body: %v", err)
		}
		recorder.requests <- codexSendToDeviceRequest{path: r.URL.Path, body: body}
		_ = json.NewEncoder(w).Encode(map[string]any{})
	}))
	t.Cleanup(server.Close)
	client, err := mautrix.NewClient(server.URL, userID, "access-token")
	if err != nil {
		t.Fatal(err)
	}
	client.DeviceID = deviceID
	client.StateStore = mautrix.NewMemoryStateStore()
	streams, err := beeperstream.New(client)
	if err != nil {
		t.Fatal(err)
	}
	return client, streams, recorder
}

func (r *codexSendToDeviceRecorder) next(t *testing.T) codexSendToDeviceRequest {
	t.Helper()
	select {
	case req := <-r.requests:
		return req
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for sendToDevice request")
		return codexSendToDeviceRequest{}
	}
}

func (r *codexSendToDeviceRecorder) rawContent(t *testing.T, req codexSendToDeviceRequest, userID id.UserID, deviceID id.DeviceID) json.RawMessage {
	t.Helper()
	var payload struct {
		Messages map[string]map[string]json.RawMessage `json:"messages"`
	}
	if err := json.Unmarshal(req.body, &payload); err != nil {
		t.Fatal(err)
	}
	userMessages := payload.Messages[string(userID)]
	if userMessages == nil {
		t.Fatalf("sendToDevice request did not target %s: %#v", userID, payload.Messages)
	}
	raw := userMessages[string(deviceID)]
	if raw == nil {
		t.Fatalf("sendToDevice request did not target %s/%s: %#v", userID, deviceID, userMessages)
	}
	return raw
}

func codexStreamToDeviceEvent(t *testing.T, evtType event.Type, sender id.UserID, toUser id.UserID, toDevice id.DeviceID, raw json.RawMessage) *event.Event {
	t.Helper()
	return &event.Event{
		Sender:     sender,
		ToUserID:   toUser,
		ToDeviceID: toDevice,
		Type:       evtType,
		Content:    event.Content{VeryRaw: raw},
	}
}

type fixedBeeperStreamPublisher struct {
	helper     *beeperstream.Helper
	descriptor *event.BeeperStreamInfo
}

func (p *fixedBeeperStreamPublisher) NewDescriptor(ctx context.Context, roomID id.RoomID, streamType string) (*event.BeeperStreamInfo, error) {
	return p.descriptor.Clone(), nil
}

func (p *fixedBeeperStreamPublisher) Register(ctx context.Context, roomID id.RoomID, eventID id.EventID, descriptor *event.BeeperStreamInfo) error {
	return p.helper.Register(ctx, roomID, eventID, descriptor)
}

func (p *fixedBeeperStreamPublisher) Publish(ctx context.Context, roomID id.RoomID, eventID id.EventID, delta map[string]any) error {
	return p.helper.Publish(ctx, roomID, eventID, delta)
}

func (p *fixedBeeperStreamPublisher) Unregister(roomID id.RoomID, eventID id.EventID) {
	p.helper.Unregister(roomID, eventID)
}

func (p *recordingBeeperStreamPublisher) NewDescriptor(ctx context.Context, roomID id.RoomID, streamType string) (*event.BeeperStreamInfo, error) {
	info := &event.BeeperStreamInfo{UserID: "@bot:example.com", DeviceID: p.deviceID, Type: streamType}
	if len(p.encryptionKey) > 0 {
		info.Encryption = &event.BeeperStreamEncryptionInfo{
			Algorithm: id.AlgorithmBeeperStreamV1,
			Key:       append([]byte(nil), p.encryptionKey...),
		}
	}
	return info, nil
}

func (p *recordingBeeperStreamPublisher) Register(ctx context.Context, roomID id.RoomID, eventID id.EventID, descriptor *event.BeeperStreamInfo) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.roomID = roomID
	p.eventID = eventID
	p.descriptor = descriptor
	return nil
}

func (p *recordingBeeperStreamPublisher) Publish(ctx context.Context, roomID id.RoomID, eventID id.EventID, delta map[string]any) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.publishes++
	if p.failOnPublish == p.publishes {
		if p.failErr != nil {
			return p.failErr
		}
		return errors.New("publish failed")
	}
	p.roomID = roomID
	p.eventID = eventID
	p.updates = append(p.updates, delta)
	return nil
}

func (p *recordingBeeperStreamPublisher) Unregister(roomID id.RoomID, eventID id.EventID) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.unregisters++
}

func (p *recordingBeeperStreamPublisher) unregisterCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.unregisters
}

var _ bridgev2.BeeperStreamPublisher = (*recordingBeeperStreamPublisher)(nil)
var _ bridgev2.BeeperStreamPublisher = (*fixedBeeperStreamPublisher)(nil)
