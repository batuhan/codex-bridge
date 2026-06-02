package bridge

import (
	"context"
	"testing"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

func TestIsThreadMetadataNotification(t *testing.T) {
	for _, method := range []string{
		"thread/status/changed",
		"thread/name/updated",
		"thread/tokenUsage/updated",
		"thread/settings/updated",
		"thread/closed",
		"thread/compacted",
		"model/rerouted",
	} {
		if !isThreadMetadataNotification(method) {
			t.Fatalf("expected %s to be metadata", method)
		}
	}
	for _, method := range []string{
		"item/agentMessage/delta",
		"item/commandExecution/outputDelta",
		"turn/started",
	} {
		if isThreadMetadataNotification(method) {
			t.Fatalf("did not expect %s to be metadata", method)
		}
	}
}

func TestCodexThreadState(t *testing.T) {
	state := codexThreadState("thread/archived", "thread-1", "/tmp/project", []byte(`{"threadId":"thread-1"}`))
	if state["threadId"] != "thread-1" || state["cwd"] != "/tmp/project" || state["lastNotification"] != "thread/archived" || state["archived"] != true {
		t.Fatalf("unexpected archived state: %#v", state)
	}

	state = codexThreadState("thread/closed", "thread-1", "", []byte(`{"threadId":"thread-1"}`))
	if state["closed"] != true {
		t.Fatalf("unexpected closed state: %#v", state)
	}

	state = codexThreadState("thread/started", "thread-2", "", []byte(`{"thread":{"id":"thread-2","sessionId":"session-1","cwd":"/tmp/project","name":"Bridge","status":"running","modelProvider":"openai"}}`))
	if state["threadId"] != "thread-2" || state["sessionId"] != "session-1" || state["cwd"] != "/tmp/project" || state["name"] != "Bridge" || state["status"] != "running" || state["modelProvider"] != "openai" {
		t.Fatalf("thread fields were not normalized: %#v", state)
	}

	state = codexThreadState("thread/status/changed", "thread-1", "", []byte(`{"threadId":"thread-1","status":{"type":"active","activeFlags":["agent"]}}`))
	if state["statusType"] != "active" {
		t.Fatalf("status type was not normalized: %#v", state)
	}
	if flags, ok := state["activeFlags"].([]any); !ok || len(flags) != 1 || flags[0] != "agent" {
		t.Fatalf("active flags were not normalized: %#v", state)
	}

	state = codexThreadState("thread/name/updated", "thread-1", "", []byte(`{"threadId":"thread-1","threadName":"New name"}`))
	if state["name"] != "New name" {
		t.Fatalf("thread name was not normalized: %#v", state)
	}

	state = codexThreadState("thread/goal/updated", "thread-1", "", []byte(`{"threadId":"thread-1","goal":{"objective":"ship","status":"active","tokensUsed":10,"tokenBudget":100,"updatedAt":7}}`))
	if state["goalStatus"] != "active" || state["goalObjective"] != "ship" || state["goalTokensUsed"].(float64) != 10 || state["goalTokenBudget"].(float64) != 100 || state["goalUpdatedAt"].(float64) != 7 {
		t.Fatalf("goal fields were not normalized: %#v", state)
	}

	state = codexThreadState("thread/goal/cleared", "thread-1", "", []byte(`{"threadId":"thread-1"}`))
	if state["goal"] != nil || state["goalStatus"] != "cleared" {
		t.Fatalf("goal clear was not normalized: %#v", state)
	}

	state = codexThreadState("thread/tokenUsage/updated", "thread-1", "", []byte(`{"threadId":"thread-1","tokenUsage":{"last":{"inputTokens":1},"total":{"inputTokens":3},"modelContextWindow":200000}}`))
	if state["tokenUsageLast"] == nil || state["tokenUsageTotal"] == nil || state["modelContextWindow"].(float64) != 200000 {
		t.Fatalf("token usage was not normalized: %#v", state)
	}

	state = codexThreadState("thread/settings/updated", "thread-1", "", []byte(`{"threadId":"thread-1","threadSettings":{"cwd":"/tmp/project","model":"gpt-5","modelProvider":"openai","approvalPolicy":"on-request","collaborationMode":"default"}}`))
	if state["cwd"] != "/tmp/project" || state["settings.cwd"] != "/tmp/project" || state["model"] != "gpt-5" || state["modelProvider"] != "openai" || state["approvalPolicy"] != "on-request" || state["collaborationMode"] != "default" {
		t.Fatalf("thread settings were not normalized: %#v", state)
	}

	state = codexThreadState("model/rerouted", "thread-1", "", []byte(`{"threadId":"thread-1","fromModel":"gpt-a","toModel":"gpt-b","reason":"policy"}`))
	if state["model"] != "gpt-b" || state["lastNotification"] != "model/rerouted" {
		t.Fatalf("model reroute was not normalized: %#v", state)
	}
}

func TestCodexAIModelStateContent(t *testing.T) {
	state := codexThreadState("thread/settings/updated", "thread-1", "", []byte(`{"threadId":"thread-1","threadSettings":{"model":"gpt-5","modelProvider":"openai","effort":"high"}}`))
	content := codexAIModelStateContent(state)
	if content["model"] != "openai/gpt-5" || content["reasoning"] != "high" {
		t.Fatalf("unexpected AI model state content: %#v", content)
	}

	state = codexThreadState("model/rerouted", "thread-1", "", []byte(`{"threadId":"thread-1","fromModel":"gpt-a","toModel":"openai/gpt-b","reason":"policy"}`))
	content = codexAIModelStateContent(state)
	if content["model"] != "openai/gpt-b" {
		t.Fatalf("reroute should write target model: %#v", content)
	}

	content = codexAIModelStateContent(map[string]any{"toModel": "gpt-b", "modelProvider": "openai"})
	if content["model"] != "openai/gpt-b" {
		t.Fatalf("reroute should use known provider: %#v", content)
	}

	if content = codexAIModelStateContent(map[string]any{"modelProvider": "openai"}); len(content) != 0 {
		t.Fatalf("blank model should not write AI model state: %#v", content)
	}
}

func TestChatNameFromThreadState(t *testing.T) {
	state := codexThreadState("thread/name/updated", "thread-1", "", []byte(`{"threadId":"thread-1","threadName":"Build bridge"}`))
	if got := chatNameFromThreadState(state, ""); got != "Build bridge" {
		t.Fatalf("unexpected name %q", got)
	}
	state = codexThreadState("thread/name/updated", "thread-1", "", []byte(`{"threadId":"thread-1","threadName":null}`))
	if got := chatNameFromThreadState(state, ""); got != "" {
		t.Fatalf("unexpected null name %q", got)
	}
	if got := chatNameFromThreadState(map[string]any{}, "/tmp/project"); got != "project" {
		t.Fatalf("unexpected cwd fallback name %q", got)
	}
}

func TestCodexThreadChatInfoSyncsMetadataNameAndBackfill(t *testing.T) {
	state := codexThreadState("thread/settings/updated", "thread-1", "/old/project", []byte(`{
		"threadId": "thread-1",
		"threadSettings": {
			"cwd": "/new/project",
			"model": "gpt-5"
		}
	}`))
	info := codexThreadChatInfo("/old/project", "thread-1", state)
	if info.Name == nil || *info.Name != "project" {
		t.Fatalf("expected room name from updated cwd, got %#v", info.Name)
	}
	if !info.ExcludeChangesFromTimeline || !info.CanBackfill {
		t.Fatalf("expected hidden metadata update with backfill enabled: %#v", info)
	}
	portal := &bridgev2.Portal{Portal: &database.Portal{
		PortalKey: networkid.PortalKey{ID: "project", Receiver: "codex"},
		Metadata:  &PortalMetadata{ThreadID: "thread-1", Cwd: "/old/project"},
	}}
	if !info.ExtraUpdates(context.Background(), portal) {
		t.Fatal("expected chat info extra updater to persist changed metadata")
	}
	meta := portalMetadata(portal.Metadata)
	if meta.ThreadID != "thread-1" || meta.Cwd != "/new/project" {
		t.Fatalf("unexpected portal metadata: %#v", meta)
	}

	state = codexThreadState("thread/name/updated", "thread-1", "/new/project", []byte(`{"threadId":"thread-1","threadName":"Build bridge"}`))
	info = codexThreadChatInfo("/new/project", "thread-1", state)
	if info.Name == nil || *info.Name != "Build bridge" {
		t.Fatalf("expected explicit thread name, got %#v", info.Name)
	}
}

func TestThreadMetadataUpdaterPersistsPortalMetadata(t *testing.T) {
	state := codexThreadState("thread/settings/updated", "thread-1", "/old/project", []byte(`{
		"threadId": "thread-1",
		"threadSettings": {
			"cwd": "/new/project",
			"model": "gpt-5"
		}
	}`))
	portal := &bridgev2.Portal{Portal: &database.Portal{
		PortalKey: networkid.PortalKey{ID: "project", Receiver: "codex"},
		Metadata:  &PortalMetadata{ThreadID: "thread-1", Cwd: "/old/project"},
	}}
	if !codexThreadMetadataUpdater("/new/project", "thread-1", state)(context.Background(), portal) {
		t.Fatal("expected metadata updater to report a change")
	}
	meta := portalMetadata(portal.Metadata)
	if meta.ThreadID != "thread-1" || meta.Cwd != "/new/project" {
		t.Fatalf("unexpected portal metadata: %#v", meta)
	}
}

func TestBlankPortalMetadataUpdaterDoesNotClearSessionMetadata(t *testing.T) {
	portal := &bridgev2.Portal{Portal: &database.Portal{
		PortalKey: networkid.PortalKey{ID: "project", Receiver: "codex"},
		Metadata:  &PortalMetadata{ThreadID: "thread-1", Cwd: "/tmp/project"},
	}}
	if codexPortalMetadataUpdater("", "")(context.Background(), portal) {
		t.Fatal("blank starter metadata should not report a session metadata change")
	}
	meta := portalMetadata(portal.Metadata)
	if meta.ThreadID != "thread-1" || meta.Cwd != "/tmp/project" {
		t.Fatalf("session metadata was cleared: %#v", meta)
	}
}

func TestMessageMetadataMergesTypedFields(t *testing.T) {
	meta := &MessageMetadata{Role: "assistant", ThreadID: "thread-1"}
	meta.CopyFrom(&MessageMetadata{TurnID: "turn-1", StreamStatus: "complete"})
	if meta.Role != "assistant" || meta.ThreadID != "thread-1" || meta.TurnID != "turn-1" || meta.StreamStatus != "complete" {
		t.Fatalf("unexpected merged metadata: %#v", meta)
	}
	meta.CopyFrom(map[string]any{"role": "map"})
	if meta.Role != "assistant" {
		t.Fatalf("untyped metadata should not merge: %#v", meta)
	}
}

func TestConnectorUsesTypedMessageMetadata(t *testing.T) {
	meta := (&Connector{}).GetDBMetaTypes().Message()
	if _, ok := meta.(*MessageMetadata); !ok {
		t.Fatalf("unexpected message metadata type %T", meta)
	}
}

func TestThreadNoticeText(t *testing.T) {
	if got := threadNoticeText("thread/compacted", []byte(`{"threadId":"thread-1"}`)); got == "" {
		t.Fatal("thread compaction should produce a user-visible notice")
	}
	got := threadNoticeText("model/rerouted", []byte(`{"threadId":"thread-1","fromModel":"gpt-a","toModel":"gpt-b","reason":"policy"}`))
	if got != "Codex switched models from gpt-a to gpt-b.\n\nReason: policy" {
		t.Fatalf("unexpected reroute notice: %q", got)
	}
	got = threadNoticeText("error", []byte(`{"threadId":"thread-1","error":{"message":"failed"}}`))
	if got != "Codex error:\n\nfailed" {
		t.Fatalf("unexpected error notice: %q", got)
	}
	got = threadNoticeText("thread/realtime/error", []byte(`{"threadId":"thread-1","message":"audio failed"}`))
	if got != "Codex realtime error:\n\naudio failed" {
		t.Fatalf("unexpected realtime error notice: %q", got)
	}
	got = threadNoticeText("configWarning", []byte(`{"summary":"bad config","details":"line 4"}`))
	if got != "Codex configWarning:\n\nbad config\n\nline 4" {
		t.Fatalf("unexpected config warning notice: %q", got)
	}
}

func TestNotificationThreadID(t *testing.T) {
	tests := map[string]string{
		`{"threadId":"thread-1"}`:                               "thread-1",
		`{"conversationId":"legacy-1"}`:                         "legacy-1",
		`{"thread":{"id":"nested-1"}}`:                          "nested-1",
		`{"turn":{"threadId":"turn-thread"}}`:                   "turn-thread",
		`{"params":{"item":{"threadId":"deep-thread"}}}`:        "deep-thread",
		`{"request":{"params":{"conversationId":"request-1"}}}`: "request-1",
	}
	for raw, want := range tests {
		if got := notificationThreadID([]byte(raw)); got != want {
			t.Fatalf("notificationThreadID(%s) = %q, want %q", raw, got, want)
		}
	}
}

func TestNotificationTurnID(t *testing.T) {
	tests := map[string]string{
		`{"turnId":"turn-1"}`:                                     "turn-1",
		`{"expectedTurnId":"turn-2"}`:                             "turn-2",
		`{"turn":{"id":"turn-3"}}`:                                "turn-3",
		`{"item":{"turnId":"turn-4"}}`:                            "turn-4",
		`{"params":{"request":{"turnId":"turn-5"}}}`:              "turn-5",
		`{"request":{"params":{"expectedTurnId":"turn-6"}}}`:      "turn-6",
		`{"request":{"params":{"turn":{"id":"turn-7"}}}}`:         "turn-7",
		`{"threadId":"thread-1","item":{"id":"item-no-turn-id"}}`: "",
	}
	for raw, want := range tests {
		if got := notificationTurnID([]byte(raw)); got != want {
			t.Fatalf("notificationTurnID(%s) = %q, want %q", raw, got, want)
		}
	}
}

func TestNotificationProcessID(t *testing.T) {
	tests := map[string]string{
		`{"processId":"proc-1","deltaBase64":"b2s="}`:       "proc-1",
		`{"processHandle":"handle-1","deltaBase64":"b2s="}`: "handle-1",
		`{"threadId":"thread-1"}`:                           "",
	}
	for raw, want := range tests {
		if got := notificationProcessID([]byte(raw)); got != want {
			t.Fatalf("notificationProcessID(%s) = %q, want %q", raw, got, want)
		}
	}
}

func TestCanStartActiveRunFromNotification(t *testing.T) {
	for _, method := range []string{
		"turn/started",
		"item/started",
		"item/agentMessage/delta",
		"hook/started",
		"turn/diff/updated",
		"model/rerouted",
	} {
		if !canStartActiveRunFromNotification(method) {
			t.Fatalf("expected %s to be able to start an active run", method)
		}
	}
	for _, method := range []string{
		"turn/completed",
		"serverRequest/resolved",
		"thread/status/changed",
		"thread/compacted",
		"thread/realtime/closed",
	} {
		if canStartActiveRunFromNotification(method) {
			t.Fatalf("did not expect %s to start an active run", method)
		}
	}
}
