package bridge

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	aistream "github.com/beeper/ai-bridge/pkg/ai-stream"
	"github.com/beeper/codex-bridge/pkg/appserver"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/status"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
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
		"model/verification",
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

	state = codexThreadState("thread/started", "thread-2", "", []byte(`{"thread":{"id":"thread-2","sessionId":"session-1","cwd":"/tmp/project","name":"Bridge","status":"running","model":"gpt-5","modelName":"GPT-5","modelProvider":"openai","serviceTier":"priority","effort":"high","turns":[{"id":"turn-1"}]}}`))
	if state["threadId"] != "thread-2" || state["sessionId"] != "session-1" || state["cwd"] != "/tmp/project" || state["name"] != "Bridge" || state["status"] != "running" || state["model"] != "gpt-5" || state["modelName"] != "GPT-5" || state["modelProvider"] != "openai" || state["serviceTier"] != "priority" || state["effort"] != "high" {
		t.Fatalf("thread fields were not normalized: %#v", state)
	}
	if _, ok := state["turns"]; ok {
		t.Fatalf("thread turns should not be copied into room state: %#v", state)
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

	state = codexThreadState("thread/settings/updated", "thread-1", "", []byte(`{"threadId":"thread-1","threadSettings":{"cwd":"/tmp/project","model":"gpt-5","modelProvider":"openai","approvalPolicy":"on-request","collaborationMode":"default","personality":"concise"}}`))
	if state["cwd"] != "/tmp/project" || state["settings.cwd"] != "/tmp/project" || state["model"] != "gpt-5" || state["modelProvider"] != "openai" || state["approvalPolicy"] != "on-request" || state["collaborationMode"] != "default" || state["personality"] != "concise" || state["settings.personality"] != "concise" {
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

	content = codexAIModelStateContent(map[string]any{"model": "gpt-b", "modelProvider": "openai", "reasoningEffort": "medium", "reasoning_mode": "adaptive"})
	if content["model"] != "openai/gpt-b" || content["reasoning"] != "medium" || content["reasoning_mode"] != "adaptive" {
		t.Fatalf("reasoning effort alias should write AI model state: %#v", content)
	}

	content = codexAIModelStateContent(map[string]any{"model": "gpt-b", "modelProvider": "openai", "modelName": "GPT-B"})
	if content["model"] != "openai/gpt-b" || content["name"] != "GPT-B" {
		t.Fatalf("model display name should write AI model state: %#v", content)
	}

	content = codexAIModelStateContent(map[string]any{"model": "gpt-b", "modelProvider": "openai", "name": "Thread title"})
	if content["model"] != "openai/gpt-b" {
		t.Fatalf("thread name should not affect AI model state: %#v", content)
	}
	if _, ok := content["name"]; ok {
		t.Fatalf("generic thread name leaked into AI model state: %#v", content)
	}

	state = codexThreadState("thread/started", "thread-1", "", []byte(`{"thread":{"id":"thread-1","model":"gpt-5","modelName":"GPT-5","modelProvider":"openai"}}`))
	content = codexAIModelStateContent(state)
	if content["model"] != "openai/gpt-5" || content["name"] != "GPT-5" {
		t.Fatalf("thread start model display name should write AI model state: %#v", content)
	}

	if content = codexAIModelStateContent(map[string]any{"modelProvider": "openai"}); len(content) != 0 {
		t.Fatalf("blank model should not write AI model state: %#v", content)
	}
}

func TestApplyAIModelStateOptionalFields(t *testing.T) {
	content := map[string]any{"model": "openai/gpt-b"}
	state := map[string]any{
		"model":           "gpt-b",
		"modelProvider":   "openai",
		"modelName":       "GPT-B",
		"reasoningEffort": "medium",
		"reasoning_mode":  "adaptive",
	}
	applyAIModelStateOptionalFields(content, state)
	if content["reasoning"] != "medium" || content["reasoning_mode"] != "adaptive" || content["name"] != "GPT-B" {
		t.Fatalf("optional AI model fields not applied: %#v", content)
	}
}

func TestEnrichThreadStateWithModelStatePreservesMissingRoomModelFields(t *testing.T) {
	state := enrichThreadStateWithModelState(
		map[string]any{"threadId": "thread-1", "model": "gpt-5", "modelProvider": "openai"},
		map[string]any{"model": "openai/gpt-5", "name": "GPT-5", "reasoning": "high", "reasoning_mode": "adaptive"},
	)
	if state["model"] != "gpt-5" || state["modelName"] != "GPT-5" || state["reasoningEffort"] != "high" || state["reasoning_mode"] != "adaptive" {
		t.Fatalf("thread state did not preserve missing room model fields: %#v", state)
	}

	state = enrichThreadStateWithModelState(
		map[string]any{"threadId": "thread-1", "model": "gpt-5", "modelProvider": "openai", "effort": "medium", "reasoning_mode": "default"},
		map[string]any{"model": "openai/gpt-5.1", "reasoning": "high", "reasoning_mode": "adaptive"},
	)
	if state["model"] != "gpt-5" || state["effort"] != "medium" || state["reasoning_mode"] != "default" {
		t.Fatalf("thread state should not overwrite present model fields: %#v", state)
	}
}

func TestInitialThreadStateIncludesHydratedModelSettings(t *testing.T) {
	thread := appserver.ThreadOpenResponse{
		Thread:          appserver.Thread{ID: "thread-1", SessionID: "session-1"},
		Model:           "gpt-5",
		ModelProvider:   "openai",
		ServiceTier:     "priority",
		Cwd:             "/tmp/project",
		ReasoningEffort: "high",
	}.HydratedThread()

	state := codexThreadInitialState(thread)
	content := codexAIModelStateContent(state)
	if content["model"] != "openai/gpt-5" || content["reasoning"] != "high" || state["serviceTier"] != "priority" {
		t.Fatalf("initial state did not include model settings: state=%#v ai=%#v", state, content)
	}
}

func TestInitialThreadStateHydratesModelFromSessionFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	err := os.WriteFile(path, []byte(strings.Join([]string{
		`{"type":"turn_context","payload":{"cwd":"/tmp/project","model":"openai/gpt-5.5","effort":"high"}}`,
		`{"type":"event_msg","payload":{"type":"token_count"}}`,
	}, "\n")), 0600)
	if err != nil {
		t.Fatal(err)
	}
	state := codexThreadInitialState(appserver.Thread{
		ID:            "thread-1",
		SessionID:     "thread-1",
		Path:          path,
		ModelProvider: "openai",
		Raw:           map[string]any{"modelProvider": "openai"},
	})
	if state["model"] != "openai/gpt-5.5" || state["effort"] != "high" || state["cwd"] != "/tmp/project" {
		t.Fatalf("session model settings were not hydrated: %#v", state)
	}
	content := codexAIModelStateContent(state)
	if content["model"] != "openai/gpt-5.5" || content["reasoning"] != "high" {
		t.Fatalf("hydrated session settings did not project to AI model state: %#v", content)
	}
}

func TestCodexThreadChatInfoName(t *testing.T) {
	state := codexThreadState("thread/name/updated", "thread-1", "", []byte(`{"threadId":"thread-1","threadName":"Build bridge"}`))
	if info := codexThreadChatInfo("", "thread-1", state); info.Name == nil || *info.Name != "Build bridge" {
		t.Fatalf("unexpected name %#v", info.Name)
	}
	if info := codexThreadChatInfo("/tmp/project", "thread-1", state); info.Name == nil || *info.Name != "Build bridge (/tmp/project)" {
		t.Fatalf("unexpected titled project name %#v", info.Name)
	}
	state = codexThreadState("thread/name/updated", "thread-1", "/tmp/project", []byte(`{"threadId":"thread-1","generatedTitle":"Generated title","preview":"First user prompt"}`))
	if state["name"] != "Generated title" {
		t.Fatalf("generated title did not normalize to name: %#v", state)
	}
	if info := codexThreadChatInfo("/tmp/project", "thread-1", state); info.Name == nil || *info.Name != "Generated title (/tmp/project)" {
		t.Fatalf("unexpected generated title name %#v", info.Name)
	}
	state = codexThreadState("thread/name/updated", "thread-1", "", []byte(`{"threadId":"thread-1","threadName":null}`))
	if info := codexThreadChatInfo("", "thread-1", state); info.Name != nil {
		t.Fatalf("unexpected null name %#v", info.Name)
	}
	if info := codexThreadChatInfo("/tmp/project", "thread-1", map[string]any{}); info.Name == nil || *info.Name != "/tmp/project" {
		t.Fatalf("unexpected cwd fallback name %#v", info.Name)
	}
}

func TestThreadNameUsesGeneratedTitleFromRawState(t *testing.T) {
	got := threadName(appserver.Thread{
		ID:      "thread-1",
		Cwd:     "/tmp/project",
		Preview: "First user prompt",
		Raw: map[string]any{
			"generatedTitle": "Generated title",
		},
	})
	if got != "Generated title (/tmp/project)" {
		t.Fatalf("unexpected thread name %q", got)
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
	if info.Name == nil || *info.Name != "/new/project" {
		t.Fatalf("expected room name from updated cwd, got %#v", info.Name)
	}
	if !info.ExcludeChangesFromTimeline || !info.CanBackfill {
		t.Fatalf("expected hidden metadata update with backfill enabled: %#v", info)
	}
	if info.Avatar == nil || string(info.Avatar.MXC) != defaultCodexAvatarMXC {
		t.Fatalf("expected thread metadata changes to preserve Codex avatar %q, got %#v", defaultCodexAvatarMXC, info.Avatar)
	}
	portal := &bridgev2.Portal{Portal: &database.Portal{
		PortalKey: networkid.PortalKey{ID: "project", Receiver: "codex"},
		Metadata:  &PortalMetadata{ThreadID: "thread-1", Cwd: "/old/project"},
	}}
	if !info.ExtraUpdates(context.Background(), portal) {
		t.Fatal("expected chat info extra updater to persist changed metadata")
	}
	meta := portalMetadata(portal.Metadata)
	if meta.Kind != portalKindProject || meta.ThreadID != "thread-1" || meta.Cwd != "/new/project" || meta.ReadOnly || meta.ParentThreadID != "" {
		t.Fatalf("unexpected portal metadata: %#v", meta)
	}

	state = codexThreadState("thread/name/updated", "thread-1", "/new/project", []byte(`{"threadId":"thread-1","threadName":"Build bridge"}`))
	info = codexThreadChatInfo("/new/project", "thread-1", state)
	if info.Name == nil || *info.Name != "Build bridge (/new/project)" {
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
	if meta.Kind != portalKindProject || meta.ThreadID != "thread-1" || meta.Cwd != "/new/project" || meta.ReadOnly || meta.ParentThreadID != "" {
		t.Fatalf("unexpected portal metadata: %#v", meta)
	}
}

func TestCodexThreadChatInfoExtraUpdatesSyncRoomState(t *testing.T) {
	ctx := context.Background()
	matrix := &fakeMatrixConnector{api: &fakeMatrixAPI{}}
	_, br := testBridgeWithDB(t, matrix)
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
	portal, err := br.GetExistingPortalByKey(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	state := codexThreadState("thread/settings/updated", "thread-1", "/tmp/project", []byte(`{
		"threadId": "thread-1",
		"threadSettings": {
			"cwd": "/tmp/project",
			"model": "gpt-5",
			"modelName": "GPT-5",
			"modelProvider": "openai",
			"effort": "high",
			"reasoning_mode": "adaptive"
		}
	}`))
	info := codexThreadChatInfo("/tmp/project", "thread-1", state)
	if !info.ExtraUpdates(ctx, portal) {
		t.Fatal("expected chat info extra updater to sync room state")
	}

	threadState := findFakeState(matrix.api.states, codexThreadStateType)
	if threadState == nil || threadState.RoomID != portal.MXID {
		t.Fatalf("Codex thread state was not synced to the portal room: %#v", matrix.api.states)
	}
	if threadState.Content == nil || threadState.Content.Raw["threadId"] != "thread-1" || threadState.Content.Raw["model"] != "gpt-5" {
		t.Fatalf("Codex thread state content was wrong: %#v", threadState.Content)
	}
	modelState := findFakeState(matrix.api.states, beeperAIModelStateType)
	if modelState == nil || modelState.RoomID != portal.MXID {
		t.Fatalf("Beeper AI model state was not synced to the portal room: %#v", matrix.api.states)
	}
	if modelState.Content == nil || modelState.Content.Raw["model"] != "openai/gpt-5" || modelState.Content.Raw["reasoning"] != "high" || modelState.Content.Raw["reasoning_mode"] != "adaptive" {
		t.Fatalf("Beeper AI model state content was wrong: %#v", modelState.Content)
	}
	if findFakeState(matrix.api.states, event.StateMSC4391BotCommand.Type) == nil {
		t.Fatalf("command state was not synced with thread metadata: %#v", matrix.api.states)
	}
}

func TestCodexThreadChatInfoExtraUpdatesClearsMissingAIModelState(t *testing.T) {
	ctx := context.Background()
	matrix := &fakeMatrixConnector{}
	_, br := testBridgeWithDB(t, matrix)
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
	portal, err := br.GetExistingPortalByKey(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	state := codexThreadState("thread/status/changed", "thread-1", "/tmp/project", []byte(`{
		"threadId": "thread-1",
		"cwd": "/tmp/project",
		"status": {"type": "notLoaded"}
	}`))
	info := codexThreadChatInfo("/tmp/project", "thread-1", state)
	if !info.ExtraUpdates(ctx, portal) {
		t.Fatal("expected chat info extra updater to sync room state")
	}

	modelState := findFakeState(matrix.api.states, beeperAIModelStateType)
	if modelState == nil || modelState.RoomID != portal.MXID {
		t.Fatalf("Beeper AI model state was not cleared in the portal room: %#v", matrix.api.states)
	}
	if modelState.Content == nil || len(modelState.Content.Raw) != 0 {
		t.Fatalf("Beeper AI model state clear used wrong content: %#v", modelState.Content)
	}
}

func TestSyncThreadPortalDoesNotUseThreadNameAsAIModelName(t *testing.T) {
	ctx := context.Background()
	matrix := &fakeMatrixConnector{}
	connector, br := testBridgeWithDB(t, matrix)
	user, err := br.GetUserByMXID(ctx, id.UserID("@user:example.com"))
	if err != nil {
		t.Fatal(err)
	}
	login, err := user.NewLogin(ctx, &database.UserLogin{
		ID:            "sh-codex",
		RemoteName:    "Codex",
		RemoteProfile: status.RemoteProfile{Name: "Codex"},
		Metadata:      map[string]any{},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	client := login.Client.(*Client)
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
	portal, err := br.GetExistingPortalByKey(ctx, key)
	if err != nil {
		t.Fatal(err)
	}

	client.syncThreadPortal(ctx, portal, appserver.Thread{
		ID:            "thread-1",
		SessionID:     "thread-1",
		Cwd:           "/tmp/project",
		Name:          "Check storage offload options",
		ModelProvider: "openai",
		Raw: map[string]any{
			"model":         "gpt-5.5",
			"modelProvider": "openai",
			"name":          "Check storage offload options",
		},
	})

	modelState := findFakeState(matrix.api.states, beeperAIModelStateType)
	if modelState == nil || modelState.Content.Raw["model"] != "openai/gpt-5.5" {
		t.Fatalf("syncThreadPortal did not sync AI model state: %#v", matrix.api.states)
	}
	if _, ok := modelState.Content.Raw["name"]; ok {
		t.Fatalf("thread title leaked into AI model state: %#v", modelState.Content.Raw)
	}
	room, ok := connector.threadRoom("thread-1")
	if !ok || room.model != "openai/gpt-5.5" || room.modelName != "" {
		t.Fatalf("thread title leaked into cached model metadata: %#v", room)
	}
}

func TestSyncThreadPortalKeepsExistingAIModelWhenThreadOmitsModel(t *testing.T) {
	ctx := context.Background()
	matrix := &modelStateMatrix{modelEvt: &event.Event{Content: event.Content{
		Raw: map[string]any{"model": "openai/gpt-5.5", "name": "GPT-5.5", "reasoning": "high"},
	}}}
	connector, br := testBridgeWithDB(t, matrix)
	user, err := br.GetUserByMXID(ctx, id.UserID("@user:example.com"))
	if err != nil {
		t.Fatal(err)
	}
	login, err := user.NewLogin(ctx, &database.UserLogin{
		ID:            "sh-codex",
		RemoteName:    "Codex",
		RemoteProfile: status.RemoteProfile{Name: "Codex"},
		Metadata:      map[string]any{},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	client := login.Client.(*Client)
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
	portal, err := br.GetExistingPortalByKey(ctx, key)
	if err != nil {
		t.Fatal(err)
	}

	client.syncThreadPortal(ctx, portal, appserver.Thread{
		ID:            "thread-1",
		SessionID:     "thread-1",
		Cwd:           "/tmp/project",
		ModelProvider: "openai",
		Raw:           map[string]any{"modelProvider": "openai"},
	})

	modelState := findFakeState(matrix.api.states, beeperAIModelStateType)
	if modelState == nil || modelState.Content.Raw["model"] != "openai/gpt-5.5" || modelState.Content.Raw["name"] != "GPT-5.5" || modelState.Content.Raw["reasoning"] != "high" {
		t.Fatalf("syncThreadPortal cleared existing AI model state: %#v", matrix.api.states)
	}
	room, ok := connector.threadRoom("thread-1")
	if !ok || room.model != "openai/gpt-5.5" || room.modelName != "GPT-5.5" || room.reasoningEffort != "high" {
		t.Fatalf("syncThreadPortal did not cache existing AI model state: %#v", room)
	}
}

func TestSyncThreadPortalRepairsAIModelFromSessionFile(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "session.jsonl")
	if err := os.WriteFile(path, []byte(`{"type":"turn_context","payload":{"cwd":"/tmp/project","model":"openai/gpt-5.5","modelName":"GPT-5.5","effort":"high"}}`+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	matrix := &modelStateMatrix{modelEvt: &event.Event{Content: event.Content{Raw: map[string]any{}}}}
	connector, br := testBridgeWithDB(t, matrix)
	user, err := br.GetUserByMXID(ctx, id.UserID("@user:example.com"))
	if err != nil {
		t.Fatal(err)
	}
	login, err := user.NewLogin(ctx, &database.UserLogin{
		ID:            "sh-codex",
		RemoteName:    "Codex",
		RemoteProfile: status.RemoteProfile{Name: "Codex"},
		Metadata:      map[string]any{},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	client := login.Client.(*Client)
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
	portal, err := br.GetExistingPortalByKey(ctx, key)
	if err != nil {
		t.Fatal(err)
	}

	client.syncThreadPortal(ctx, portal, appserver.Thread{
		ID:            "thread-1",
		SessionID:     "thread-1",
		Cwd:           "/tmp/project",
		Path:          path,
		ModelProvider: "openai",
		Raw:           map[string]any{"modelProvider": "openai"},
	})

	modelState := findFakeState(matrix.api.states, beeperAIModelStateType)
	if modelState == nil || modelState.Content.Raw["model"] != "openai/gpt-5.5" || modelState.Content.Raw["name"] != "GPT-5.5" || modelState.Content.Raw["reasoning"] != "high" {
		t.Fatalf("syncThreadPortal did not repair AI model state from session file: %#v", matrix.api.states)
	}
	room, ok := connector.threadRoom("thread-1")
	if !ok || room.model != "openai/gpt-5.5" || room.modelName != "GPT-5.5" || room.reasoningEffort != "high" {
		t.Fatalf("syncThreadPortal did not cache session model state: %#v", room)
	}
}

func TestSyncThreadPortalPreservesCustomRoomNameAndTopic(t *testing.T) {
	ctx := context.Background()
	_, br := testBridgeWithDB(t, &fakeMatrixConnector{})
	user, err := br.GetUserByMXID(ctx, id.UserID("@user:example.com"))
	if err != nil {
		t.Fatal(err)
	}
	login, err := user.NewLogin(ctx, &database.UserLogin{
		ID:            "sh-codex",
		RemoteName:    "Codex",
		RemoteProfile: status.RemoteProfile{Name: "Codex"},
		Metadata:      map[string]any{},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	client := login.Client.(*Client)
	key := projectPortalKey("/tmp/project", login.ID)
	if err = br.DB.Portal.Insert(ctx, &database.Portal{
		PortalKey: key,
		MXID:      "!room:example.com",
		Name:      "Custom room",
		NameSet:   true,
		Topic:     "Pinned operator notes",
		TopicSet:  true,
		RoomType:  database.RoomTypeDM,
		Metadata:  &PortalMetadata{ThreadID: "thread-1", Cwd: "/tmp/project"},
	}); err != nil {
		t.Fatal(err)
	}
	portal, err := br.GetExistingPortalByKey(ctx, key)
	if err != nil {
		t.Fatal(err)
	}

	client.syncThreadPortal(ctx, portal, appserver.Thread{
		ID:        "thread-1",
		SessionID: "thread-1",
		Cwd:       "/tmp/project",
		Preview:   "Codex preview should not replace custom room name",
		Raw: map[string]any{
			"threadId": "thread-1",
			"preview":  "Codex preview should not replace custom room name",
		},
	})

	portal, err = br.GetExistingPortalByKey(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if portal.Name != "Custom room" || !portal.NameSet || portal.Topic != "Pinned operator notes" {
		t.Fatalf("syncThreadPortal overwrote custom room metadata: %#v", portal.Portal)
	}
}

func TestPortalInfoWithThreadStateSyncsAllRoomState(t *testing.T) {
	ctx := context.Background()
	matrix := &fakeMatrixConnector{}
	_, br := testBridgeWithDB(t, matrix)
	key := projectPortalKey("/tmp/project", "sh-codex")
	if err := br.DB.Portal.Insert(ctx, &database.Portal{
		PortalKey: key,
		MXID:      "!room:example.com",
		Name:      "project",
		RoomType:  database.RoomTypeDM,
		Metadata:  &PortalMetadata{},
	}); err != nil {
		t.Fatal(err)
	}
	portal, err := br.GetExistingPortalByKey(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	state := codexThreadState("thread/settings/updated", "thread-1", "/tmp/project", []byte(`{
		"threadId": "thread-1",
		"threadSettings": {
			"cwd": "/tmp/project",
			"model": "gpt-5",
			"modelName": "GPT-5",
			"modelProvider": "openai",
			"effort": "high"
		}
	}`))
	info := portalInfo("project", (&Client{}).codexMembers(), "/tmp/project", "thread-1", state)
	if !info.ExtraUpdates(ctx, portal) {
		t.Fatal("expected portal info extra updater to sync initial room state")
	}
	meta := portalMetadata(portal.Metadata)
	if meta.Kind != portalKindProject || meta.ThreadID != "thread-1" || meta.Cwd != "/tmp/project" || meta.ReadOnly || meta.ParentThreadID != "" {
		t.Fatalf("portal metadata was not persisted: %#v", meta)
	}
	for _, eventType := range []string{codexThreadStateType, beeperAIModelStateType, event.StateMSC4391BotCommand.Type} {
		if findFakeState(matrix.api.states, eventType) == nil {
			t.Fatalf("%s was not synced by portal info: %#v", eventType, matrix.api.states)
		}
	}
}

func TestBridgeInfoIncludesSelfHostedIdentity(t *testing.T) {
	ctx := context.Background()
	matrix := &fakeMatrixConnector{}
	_, br := testBridgeWithDB(t, matrix)
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
	portal, err := br.GetExistingPortalByKey(ctx, key)
	if err != nil {
		t.Fatal(err)
	}

	portal.UpdateBridgeInfo(ctx)

	for _, eventType := range []string{event.StateBridge.Type, event.StateHalfShotBridge.Type} {
		state := findFakeState(matrix.api.states, eventType)
		if state == nil {
			t.Fatalf("%s was not synced: %#v", eventType, matrix.api.states)
		}
		content, ok := state.Content.Parsed.(*event.BridgeEventContent)
		if !ok {
			t.Fatalf("%s used unexpected content: %#v", eventType, state.Content)
		}
		if content.BeeperBridgeName != "sh-codex" || !content.BeeperSelfHosted {
			t.Fatalf("%s did not include self-hosted bridge identity: %#v", eventType, content)
		}
	}
}

func TestBlankProjectMetadataDoesNotClearSessionMetadata(t *testing.T) {
	portal := &bridgev2.Portal{Portal: &database.Portal{
		PortalKey: networkid.PortalKey{ID: "project", Receiver: "codex"},
		Metadata:  &PortalMetadata{ThreadID: "thread-1", Cwd: "/tmp/project"},
	}}
	meta := portalMetadata(portal.Metadata)
	if meta.applyProject("", "") {
		t.Fatal("blank starter metadata should not report a session metadata change")
	}
	if meta.ThreadID != "thread-1" || meta.Cwd != "/tmp/project" {
		t.Fatalf("session metadata was cleared: %#v", meta)
	}
}

func TestPortalMetadataApplyGuards(t *testing.T) {
	var nilMeta *PortalMetadata
	if nilMeta.canApplyProject("/tmp/project", "thread-1") {
		t.Fatal("nil portal metadata should not accept project updates")
	}
	meta := &PortalMetadata{}
	if meta.canApplyProject("", "") {
		t.Fatal("blank project metadata should not be applied")
	}
	if !meta.canApplyProject("/tmp/project", "") {
		t.Fatal("project metadata with cwd should be applied")
	}
	if meta.canApplySubagent("") {
		t.Fatal("blank subagent thread ID should not be applied")
	}
	if !meta.canApplySubagent("thread-2") {
		t.Fatal("subagent metadata with thread ID should be applied")
	}
}

func TestPortalMetadataApplyProjectDefaults(t *testing.T) {
	meta := &PortalMetadata{Kind: portalKindSubagent, ParentThreadID: "thread-1", ReadOnly: true}
	changed := false
	meta.applyProjectDefaults(&changed)
	if !changed {
		t.Fatal("project defaults should report changed metadata")
	}
	if meta.Kind != portalKindProject || meta.ParentThreadID != "" || meta.ReadOnly {
		t.Fatalf("unexpected project defaults: %#v", meta)
	}
}

func TestPortalMetadataApplySubagentFields(t *testing.T) {
	meta := &PortalMetadata{Kind: portalKindProject, ThreadID: "thread-1", Cwd: "/old/project"}
	changed := false
	meta.applySubagentFields("thread-1", "thread-2", "/tmp/project", &changed)
	if !changed {
		t.Fatal("subagent fields should report changed metadata")
	}
	if meta.Kind != portalKindSubagent || meta.ThreadID != "thread-2" || meta.ParentThreadID != "thread-1" || meta.Cwd != "/tmp/project" || !meta.ReadOnly {
		t.Fatalf("unexpected subagent fields: %#v", meta)
	}
}

func TestSubagentMetadataMarksReadOnlyPortal(t *testing.T) {
	portal := &bridgev2.Portal{Portal: &database.Portal{
		PortalKey: subagentPortalKey("thread-2", "codex"),
	}}
	meta := portalMetadata(portal.Metadata)
	if !meta.applySubagent("thread-1", "thread-2", "/tmp/project") {
		t.Fatal("expected subagent metadata to be saved")
	}
	portal.Metadata = meta
	if meta.Kind != portalKindSubagent || meta.ThreadID != "thread-2" || meta.ParentThreadID != "thread-1" || meta.Cwd != "/tmp/project" || !meta.ReadOnly {
		t.Fatalf("unexpected subagent metadata: %#v", meta)
	}
	if !isReadOnlyPortal(portal) {
		t.Fatal("subagent portal should be read-only")
	}
}

func TestSubagentMetadataUpdaterPersistsReadOnlyMetadata(t *testing.T) {
	portal := &bridgev2.Portal{Portal: &database.Portal{
		PortalKey: subagentPortalKey("thread-2", "codex"),
	}}
	if !subagentMetadataUpdater("thread-1", "thread-2", "/tmp/project")(context.Background(), portal) {
		t.Fatal("expected subagent metadata updater to report a change")
	}
	meta := portalMetadata(portal.Metadata)
	if meta.Kind != portalKindSubagent || meta.ThreadID != "thread-2" || meta.ParentThreadID != "thread-1" || meta.Cwd != "/tmp/project" || !meta.ReadOnly {
		t.Fatalf("unexpected subagent metadata: %#v", meta)
	}
	if subagentMetadataUpdater("thread-1", "", "/tmp/project")(context.Background(), portal) {
		t.Fatal("blank subagent thread id should not update metadata")
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

func TestMessageMetadataCopyApprovalClonesContext(t *testing.T) {
	source := &MessageMetadata{Approval: &aistream.ApprovalContext{ID: "approval-1", ThreadID: "thread-1"}}
	meta := &MessageMetadata{}
	meta.CopyFrom(source)
	if meta.Approval == nil || meta.Approval.ID != "approval-1" || meta.Approval.ThreadID != "thread-1" {
		t.Fatalf("approval metadata was not copied: %#v", meta)
	}
	source.Approval.ID = "approval-2"
	if meta.Approval.ID != "approval-1" {
		t.Fatalf("approval metadata should be cloned, got %#v", meta.Approval)
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
	got = threadNoticeText("thread/realtime/closed", []byte(`{"threadId":"thread-1"}`))
	if got != "Codex realtime closed." {
		t.Fatalf("unexpected realtime closed notice: %q", got)
	}
	got = threadNoticeText("thread/status/changed", []byte(`{"threadId":"thread-1","model":"openai/gpt-5.5","status":{"type":"systemError"}}`))
	if got != "Codex entered system error state while using openai/gpt-5.5." {
		t.Fatalf("unexpected system error notice: %q", got)
	}
	got = threadNoticeText("configWarning", []byte(`{"summary":"bad config","details":"line 4"}`))
	if got != "Codex configWarning:\n\nbad config\n\nline 4" {
		t.Fatalf("unexpected config warning notice: %q", got)
	}
}

func TestModelRerouteNoticeDetail(t *testing.T) {
	got := modelRerouteNoticeDetail(map[string]any{"reason": "policy"})
	if got != "Reason: policy" {
		t.Fatalf("unexpected reroute detail: %q", got)
	}
}

func TestSetMapFieldWithMirror(t *testing.T) {
	got := map[string]any{}
	setMapFieldWithMirror(got, "model", "gpt-5", "settings.")
	if got["model"] != "gpt-5" || got["settings.model"] != "gpt-5" || len(got) != 2 {
		t.Fatalf("unexpected mirrored map field: %#v", got)
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
		if got := nestedIDFromMap(rawPayload([]byte(raw)), 0, []string{"turnId", "expectedTurnId"}, []string{"turn"}, []string{"item", "params", "request"}); got != want {
			t.Fatalf("nested turn ID from %s = %q, want %q", raw, got, want)
		}
	}
}

func TestCanStartActiveRunFromNotification(t *testing.T) {
	for _, method := range []string{
		"error",
		"turn/started",
		"turn/completed",
		"item/started",
		"item/agentMessage/delta",
		"hook/started",
		"turn/diff/updated",
		"model/rerouted",
		"thread/goal/updated",
		"thread/tokenUsage/updated",
		"thread/compacted",
		"thread/realtime/closed",
		"serverRequest/resolved",
		"configWarning",
	} {
		if !isActiveRunNotification(method) {
			t.Fatalf("expected %s to be able to start an active run", method)
		}
	}
	for _, method := range []string{
		"account/updated",
		"remoteControl/status/changed",
	} {
		if isActiveRunNotification(method) {
			t.Fatalf("did not expect %s to start an active run", method)
		}
	}
}

func TestMetadataNotificationWithoutTurnDoesNotStartStream(t *testing.T) {
	ctx := context.Background()
	oldPortalEventBuffer := bridgev2.PortalEventBuffer
	bridgev2.PortalEventBuffer = 0
	t.Cleanup(func() { bridgev2.PortalEventBuffer = oldPortalEventBuffer })

	matrix := &fakeMatrixConnector{}
	connector, br := testBridgeWithDB(t, matrix)
	user, err := br.GetUserByMXID(ctx, id.UserID("@user:example.com"))
	if err != nil {
		t.Fatal(err)
	}
	login, err := user.NewLogin(ctx, &database.UserLogin{
		ID:            "sh-codex",
		RemoteName:    "Codex",
		RemoteProfile: status.RemoteProfile{Name: "Codex"},
		Metadata:      map[string]any{},
	}, nil)
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
	connector.rememberThreadRoom("thread-1", login.Client.(*Client), key, "/tmp/project", map[string]any{"modelProvider": "openai", "model": "gpt-5", "reasoningEffort": "high"})

	connector.handleNotification("thread/status/changed", []byte(`{
		"threadId": "thread-1",
		"status": {"type": "active", "activeFlags": ["agent"]}
	}`))

	if run := connector.activeRun("thread-1"); run != nil {
		t.Fatalf("metadata-only notification should not create an active stream: %#v", run)
	}
	if len(matrix.api.messages) != 0 {
		t.Fatalf("non-notice metadata-only notification should not queue a message: %#v", matrix.api.messages)
	}
	if findFakeState(matrix.api.states, codexThreadStateType) == nil {
		t.Fatalf("metadata-only notification should still sync thread state: %#v", matrix.api.states)
	}
}

func TestThreadMetadataNotificationUpdatesCachedRoomState(t *testing.T) {
	connector := &Connector{threadRooms: map[string]threadRoom{}}
	client := &Client{Main: connector, UserLogin: testUserLogin("sh-codex")}
	oldKey := projectPortalKey("/old/project", "sh-codex")
	connector.rememberThreadRoom("thread-1", client, oldKey, "/old/project", map[string]any{"modelProvider": "openai", "model": "gpt-4"})

	connector.handleThreadMetadataNotification("thread/settings/updated", "thread-1", []byte(`{
		"threadId": "thread-1",
		"threadSettings": {
			"cwd": "/new/project",
			"model": "gpt-5",
			"modelProvider": "openai",
			"effort": "high"
		}
	}`))

	room, ok := connector.threadRoom("thread-1")
	if !ok {
		t.Fatal("expected cached thread room")
	}
	if room.portalKey != oldKey {
		t.Fatalf("metadata notification without bridge should not re-id portal: %#v", room)
	}
	if room.cwd != "/new/project" || room.modelProvider != "openai" || room.model != "openai/gpt-5" || room.reasoningEffort != "high" {
		t.Fatalf("metadata notification did not update cached state: %#v", room)
	}
}

func TestThreadArchivedNotificationDetachesPortal(t *testing.T) {
	ctx := context.Background()
	oldPortalEventBuffer := bridgev2.PortalEventBuffer
	bridgev2.PortalEventBuffer = 0
	t.Cleanup(func() { bridgev2.PortalEventBuffer = oldPortalEventBuffer })

	connector, br := testBridgeWithDB(t, &fakeMatrixConnector{})
	user, err := br.GetUserByMXID(ctx, id.UserID("@user:example.com"))
	if err != nil {
		t.Fatal(err)
	}
	login, err := user.NewLogin(ctx, &database.UserLogin{
		ID:            "sh-codex",
		RemoteName:    "Codex",
		RemoteProfile: status.RemoteProfile{Name: "Codex"},
		Metadata:      map[string]any{},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	client := login.Client.(*Client)
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
	run := newActiveRun(client, key, "thread-1", "turn-1")
	connector.setActive("thread-1", run)
	connector.rememberThreadRoom("thread-1", client, key, "/tmp/project", map[string]any{"modelProvider": "openai", "model": "gpt-5"})
	connector.rememberProcess("proc-1", run)
	connector.rememberWarmThread("thread-1")

	connector.handleThreadMetadataNotification("thread/archived", "thread-1", []byte(`{"threadId":"thread-1"}`))

	if connector.activeRun("thread-1") != nil {
		t.Fatal("archived Codex thread should stop active bridging")
	}
	if _, ok := connector.threadRoom("thread-1"); ok {
		t.Fatal("archived Codex thread should remove thread room mapping")
	}
	if run := connector.activeRunForProcess([]byte(`{"processId":"proc-1"}`)); run != nil {
		t.Fatal("archived Codex thread should remove process bridge mapping")
	}
	if _, warm := connector.warmThreads["thread-1"]; warm {
		t.Fatal("archived Codex thread should forget warm thread state")
	}
	requireEventually(t, time.Second, func() bool {
		portal, err := br.GetExistingPortalByKey(ctx, key)
		return err == nil && portal == nil
	})
}

func TestThreadMetadataNotificationUpdatesActiveRunPortalKey(t *testing.T) {
	ctx := context.Background()
	connector, br := testBridgeWithDB(t, &fakeMatrixConnector{})
	login := testUserLogin("sh-codex")
	client := &Client{Main: connector, UserLogin: login}
	oldKey := projectPortalKey("/old/project", login.ID)
	targetKey := projectPortalKey("/new/project", login.ID)
	if err := br.DB.Portal.Insert(ctx, &database.Portal{
		PortalKey: oldKey,
		MXID:      "!room:example.com",
		Name:      "project",
		RoomType:  database.RoomTypeDM,
		Metadata:  &PortalMetadata{ThreadID: "thread-1", Cwd: "/old/project"},
	}); err != nil {
		t.Fatal(err)
	}
	connector.rememberThreadRoom("thread-1", client, oldKey, "/old/project", map[string]any{"modelProvider": "openai", "model": "gpt-4"})
	run := newActiveRun(client, oldKey, "thread-1", "turn-1")
	connector.setActive("thread-1", run)

	connector.handleThreadMetadataNotification("thread/settings/updated", "thread-1", []byte(`{
		"threadId": "thread-1",
		"threadSettings": {
			"cwd": "/new/project",
			"model": "gpt-5",
			"modelProvider": "openai"
		}
	}`))

	room, ok := connector.threadRoom("thread-1")
	if !ok {
		t.Fatal("expected cached thread room")
	}
	if room.portalKey != targetKey || room.cwd != "/new/project" || room.model != "openai/gpt-5" {
		t.Fatalf("thread room was not canonicalized: %#v", room)
	}
	run.mu.Lock()
	activeKey := run.portalKey
	activeLogin := run.client.UserLogin
	activeModel := run.run.Model
	run.writer.Text("after metadata")
	activeEventModel := run.run.Events[len(run.run.Events)-1].Get("model")
	run.mu.Unlock()
	if activeKey != targetKey {
		t.Fatalf("active run kept stale portal key: got %#v want %#v", activeKey, targetKey)
	}
	if activeLogin != login {
		t.Fatalf("active run login changed unexpectedly: %#v", activeLogin)
	}
	if activeModel != "openai/gpt-5" || activeEventModel != "openai/gpt-5" {
		t.Fatalf("active run kept stale model: run=%q event=%#v", activeModel, activeEventModel)
	}
	if !hasCodexRoomStateDelta(run.run.Events, codexThreadStateType, "cwd", "/new/project") ||
		!hasCodexRoomStateDelta(run.run.Events, codexThreadStateType, "model", "gpt-5") {
		t.Fatalf("active run did not sync Codex thread room state: %#v", run.run.Events)
	}
	if portal, err := br.GetExistingPortalByKey(ctx, targetKey); err != nil {
		t.Fatal(err)
	} else if portal == nil || portal.MXID != "!room:example.com" {
		t.Fatalf("canonical portal missing after metadata update: %#v", portal)
	}
}

func TestThreadMetadataNotificationQueuesBridgeV2StateSync(t *testing.T) {
	ctx := context.Background()
	oldPortalEventBuffer := bridgev2.PortalEventBuffer
	bridgev2.PortalEventBuffer = 0
	t.Cleanup(func() { bridgev2.PortalEventBuffer = oldPortalEventBuffer })

	matrix := &fakeMatrixConnector{}
	connector, br := testBridgeWithDB(t, matrix)
	user, err := br.GetUserByMXID(ctx, id.UserID("@user:example.com"))
	if err != nil {
		t.Fatal(err)
	}
	login, err := user.NewLogin(ctx, &database.UserLogin{
		ID:            "sh-codex",
		RemoteName:    "Codex",
		RemoteProfile: status.RemoteProfile{Name: "Codex"},
		Metadata:      map[string]any{},
	}, nil)
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
	connector.rememberThreadRoom("thread-1", login.Client.(*Client), key, "/tmp/project", map[string]any{"modelProvider": "openai", "model": "gpt-4"})

	connector.handleThreadMetadataNotification("thread/settings/updated", "thread-1", []byte(`{
		"threadId": "thread-1",
		"threadSettings": {
			"cwd": "/tmp/project",
			"model": "gpt-5",
			"modelName": "GPT-5",
			"modelProvider": "openai",
			"effort": "high"
		}
	}`))

	threadState := findFakeState(matrix.api.states, codexThreadStateType)
	if threadState == nil || threadState.RoomID != "!room:example.com" || threadState.Content.Raw["threadId"] != "thread-1" {
		t.Fatalf("thread metadata notification did not sync Codex state: %#v", matrix.api.states)
	}
	modelState := findFakeState(matrix.api.states, beeperAIModelStateType)
	if modelState == nil || modelState.Content.Raw["model"] != "openai/gpt-5" || modelState.Content.Raw["name"] != "GPT-5" || modelState.Content.Raw["reasoning"] != "high" {
		t.Fatalf("thread metadata notification did not sync AI model state: %#v", matrix.api.states)
	}
	if findFakeState(matrix.api.states, event.StateMSC4391BotCommand.Type) == nil {
		t.Fatalf("thread metadata notification did not sync command state: %#v", matrix.api.states)
	}
}

func TestThreadMetadataNotificationPreservesCustomRoomNameAndTopic(t *testing.T) {
	ctx := context.Background()
	oldPortalEventBuffer := bridgev2.PortalEventBuffer
	bridgev2.PortalEventBuffer = 0
	t.Cleanup(func() { bridgev2.PortalEventBuffer = oldPortalEventBuffer })

	connector, br := testBridgeWithDB(t, &fakeMatrixConnector{})
	user, err := br.GetUserByMXID(ctx, id.UserID("@user:example.com"))
	if err != nil {
		t.Fatal(err)
	}
	login, err := user.NewLogin(ctx, &database.UserLogin{
		ID:            "sh-codex",
		RemoteName:    "Codex",
		RemoteProfile: status.RemoteProfile{Name: "Codex"},
		Metadata:      map[string]any{},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	key := projectPortalKey("/tmp/project", login.ID)
	if err = br.DB.Portal.Insert(ctx, &database.Portal{
		PortalKey: key,
		MXID:      "!room:example.com",
		Name:      "Custom room",
		NameSet:   true,
		Topic:     "Pinned operator notes",
		TopicSet:  true,
		RoomType:  database.RoomTypeDM,
		Metadata:  &PortalMetadata{ThreadID: "thread-1", Cwd: "/tmp/project"},
	}); err != nil {
		t.Fatal(err)
	}
	connector.rememberThreadRoom("thread-1", login.Client.(*Client), key, "/tmp/project", map[string]any{"modelProvider": "openai", "model": "gpt-4"})

	connector.handleThreadMetadataNotification("thread/name/updated", "thread-1", []byte(`{
		"threadId": "thread-1",
		"threadName": "Codex generated name"
	}`))

	portal, err := br.GetExistingPortalByKey(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if portal.Name != "Custom room" || !portal.NameSet || portal.Topic != "Pinned operator notes" {
		t.Fatalf("thread metadata notification overwrote custom room metadata: %#v", portal.Portal)
	}
}

func TestThreadMetadataNotificationPreservesClearedRoomNameAndTopic(t *testing.T) {
	ctx := context.Background()
	oldPortalEventBuffer := bridgev2.PortalEventBuffer
	bridgev2.PortalEventBuffer = 0
	t.Cleanup(func() { bridgev2.PortalEventBuffer = oldPortalEventBuffer })

	matrix := &fakeMatrixConnector{}
	connector, br := testBridgeWithDB(t, matrix)
	user, err := br.GetUserByMXID(ctx, id.UserID("@user:example.com"))
	if err != nil {
		t.Fatal(err)
	}
	login, err := user.NewLogin(ctx, &database.UserLogin{
		ID:            "sh-codex",
		RemoteName:    "Codex",
		RemoteProfile: status.RemoteProfile{Name: "Codex"},
		Metadata:      map[string]any{},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	key := projectPortalKey("/tmp/project", login.ID)
	if err = br.DB.Portal.Insert(ctx, &database.Portal{
		PortalKey: key,
		MXID:      "!room:example.com",
		Name:      "",
		NameSet:   true,
		Topic:     "",
		TopicSet:  true,
		AvatarSet: true,
		RoomType:  database.RoomTypeDM,
		Metadata:  &PortalMetadata{ThreadID: "thread-1", Cwd: "/tmp/project"},
	}); err != nil {
		t.Fatal(err)
	}
	connector.rememberThreadRoom("thread-1", login.Client.(*Client), key, "/tmp/project", map[string]any{"modelProvider": "openai", "model": "gpt-4"})

	connector.handleThreadMetadataNotification("thread/name/updated", "thread-1", []byte(`{
		"threadId": "thread-1",
		"threadName": "Codex generated name"
	}`))

	portal, err := br.GetExistingPortalByKey(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if portal.Name != "" || !portal.NameSet || portal.Topic != "" || !portal.TopicSet {
		t.Fatalf("thread metadata notification overwrote cleared room metadata: %#v", portal.Portal)
	}
	for _, state := range matrix.api.states {
		if state.Type != event.StateRoomName || state.Content == nil {
			continue
		}
		name := ""
		if content, _ := state.Content.Parsed.(*event.RoomNameEventContent); content != nil {
			name = content.Name
		} else {
			name, _ = state.Content.Raw["name"].(string)
		}
		if name != "" {
			t.Fatalf("chat info sync did not preserve cleared room name: %#v", state.Content.Raw)
		}
	}
}

func TestApplyStoredPortalInfoPreservesClearedTopic(t *testing.T) {
	name := "Generated"
	topic := "Generated topic"
	info := &bridgev2.ChatInfo{Name: &name, Topic: &topic}
	portal := &bridgev2.Portal{Portal: &database.Portal{
		Name:     "",
		NameSet:  true,
		Topic:    "",
		TopicSet: true,
	}}

	applyStoredPortalInfo(info, portal)

	if info.Name == nil || *info.Name != "" {
		t.Fatalf("cleared room name was not preserved: %#v", info.Name)
	}
	if info.Topic == nil || *info.Topic != "" {
		t.Fatalf("cleared room topic was not preserved: %#v", info.Topic)
	}
}

func TestModelVerificationNotificationKeepsCachedAIModelState(t *testing.T) {
	ctx := context.Background()
	oldPortalEventBuffer := bridgev2.PortalEventBuffer
	bridgev2.PortalEventBuffer = 0
	t.Cleanup(func() { bridgev2.PortalEventBuffer = oldPortalEventBuffer })

	matrix := &fakeMatrixConnector{}
	connector, br := testBridgeWithDB(t, matrix)
	user, err := br.GetUserByMXID(ctx, id.UserID("@user:example.com"))
	if err != nil {
		t.Fatal(err)
	}
	login, err := user.NewLogin(ctx, &database.UserLogin{
		ID:            "sh-codex",
		RemoteName:    "Codex",
		RemoteProfile: status.RemoteProfile{Name: "Codex"},
		Metadata:      map[string]any{},
	}, nil)
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
	connector.rememberThreadRoom("thread-1", login.Client.(*Client), key, "/tmp/project", map[string]any{"modelProvider": "openai", "model": "gpt-5", "reasoningEffort": "high"})

	connector.handleThreadMetadataNotification("model/verification", "thread-1", []byte(`{
		"threadId": "thread-1",
		"turnId": "turn-1",
		"verifications": [{"type": "trustedAccessForCyber"}]
	}`))

	threadState := findFakeState(matrix.api.states, codexThreadStateType)
	if threadState == nil || threadState.Content.Raw["lastNotification"] != "model/verification" {
		t.Fatalf("model verification did not sync Codex thread state: %#v", matrix.api.states)
	}
	modelState := findFakeState(matrix.api.states, beeperAIModelStateType)
	if modelState == nil || modelState.Content.Raw["model"] != "openai/gpt-5" || modelState.Content.Raw["reasoning"] != "high" {
		t.Fatalf("model verification did not keep AI model state: %#v", matrix.api.states)
	}
}

func TestThreadNoticeNotificationQueuesBeeperAINotice(t *testing.T) {
	ctx := context.Background()
	oldPortalEventBuffer := bridgev2.PortalEventBuffer
	bridgev2.PortalEventBuffer = 0
	t.Cleanup(func() { bridgev2.PortalEventBuffer = oldPortalEventBuffer })

	matrix := &fakeMatrixConnector{}
	connector, br := testBridgeWithDB(t, matrix)
	user, err := br.GetUserByMXID(ctx, id.UserID("@user:example.com"))
	if err != nil {
		t.Fatal(err)
	}
	login, err := user.NewLogin(ctx, &database.UserLogin{
		ID:            "sh-codex",
		RemoteName:    "Codex",
		RemoteProfile: status.RemoteProfile{Name: "Codex"},
		Metadata:      map[string]any{},
	}, nil)
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
	connector.rememberThreadRoom("thread-1", login.Client.(*Client), key, "/tmp/project", map[string]any{"modelProvider": "openai", "model": "gpt-5", "reasoningEffort": "high"})

	connector.handleThreadNoticeNotification("thread/compacted", "thread-1", []byte(`{"threadId":"thread-1"}`))

	if len(matrix.api.messages) != 1 {
		t.Fatalf("expected one notice message, got %#v", matrix.api.messages)
	}
	msg := matrix.api.messages[0]
	if msg.RoomID != "!room:example.com" || msg.Type != event.EventMessage {
		t.Fatalf("notice sent to wrong Matrix target: %#v", msg)
	}
	content, ok := msg.Content.Parsed.(*event.MessageEventContent)
	if !ok || !strings.Contains(content.Body, "Codex compacted the thread context.") {
		t.Fatalf("notice used unexpected Matrix content: %#v", msg.Content)
	}
	if content.BeeperPerMessageProfile == nil || content.BeeperPerMessageProfile.Displayname != "Codex" {
		t.Fatalf("notice missing Codex per-message profile: %#v", content.BeeperPerMessageProfile)
	}
	ai, ok := msg.Content.Raw[aistream.BeeperAIKey].(aistream.BeeperAI)
	if !ok || ai.Kind != aistream.AIKindFinal || ai.ThreadID != "thread-1" || ai.Model != "openai/gpt-5" {
		t.Fatalf("notice missing final Beeper AI payload: %#v", msg.Content.Raw)
	}
	dbMsg, err := br.DB.Message.GetPartByID(ctx, key.Receiver, msg.Extra.MessageMeta.ID, partID("command"))
	if err != nil {
		t.Fatal(err)
	}
	meta, ok := dbMsg.Metadata.(*MessageMetadata)
	if !ok || meta.Role != "assistant" || meta.ThreadID != "thread-1" || meta.StreamStatus != "notice" {
		t.Fatalf("notice DB metadata was wrong: %#v", dbMsg.Metadata)
	}
}

func TestThreadNoticeNotificationUsesActiveStreamWhenPresent(t *testing.T) {
	ctx := context.Background()
	oldPortalEventBuffer := bridgev2.PortalEventBuffer
	bridgev2.PortalEventBuffer = 0
	t.Cleanup(func() { bridgev2.PortalEventBuffer = oldPortalEventBuffer })

	matrix := &fakeMatrixConnector{}
	connector, br := testBridgeWithDB(t, matrix)
	user, err := br.GetUserByMXID(ctx, id.UserID("@user:example.com"))
	if err != nil {
		t.Fatal(err)
	}
	login, err := user.NewLogin(ctx, &database.UserLogin{
		ID:            "sh-codex",
		RemoteName:    "Codex",
		RemoteProfile: status.RemoteProfile{Name: "Codex"},
		Metadata:      map[string]any{},
	}, nil)
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
	client := login.Client.(*Client)
	connector.rememberThreadRoom("thread-1", client, key, "/tmp/project", map[string]any{"modelProvider": "openai", "model": "gpt-5", "reasoningEffort": "high"})
	run := newActiveRun(client, key, "thread-1", "turn-1")
	connector.setActive("thread-1", run)

	connector.handleNotification("thread/compacted", []byte(`{"threadId":"thread-1"}`))

	if len(matrix.api.messages) != 0 {
		t.Fatalf("active stream notice should not also queue a standalone message: %#v", matrix.api.messages)
	}
	if countTextDelta(run.run.Events, codexCompactionNotice) == 0 {
		t.Fatalf("active stream did not include compaction notice: %#v", run.run.Events)
	}
}

func findFakeState(states []fakeStateEvent, eventType string) *fakeStateEvent {
	for i := range states {
		if states[i].Type.Type == eventType {
			return &states[i]
		}
	}
	return nil
}
