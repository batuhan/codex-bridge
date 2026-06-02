package bridge

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/util/dbutil"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/bridgeconfig"
	"maunium.net/go/mautrix/bridgev2/database"
	matrixconnector "maunium.net/go/mautrix/bridgev2/matrix"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/provisionutil"
	bridgestatus "maunium.net/go/mautrix/bridgev2/status"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"github.com/beeper/codex-bridge/pkg/appserver"
	_ "github.com/mattn/go-sqlite3"
)

func TestTurnTextInput(t *testing.T) {
	got := turnTextInput("hello")
	want := []map[string]any{{
		"type":         "text",
		"text":         "hello",
		"textElements": []any{},
	}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected turn input:\n got: %#v\nwant: %#v", got, want)
	}
}

func TestCleanProjectDirExpandsHome(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	dir := filepath.Join(home, "projects", "bettergit")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	got, err := cleanProjectDir("~/projects/bettergit")
	if err != nil {
		t.Fatal(err)
	}
	if got != dir {
		t.Fatalf("unexpected path: got %q want %q", got, dir)
	}
}

func TestCreateChatWithGhostMapsProjectGhosts(t *testing.T) {
	client := &Client{UserLogin: testUserLogin(defaultLoginID)}
	resp, err := client.CreateChatWithGhost(context.Background(), testGhost(projectUserID("/tmp/project")))
	if err != nil {
		t.Fatal(err)
	}
	if resp.PortalKey != projectPortalKey("/tmp/project", defaultLoginID) {
		t.Fatalf("unexpected portal key: %#v", resp.PortalKey)
	}
}

func TestCreateChatWithGhostMapsNewProjectGhost(t *testing.T) {
	client := &Client{UserLogin: testUserLogin("login-1")}
	resp, err := client.CreateChatWithGhost(context.Background(), testGhost(newProjectUserID))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(resp.PortalKey.ID), "new:") || resp.PortalKey.Receiver != "login-1" {
		t.Fatalf("unexpected portal key: %#v", resp.PortalKey)
	}
	if resp.DMRedirectedTo != codexUserID {
		t.Fatalf("new project DM should redirect to Codex ghost, got %q", resp.DMRedirectedTo)
	}
	if resp.PortalInfo == nil || resp.PortalInfo.Topic == nil || *resp.PortalInfo.Topic != newProjectPrompt {
		t.Fatalf("new project chat should ask for a directory in topic: %#v", resp.PortalInfo)
	}
	second, err := client.CreateChatWithGhost(context.Background(), testGhost(newProjectUserID))
	if err != nil {
		t.Fatal(err)
	}
	if second.PortalKey == resp.PortalKey {
		t.Fatalf("new project chats should use unique starter keys: %#v", resp.PortalKey)
	}
}

func TestCreateChatWithGhostRejectsUnknownGhost(t *testing.T) {
	client := &Client{UserLogin: testUserLogin(defaultLoginID)}
	if _, err := client.CreateChatWithGhost(context.Background(), testGhost("other")); err == nil {
		t.Fatal("expected unknown ghost error")
	}
}

func TestStarterThreadCWDConsumesOnlyStarterRoomBody(t *testing.T) {
	cwd, consumed := starterThreadCWD(&PortalMetadata{}, &bridgev2.Portal{Portal: &database.Portal{
		PortalKey: networkid.PortalKey{ID: "new:starter", Receiver: "sh-codex"},
	}}, " /tmp/project ")
	if cwd != "/tmp/project" || !consumed {
		t.Fatalf("starter room should consume body as cwd, got cwd=%q consumed=%v", cwd, consumed)
	}

	cwd, consumed = starterThreadCWD(&PortalMetadata{Cwd: "/tmp/project"}, &bridgev2.Portal{Portal: &database.Portal{
		PortalKey: projectPortalKey("/tmp/project", "sh-codex"),
	}}, "real prompt")
	if cwd != "/tmp/project" || consumed {
		t.Fatalf("project room should not consume prompt as cwd, got cwd=%q consumed=%v", cwd, consumed)
	}

	cwd, consumed = starterThreadCWD(&PortalMetadata{}, &bridgev2.Portal{Portal: &database.Portal{
		PortalKey: projectPortalKey("/tmp/project", "sh-codex"),
	}}, "real prompt")
	if cwd != "/tmp/project" || consumed {
		t.Fatalf("project portal ID should provide cwd without consuming prompt, got cwd=%q consumed=%v", cwd, consumed)
	}
}

func TestHandleMatrixRoomTopicUpdatesPortal(t *testing.T) {
	portal := &bridgev2.Portal{Portal: &database.Portal{}}
	ok, err := (&Client{}).HandleMatrixRoomTopic(context.Background(), &bridgev2.MatrixRoomTopic{
		MatrixEventBase: bridgev2.MatrixEventBase[*event.TopicEventContent]{
			Portal:  portal,
			Content: &event.TopicEventContent{Topic: "Codex work"},
		},
	})
	if err != nil || !ok {
		t.Fatalf("topic update failed: ok=%v err=%v", ok, err)
	}
	if portal.Topic != "Codex work" || !portal.TopicSet {
		t.Fatalf("portal topic not updated: %#v", portal.Portal)
	}
}

func TestHandleMatrixRoomStateUpdatesAIModelCache(t *testing.T) {
	key := projectPortalKey("/tmp/project", "sh-codex")
	connector := &Connector{}
	connector.Init(&bridgev2.Bridge{})
	login := testUserLogin("sh-codex")
	connector.rememberThreadRoom("thread-1", &Client{Main: connector, UserLogin: login}, key, "/tmp/project", "openai", "gpt-5", "low")
	portal := &bridgev2.Portal{Portal: &database.Portal{PortalKey: key}}

	ok, err := (&Client{Main: connector}).HandleMatrixRoomState(context.Background(), &bridgev2.MatrixRoomState{
		MatrixEventBase: bridgev2.MatrixEventBase[map[string]any]{
			Portal: portal,
			Content: map[string]any{
				"model":     "claude-sonnet-4.5",
				"provider":  "anthropic",
				"reasoning": "high",
			},
		},
		Type: event.Type{Type: beeperAIModelStateType, Class: event.StateEventType},
	})
	if err != nil || !ok {
		t.Fatalf("room state update failed: ok=%v err=%v", ok, err)
	}
	state := connector.modelStateForPortalKey(key)
	if state["model"] != "anthropic/claude-sonnet-4.5" || state["reasoning"] != "high" {
		t.Fatalf("model state cache not updated: %#v", state)
	}

	ok, err = (&Client{Main: connector}).HandleMatrixRoomState(context.Background(), &bridgev2.MatrixRoomState{
		MatrixEventBase: bridgev2.MatrixEventBase[map[string]any]{
			Portal:  portal,
			Content: map[string]any{"reasoning": "low"},
		},
		Type: event.Type{Type: beeperAIModelStateType, Class: event.StateEventType},
	})
	if err != nil || !ok {
		t.Fatalf("effort-only room state update failed: ok=%v err=%v", ok, err)
	}
	state = connector.modelStateForPortalKey(key)
	if _, hasModel := state["model"]; hasModel || state["reasoning"] != "low" {
		t.Fatalf("effort-only model state cache mismatch: %#v", state)
	}

	ok, err = (&Client{Main: connector}).HandleMatrixRoomState(context.Background(), &bridgev2.MatrixRoomState{
		MatrixEventBase: bridgev2.MatrixEventBase[map[string]any]{
			Portal:  portal,
			Content: map[string]any{},
		},
		Type: event.Type{Type: beeperAIModelStateType, Class: event.StateEventType},
	})
	if err != nil || !ok {
		t.Fatalf("clear room state update failed: ok=%v err=%v", ok, err)
	}
	if state = connector.modelStateForPortalKey(key); state != nil {
		t.Fatalf("cleared model state should not stay cached: %#v", state)
	}
}

func TestHandleMatrixRoomStateIgnoresBridgeOwnedCodexThreadMetadata(t *testing.T) {
	key := newProjectPortalKey("sh-codex")
	connector := &Connector{}
	connector.Init(&bridgev2.Bridge{})
	login := testUserLogin("sh-codex")
	portal := &bridgev2.Portal{Portal: &database.Portal{
		PortalKey: key,
		Metadata:  &PortalMetadata{ThreadID: "old-thread", Cwd: "/old/project"},
	}}
	connector.rememberThreadRoom("old-thread", &Client{Main: connector, UserLogin: login}, key, "/old/project", "openai", "gpt-4.1", "low")

	ok, err := (&Client{Main: connector, UserLogin: login}).HandleMatrixRoomState(context.Background(), &bridgev2.MatrixRoomState{
		MatrixEventBase: bridgev2.MatrixEventBase[map[string]any]{
			Portal: portal,
			Content: map[string]any{
				"threadId":      "thread-1",
				"cwd":           "/tmp/project",
				"model":         "gpt-5",
				"modelProvider": "openai",
				"effort":        "high",
			},
		},
		Type: event.Type{Type: codexThreadStateType, Class: event.StateEventType},
	})
	if err != nil || !ok {
		t.Fatalf("Codex thread room state update failed: ok=%v err=%v", ok, err)
	}
	meta := portalMetadata(portal.Metadata)
	if meta.ThreadID != "old-thread" || meta.Cwd != "/old/project" {
		t.Fatalf("bridge-owned Codex state should not rewrite portal metadata: %#v", meta)
	}
	if _, ok := connector.threadRoom("thread-1"); ok {
		t.Fatalf("bridge-owned Codex state should not create a new thread cache entry")
	}
	room, ok := connector.threadRoom("old-thread")
	if !ok || room.portalKey != key || room.cwd != "/old/project" || room.model != "openai/gpt-4.1" || room.reasoningEffort != "low" {
		t.Fatalf("existing thread room cache should be preserved: ok=%v room=%#v", ok, room)
	}
	state := connector.modelStateForPortalKey(key)
	if state["model"] != "openai/gpt-4.1" || state["reasoning"] != "low" {
		t.Fatalf("model cache should be preserved: %#v", state)
	}
}

func TestHandleMatrixRoomStateRejectsModelStateKey(t *testing.T) {
	ok, err := (&Client{}).HandleMatrixRoomState(context.Background(), &bridgev2.MatrixRoomState{
		MatrixEventBase: bridgev2.MatrixEventBase[map[string]any]{
			Portal: &bridgev2.Portal{Portal: &database.Portal{}},
		},
		Type:     event.Type{Type: beeperAIModelStateType, Class: event.StateEventType},
		StateKey: "bad",
	})
	if err == nil || ok {
		t.Fatalf("expected non-empty model state key to be rejected, ok=%v err=%v", ok, err)
	}
}

func TestHandleMatrixRoomNameUpdatesCodexThreadAndPortal(t *testing.T) {
	ctx := context.Background()
	logPath := filepath.Join(t.TempDir(), "fake-appserver.jsonl")
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	app, err := appserver.Start(ctx, exe, map[string]string{
		fakeAppServerEnv:    "1",
		fakeAppServerLogEnv: logPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()
	connector := &Connector{app: app}
	portal := &bridgev2.Portal{Portal: &database.Portal{
		Metadata: &PortalMetadata{ThreadID: "thread-1", Cwd: "/tmp/project"},
	}}
	ok, err := (&Client{Main: connector}).HandleMatrixRoomName(ctx, &bridgev2.MatrixRoomName{
		MatrixEventBase: bridgev2.MatrixEventBase[*event.RoomNameEventContent]{
			Portal:  portal,
			Content: &event.RoomNameEventContent{Name: "Bridge polish"},
		},
	})
	if err != nil || !ok {
		t.Fatalf("room name update failed: ok=%v err=%v", ok, err)
	}
	if portal.Name != "Bridge polish" || !portal.NameSet {
		t.Fatalf("portal name not updated: %#v", portal.Portal)
	}
	requests := readFakeAppServerRequests(t, logPath)
	req, ok := findFakeAppServerRequest(requests, "thread/name/set")
	if !ok {
		t.Fatalf("thread/name/set request missing: %#v", requests)
	}
	if req.Params["threadId"] != "thread-1" || req.Params["name"] != "Bridge polish" {
		t.Fatalf("bad thread/name/set params: %#v", req.Params)
	}
}

func TestGetChatInfoIncludesStoredPortalRoomState(t *testing.T) {
	portal := &bridgev2.Portal{Portal: &database.Portal{
		Name:     "Custom Codex",
		NameSet:  true,
		Topic:    "Codex work",
		TopicSet: true,
		Disappear: database.DisappearingSetting{
			Type:  event.DisappearingTypeAfterRead,
			Timer: time.Hour,
		},
	}}
	info, err := (&Client{}).GetChatInfo(context.Background(), portal)
	if err != nil {
		t.Fatal(err)
	}
	if info.Name == nil || *info.Name != "Custom Codex" {
		t.Fatalf("stored room name was not returned: %#v", info.Name)
	}
	if info.Topic == nil || *info.Topic != "Codex work" {
		t.Fatalf("stored room topic was not returned: %#v", info.Topic)
	}
	if info.Disappear != nil {
		t.Fatalf("stored disappearing timer should not be returned: %#v", info.Disappear)
	}
	if info.Avatar == nil || string(info.Avatar.MXC) != defaultCodexAvatarMXC {
		t.Fatalf("stored chat info should include Codex avatar %q, got %#v", defaultCodexAvatarMXC, info.Avatar)
	}
}

func TestGetChatInfoForNewProjectAsksForDirectory(t *testing.T) {
	info, err := (&Client{}).GetChatInfo(context.Background(), &bridgev2.Portal{Portal: &database.Portal{
		PortalKey: newProjectPortalKey("sh-codex"),
		Metadata:  &PortalMetadata{},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if info.Topic == nil || *info.Topic != newProjectPrompt {
		t.Fatalf("new project chat should ask for a directory: %#v", info.Topic)
	}
}

func TestGetChatInfoClearsMissingThreadMetadataAndState(t *testing.T) {
	ctx := context.Background()
	matrix := &fakeMatrixConnector{}
	connector, br := testBridgeWithDB(t, matrix)
	logPath := filepath.Join(t.TempDir(), "fake-appserver.jsonl")
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	app, err := appserver.Start(ctx, exe, map[string]string{
		fakeAppServerEnv:    "1",
		fakeAppServerLogEnv: logPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()
	connector.appMu.Lock()
	connector.app = app
	connector.appMu.Unlock()

	user, err := br.GetUserByMXID(ctx, "@alice:example.com")
	if err != nil {
		t.Fatal(err)
	}
	login, err := connector.ensureLoginID(ctx, user, "sh-codex", "alice@example.com")
	if err != nil {
		t.Fatal(err)
	}
	client := &Client{Main: connector, UserLogin: login, loggedIn: true}
	login.Client = client
	key := projectPortalKey("/tmp/project", login.ID)
	if err = br.DB.Portal.Insert(ctx, &database.Portal{
		PortalKey: key,
		MXID:      "!room:example.com",
		Name:      "project",
		RoomType:  database.RoomTypeDM,
		Metadata:  &PortalMetadata{ThreadID: "missing-thread", Cwd: "/tmp/project"},
	}); err != nil {
		t.Fatal(err)
	}
	portal, err := br.GetExistingPortalByKey(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	connector.rememberThreadRoom("missing-thread", client, key, "/tmp/project", "openai", "gpt-5")

	info, err := client.GetChatInfo(ctx, portal)
	if err != nil {
		t.Fatal(err)
	}
	if info.CanBackfill {
		t.Fatalf("missing thread chat info should not keep backfill enabled: %#v", info)
	}
	meta := portalMetadata(portal.Metadata)
	if meta.ThreadID != "" || meta.Cwd != "/tmp/project" {
		t.Fatalf("missing thread metadata was not cleared: %#v", meta)
	}
	if _, ok := connector.threadRoom("missing-thread"); ok {
		t.Fatal("missing thread should be removed from thread room cache")
	}
	if !info.ExtraUpdates(ctx, portal) {
		t.Fatal("expected missing-thread chat info to clear custom room state")
	}
	threadState := findFakeState(matrix.api.states, codexThreadStateType)
	if threadState == nil || threadState.Content == nil || len(threadState.Content.Raw) != 0 {
		t.Fatalf("missing thread should clear Codex thread room state: %#v", matrix.api.states)
	}
	modelState := findFakeState(matrix.api.states, beeperAIModelStateType)
	if modelState == nil || modelState.Content == nil || len(modelState.Content.Raw) != 0 {
		t.Fatalf("missing thread should clear Beeper AI model room state: %#v", matrix.api.states)
	}
}

func TestCodexGhostInfoDoesNotUseNetworkBotFlag(t *testing.T) {
	info, err := (&Client{}).GetUserInfo(context.Background(), testGhost(codexUserID))
	if err != nil {
		t.Fatal(err)
	}
	if info == nil || info.IsBot == nil || *info.IsBot {
		t.Fatalf("Codex assistant ghost should not be marked as a network bot: %#v", info)
	}
	if info.Avatar == nil || string(info.Avatar.MXC) != defaultCodexAvatarMXC {
		t.Fatalf("Codex assistant ghost should use shared AI avatar %q, got %#v", defaultCodexAvatarMXC, info.Avatar)
	}
}

func TestGetUserInfoOnlyAcceptsKnownCodexGhosts(t *testing.T) {
	client := &Client{}
	if info, err := client.GetUserInfo(context.Background(), testGhost("login:sh-codex")); err != nil {
		t.Fatal(err)
	} else if info.Name == nil || *info.Name != "You" || info.IsBot == nil || *info.IsBot {
		t.Fatalf("login ghost should represent the Matrix user, got %#v", info)
	}
	if _, err := client.GetUserInfo(context.Background(), testGhost("random")); err == nil {
		t.Fatal("unknown ghosts should not be silently mapped to Codex")
	}
}

func TestEnsureLoginRefreshesLoginGhostAsMatrixUser(t *testing.T) {
	ctx := context.Background()
	connector, br := testBridgeWithDB(t, &fakeMatrixConnector{})
	if err := br.DB.Ghost.Insert(ctx, &database.Ghost{
		ID:        "login:sh-codex",
		Name:      "Codex",
		NameSet:   true,
		IsBot:     true,
		AvatarMXC: id.ContentURIString(defaultCodexAvatarMXC),
		AvatarSet: true,
	}); err != nil {
		t.Fatal(err)
	}
	user, err := br.GetUserByMXID(ctx, "@alice:example.com")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = connector.ensureLoginID(ctx, user, "sh-codex", "alice@example.com"); err != nil {
		t.Fatal(err)
	}
	ghost, err := br.GetExistingGhostByID(ctx, "login:sh-codex")
	if err != nil {
		t.Fatal(err)
	}
	if ghost == nil || ghost.Name != "You" || ghost.IsBot || ghost.AvatarMXC != "" {
		t.Fatalf("login ghost should represent the Matrix user, got %#v", ghost.Ghost)
	}
}

func TestRoomModelReadsBeeperAIModelState(t *testing.T) {
	matrix := &fakeArbitraryRoomState{evt: &event.Event{Content: event.Content{
		Raw: map[string]any{"model": "openai/gpt-5"},
	}}}
	client := &Client{Main: &Connector{Bridge: &bridgev2.Bridge{Matrix: matrix}}}
	portal := &bridgev2.Portal{Portal: &database.Portal{MXID: "!room:example.com"}}
	if got := client.roomModel(context.Background(), portal); got != "openai/gpt-5" {
		t.Fatalf("unexpected room model: %q", got)
	}
	if matrix.roomID != portal.MXID || matrix.stateType.Type != beeperAIModelStateType || matrix.stateKey != "" {
		t.Fatalf("room model read wrong state event: %#v", matrix)
	}
}

func TestRoomModelUsesCachedThreadStateBeforeRoomStateRoundtrip(t *testing.T) {
	connector := &Connector{threadRooms: map[string]threadRoom{}}
	client := &Client{Main: connector, UserLogin: testUserLogin("sh-codex")}
	key := projectPortalKey("/tmp/project", "sh-codex")
	connector.rememberThreadRoom("thread-1", client, key, "/tmp/project", "openai", "gpt-5", "high")

	portal := &bridgev2.Portal{Portal: &database.Portal{PortalKey: key}}
	if got := client.roomModel(context.Background(), portal); got != "openai/gpt-5" {
		t.Fatalf("unexpected cached room model: %q", got)
	}
	state := client.roomAIModelStateForPortalKey(context.Background(), key)
	if state["model"] != "openai/gpt-5" || state["modelProvider"] != "openai" || state["effort"] != "high" || state["reasoning"] != "high" {
		t.Fatalf("unexpected cached model state: %#v", state)
	}
}

func TestRoomModelStateOverridesCachedThreadState(t *testing.T) {
	matrix := &fakeArbitraryRoomState{evt: &event.Event{Content: event.Content{
		Raw: map[string]any{"model": "openai/gpt-5.1", "reasoning": "high"},
	}}}
	connector := &Connector{Bridge: &bridgev2.Bridge{Matrix: matrix}, threadRooms: map[string]threadRoom{}}
	client := &Client{Main: connector, UserLogin: testUserLogin("sh-codex")}
	key := projectPortalKey("/tmp/project", "sh-codex")
	connector.rememberThreadRoom("thread-1", client, key, "/tmp/project", "openai", "gpt-5")

	portal := &bridgev2.Portal{Portal: &database.Portal{PortalKey: key, MXID: "!room:example.com"}}
	state := client.roomAIModelState(context.Background(), portal)
	if state["model"] != "openai/gpt-5.1" || state["reasoning"] != "high" || state["modelProvider"] != "openai" {
		t.Fatalf("unexpected merged model state: %#v", state)
	}
}

func TestRoomModelReadsParsedBeeperAIModelState(t *testing.T) {
	client := &Client{Main: &Connector{Bridge: &bridgev2.Bridge{Matrix: &fakeArbitraryRoomState{
		evt: &event.Event{Content: event.Content{Parsed: &aiModelStateEventContent{
			"model":        "anthropic/claude-sonnet-4.5",
			"reasoning":    "high",
			"capabilities": map[string]any{"tools": true},
		}}},
	}}}}
	portal := &bridgev2.Portal{Portal: &database.Portal{MXID: "!room:example.com"}}
	if got := client.roomModel(context.Background(), portal); got != "anthropic/claude-sonnet-4.5" {
		t.Fatalf("unexpected parsed room model: %q", got)
	}
	state := client.roomAIModelState(context.Background(), portal)
	if state["reasoning"] != "high" || state["capabilities"] == nil {
		t.Fatalf("parsed room model state lost fields: %#v", state)
	}
}

func TestRoomModelIgnoresMissingState(t *testing.T) {
	client := &Client{Main: &Connector{Bridge: &bridgev2.Bridge{Matrix: &fakeArbitraryRoomState{err: mautrix.MNotFound}}}}
	portal := &bridgev2.Portal{Portal: &database.Portal{MXID: "!room:example.com"}}
	if got := client.roomModel(context.Background(), portal); got != "" {
		t.Fatalf("missing model state should be ignored, got %q", got)
	}
}

func TestApplyRoomTurnSettingsMapsModelAndReasoning(t *testing.T) {
	params := map[string]any{"threadId": "thread-1"}
	applyRoomTurnSettings(params, map[string]any{
		"model":     "openai/gpt-5",
		"reasoning": "high",
	})
	if params["model"] != "gpt-5" || params["effort"] != "high" {
		t.Fatalf("room turn settings not applied: %#v", params)
	}

	applyRoomTurnSettings(params, map[string]any{
		"reasoningEffort": "medium",
	})
	if params["model"] != "gpt-5" || params["effort"] != "medium" {
		t.Fatalf("room turn settings did not preserve model and update effort: %#v", params)
	}
}

func TestCodexRequestModelStripsOpenAIProviderPrefix(t *testing.T) {
	if got := codexRequestModel("openai/gpt-5.5"); got != "gpt-5.5" {
		t.Fatalf("unexpected Codex model: %q", got)
	}
	if got := codexRequestModel("anthropic/claude-sonnet-4.5"); got != "anthropic/claude-sonnet-4.5" {
		t.Fatalf("non-OpenAI model should be preserved: %q", got)
	}
}

func TestContactsIncludeCodexAndNewProject(t *testing.T) {
	client := &Client{UserLogin: testUserLogin("sh-codex")}
	contacts := client.contactsForThreads(context.Background(), nil)
	got := make([]networkid.UserID, 0, len(contacts))
	for _, contact := range contacts {
		got = append(got, contact.UserID)
	}
	want := []networkid.UserID{codexUserID, newProjectUserID}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected contacts:\n got: %#v\nwant: %#v", got, want)
	}
	for _, contact := range contacts {
		if contact.Chat != nil {
			t.Fatalf("base contact %s should not allocate random chat portals: %#v", contact.UserID, contact.Chat)
		}
	}
	if contacts[0].UserInfo == nil || contacts[0].UserInfo.IsBot == nil || *contacts[0].UserInfo.IsBot {
		t.Fatalf("Codex contact should not be marked as a network bot: %#v", contacts[0].UserInfo)
	}
	if contacts[1].UserInfo == nil || contacts[1].UserInfo.IsBot == nil || *contacts[1].UserInfo.IsBot {
		t.Fatalf("New Project contact should not be marked as a network bot: %#v", contacts[1].UserInfo)
	}
}

func TestContactsUseConcreteGhostsWhenBridgeIsAvailable(t *testing.T) {
	connector, br := testBridgeWithDB(t, &fakeMatrixConnector{})
	login := &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "sh-codex"}, Bridge: br}
	client := &Client{Main: connector, UserLogin: login}
	login.Client = client

	contacts := client.contactsForThreads(context.Background(), []appserver.Thread{
		{ID: "thread-1", Cwd: "/tmp/project", UpdatedAt: 20},
	})
	if len(contacts) != 3 {
		t.Fatalf("unexpected contacts: %#v", contacts)
	}
	for _, contact := range contacts {
		if contact.Ghost == nil {
			t.Fatalf("contact %s should expose a concrete ghost", contact.UserID)
		}
		if contact.Ghost.ID != contact.UserID {
			t.Fatalf("contact ghost mismatch: got %s want %s", contact.Ghost.ID, contact.UserID)
		}
		if contact.Ghost.Name != "" || contact.Ghost.AvatarMXC != "" {
			t.Fatalf("contact list should not eagerly sync ghost profile info: %#v", contact.Ghost.Ghost)
		}
	}
	if contacts[0].Ghost.ID != codexUserID || contacts[1].Ghost.ID != newProjectUserID {
		t.Fatalf("unexpected base contact ghosts: %#v %#v", contacts[0].Ghost.ID, contacts[1].Ghost.ID)
	}
	if contacts[2].Ghost.ID != projectUserID("/tmp/project") {
		t.Fatalf("unexpected project contact ghost: %#v", contacts[2].Ghost.ID)
	}

	listed, err := provisionutil.GetContactList(context.Background(), login)
	if err != nil {
		t.Fatal(err)
	}
	if len(listed.Contacts) < 2 || listed.Contacts[0].MXID == "" || listed.Contacts[1].MXID == "" {
		t.Fatalf("provisioned base contacts should include ghost MXIDs: %#v", listed.Contacts)
	}
	searched, err := provisionutil.SearchUsers(context.Background(), login, "new project")
	if err != nil {
		t.Fatal(err)
	}
	if len(searched.Results) != 1 || searched.Results[0].ID != newProjectUserID || searched.Results[0].MXID == "" {
		t.Fatalf("provisioned search should find New Project ghost: %#v", searched.Results)
	}
	projectDir := t.TempDir()
	resolved, err := provisionutil.ResolveIdentifier(context.Background(), login, projectDir, false)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.ID != projectUserID(projectDir) || resolved.MXID == "" || resolved.AvatarURL == "" {
		t.Fatalf("provisioned resolve should expose project ghost: %#v", resolved)
	}
}

func TestSearchUsersFindsBaseContactsWithoutAllocatingChats(t *testing.T) {
	client := &Client{UserLogin: testUserLogin("sh-codex")}
	for _, query := range []string{"codex", "new project"} {
		results, err := client.SearchUsers(context.Background(), query)
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 1 {
			t.Fatalf("expected one result for %q, got %#v", query, results)
		}
		if results[0].Chat != nil {
			t.Fatalf("base search result %q should not allocate random chat portals: %#v", query, results[0].Chat)
		}
	}
}

func TestResolveIdentifierDecodesProjectIDs(t *testing.T) {
	dir := t.TempDir()
	client := &Client{UserLogin: testUserLogin("sh-codex")}
	resp, err := client.ResolveIdentifier(context.Background(), string(projectPortalKey(dir, "sh-codex").ID), true)
	if err != nil {
		t.Fatal(err)
	}
	if resp.UserID != projectUserID(dir) || resp.Chat == nil || resp.Chat.PortalKey != projectPortalKey(dir, "sh-codex") {
		t.Fatalf("unexpected project identifier response: %#v", resp)
	}
}

func TestResolveIdentifierCodexUsesAssistantGhostProfile(t *testing.T) {
	client := &Client{UserLogin: testUserLogin("sh-codex")}
	resp, err := client.ResolveIdentifier(context.Background(), "codex", false)
	if err != nil {
		t.Fatal(err)
	}
	if resp.UserID != codexUserID || resp.UserInfo == nil || resp.UserInfo.IsBot == nil || *resp.UserInfo.IsBot {
		t.Fatalf("Codex identifier should resolve to assistant ghost profile: %#v", resp)
	}
}

func TestProvisioningCreateDMCreatesCodexRooms(t *testing.T) {
	ctx := context.Background()
	matrix := &fakeMatrixConnector{}
	connector, br := testBridgeWithDB(t, matrix)
	user, err := br.GetUserByMXID(ctx, "@alice:example.com")
	if err != nil {
		t.Fatal(err)
	}
	login, err := connector.ensureLoginID(ctx, user, "sh-codex", "alice@example.com")
	if err != nil {
		t.Fatal(err)
	}
	client := &Client{Main: connector, UserLogin: login, loggedIn: true}
	login.Client = client

	for _, identifier := range []string{"codex", "new project"} {
		stateCount := len(matrix.intent().states)
		resp, err := provisionutil.ResolveIdentifier(ctx, login, identifier, true)
		if err != nil {
			t.Fatalf("create DM for %q failed: %v", identifier, err)
		}
		if resp == nil || resp.DMRoomID == "" || !resp.JustCreated || resp.Portal == nil {
			t.Fatalf("create DM for %q did not materialize a room: %#v", identifier, resp)
		}
		if resp.Portal.PortalKey.Receiver != login.ID || resp.Portal.RoomType != database.RoomTypeDM {
			t.Fatalf("create DM for %q used wrong portal: %#v", identifier, resp.Portal.Portal)
		}
		meta := portalMetadata(resp.Portal.Metadata)
		if meta.ThreadID != "" || meta.Cwd != "" {
			t.Fatalf("new project room %q should wait for directory selection, got metadata %#v", identifier, meta)
		}
		commandState := findFakeState(matrix.intent().states[stateCount:], event.StateMSC4391BotCommand.Type)
		if commandState == nil || commandState.RoomID != resp.Portal.MXID {
			t.Fatalf("create DM for %q did not sync command state: %#v", identifier, matrix.intent().states[stateCount:])
		}
	}
}

func TestProvisioningCreateDMMaterializesProjectRoom(t *testing.T) {
	ctx := context.Background()
	connector, br := testBridgeWithDB(t, &fakeMatrixConnector{})
	user, err := br.GetUserByMXID(ctx, "@alice:example.com")
	if err != nil {
		t.Fatal(err)
	}
	login, err := connector.ensureLoginID(ctx, user, "sh-codex", "alice@example.com")
	if err != nil {
		t.Fatal(err)
	}
	client := &Client{Main: connector, UserLogin: login, loggedIn: true}
	login.Client = client

	cwd := t.TempDir()
	resp, err := provisionutil.ResolveIdentifier(ctx, login, cwd, true)
	if err != nil {
		t.Fatal(err)
	}
	if resp == nil || resp.DMRoomID == "" || !resp.JustCreated || resp.Portal == nil {
		t.Fatalf("project create DM did not materialize a room: %#v", resp)
	}
	if resp.ID != projectUserID(cwd) || resp.Portal.PortalKey != projectPortalKey(cwd, login.ID) {
		t.Fatalf("project create DM used wrong identity: %#v portal=%#v", resp, resp.Portal.Portal)
	}
	meta := portalMetadata(resp.Portal.Metadata)
	if meta.Cwd != cwd {
		t.Fatalf("project create DM should persist cwd metadata, got %#v", meta)
	}
}

func TestIsThreadNotFoundErrorOnlyMatchesCodexRPCError(t *testing.T) {
	if !isThreadNotFoundError(&appserver.RPCError{Method: "turn/start", Msg: "thread not found: thread-1"}) {
		t.Fatal("expected Codex thread-not-found RPC error to match")
	}
	if !isThreadNotFoundError(&appserver.RPCError{Method: "thread/read", Msg: "thread/read: thread not loaded: thread-1"}) {
		t.Fatal("expected Codex thread-not-loaded RPC error to match")
	}
	if isThreadNotFoundError(&appserver.RPCError{Method: "turn/start", Msg: "different failure"}) {
		t.Fatal("unrelated RPC error should not match")
	}
	if isThreadNotFoundError(os.ErrNotExist) {
		t.Fatal("non-Codex errors should not match")
	}
}

func TestThreadTurnsListUnavailableOnlyMatchesProtocolFallbacks(t *testing.T) {
	if !isThreadTurnsListUnavailable(&appserver.RPCError{Method: "thread/turns/list", Code: -32601, Msg: "method not found"}) {
		t.Fatal("method-not-found should use thread/read fallback")
	}
	if !isThreadTurnsListUnavailable(&appserver.RPCError{Method: "thread/turns/list", Msg: "experimental API disabled"}) {
		t.Fatal("experimental API unavailability should use thread/read fallback")
	}
	if isThreadTurnsListUnavailable(&appserver.RPCError{Method: "thread/turns/list", Code: -32001, Msg: "Server overloaded; retry later."}) {
		t.Fatal("retryable server errors must not be hidden by fallback")
	}
	if isThreadTurnsListUnavailable(os.ErrNotExist) {
		t.Fatal("non-Codex errors should not match")
	}
}

func TestContactsIncludeRecentDirectoriesWithLatestThread(t *testing.T) {
	client := &Client{UserLogin: testUserLogin("sh-codex")}
	contacts := client.contactsForThreads(context.Background(), []appserver.Thread{
		{ID: "old-thread", Cwd: "/tmp/project", UpdatedAt: 10},
		{ID: "new-thread", Cwd: "/tmp/project", UpdatedAt: 20},
	})
	if len(contacts) != 3 {
		t.Fatalf("unexpected contacts: %#v", contacts)
	}
	if contacts[0].Chat != nil || contacts[1].Chat != nil {
		t.Fatalf("base contacts should not include random chats: %#v", contacts[:2])
	}
	project := contacts[2]
	if project.UserID != projectUserID("/tmp/project") {
		t.Fatalf("unexpected project contact: %#v", project)
	}
	if !identifiersContain(project.UserInfo, "/tmp/project") || !identifiersContain(project.UserInfo, "new-thread") {
		t.Fatalf("project contact identifiers should include cwd and latest thread id: %#v", project.UserInfo)
	}
	if project.Chat != nil {
		t.Fatalf("project contact should not allocate a chat before opening it: %#v", project.Chat)
	}
	chat := client.chatForProject(context.Background(), "/tmp/project", "project", "new-thread", nil)
	if chat == nil || chat.PortalKey != projectPortalKey("/tmp/project", "sh-codex") {
		t.Fatalf("project chat has unexpected key: %#v", chat)
	}
	if chat.DMRedirectedTo != codexUserID {
		t.Fatalf("project chat DM should redirect to Codex ghost, got %q", chat.DMRedirectedTo)
	}
	if !chat.PortalInfo.CanBackfill {
		t.Fatalf("project chat should advertise backfill: %#v", chat.PortalInfo)
	}
	portal := &bridgev2.Portal{Portal: &database.Portal{PortalKey: chat.PortalKey}}
	if !chat.PortalInfo.ExtraUpdates(context.Background(), portal) {
		t.Fatal("expected portal metadata updater to report a change")
	}
	meta := portalMetadata(portal.Metadata)
	if meta.Cwd != "/tmp/project" || meta.ThreadID != "new-thread" {
		t.Fatalf("unexpected portal metadata: %#v", meta)
	}
}

func TestRecentDirectoryContactsAreSearchable(t *testing.T) {
	client := &Client{UserLogin: testUserLogin("sh-codex")}
	contacts := client.contactsForThreads(context.Background(), []appserver.Thread{
		{ID: "thread-1", Cwd: "/tmp/codex-bridge", UpdatedAt: 20},
	})
	if len(contacts) != 3 {
		t.Fatalf("unexpected contacts: %#v", contacts)
	}
	project := contacts[2]
	for _, query := range []string{"codex-bridge", "/tmp/codex-bridge", "thread-1"} {
		name := ""
		if project.UserInfo != nil && project.UserInfo.Name != nil {
			name = strings.ToLower(*project.UserInfo.Name)
		}
		query = strings.ToLower(query)
		if !strings.Contains(name, query) && !strings.Contains(strings.ToLower(string(project.UserID)), query) && !identifiersContain(project.UserInfo, query) {
			t.Fatalf("project contact should match query %q: %#v", query, project.UserInfo)
		}
	}
}

func TestSyncContactGhostsCreatesBaseAndRecentDirectoryGhosts(t *testing.T) {
	ctx := context.Background()
	connector, br := testBridgeWithDB(t, &fakeMatrixConnector{})
	connector.syncContactGhostsForThreads(ctx, []appserver.Thread{
		{ID: "old-thread", Cwd: "/tmp/project", Preview: "old", UpdatedAt: 10},
		{ID: "new-thread", Cwd: "/tmp/project", Name: "Build bridge", Preview: strings.Repeat("preview ", 100), UpdatedAt: 20},
	})

	for _, userID := range []networkid.UserID{codexUserID, newProjectUserID, projectUserID("/tmp/project")} {
		ghost, err := br.GetExistingGhostByID(ctx, userID)
		if err != nil {
			t.Fatal(err)
		}
		if ghost == nil || ghost.Name == "" || ghost.AvatarMXC != id.ContentURIString(defaultCodexAvatarMXC) {
			t.Fatalf("contact ghost %s was not synced: %#v", userID, ghost)
		}
		if ghost.IsBot {
			t.Fatalf("Codex contact ghost %s should not be marked as a network bot", userID)
		}
	}
	project, err := br.GetExistingGhostByID(ctx, projectUserID("/tmp/project"))
	if err != nil {
		t.Fatal(err)
	}
	if !identifiersContain(&bridgev2.UserInfo{Identifiers: project.Identifiers}, "new-thread") {
		t.Fatalf("project contact did not use latest thread identifiers: %#v", project.Identifiers)
	}
	if !identifiersContain(&bridgev2.UserInfo{Identifiers: project.Identifiers}, "build bridge") {
		t.Fatalf("project contact did not include thread name identifiers: %#v", project.Identifiers)
	}
	if identifiersContain(&bridgev2.UserInfo{Identifiers: project.Identifiers}, "preview preview") {
		t.Fatalf("project contact should not include preview identifiers: %#v", project.Identifiers)
	}
	if project.Name != "project" {
		t.Fatalf("project contact should use directory name, got %q", project.Name)
	}
	portals, err := br.GetAllPortals(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(portals) != 0 {
		t.Fatalf("contact ghost sync must not create portals: %#v", portals)
	}
}

func TestCodexMembersUseGlobalGhost(t *testing.T) {
	members := (&Client{UserLogin: testUserLogin("sh-codex")}).codexMembers()
	if members == nil || !members.IsFull || members.OtherUserID != codexUserID {
		t.Fatalf("unexpected Codex chat members: %#v", members)
	}
	if !members.ExcludeChangesFromTimeline {
		t.Fatal("expected synthetic Codex member changes to be excluded from timeline")
	}
	if member, ok := members.MemberMap[networkid.UserID("")]; !ok || !member.IsFromMe {
		t.Fatalf("expected Matrix user self member, got %#v", members.MemberMap)
	} else if member.Sender != "" || member.SenderLogin != "" {
		t.Fatalf("self member should let bridgev2 resolve the login sender, got %#v", member.EventSender)
	} else if member.MemberEventExtra["com.beeper.exclude_from_timeline"] != true {
		t.Fatalf("expected self member event to be hidden, got %#v", member.MemberEventExtra)
	}
	if member, ok := members.MemberMap[codexUserID]; !ok || member.Sender != codexUserID {
		t.Fatalf("expected Codex ghost member, got %#v", members.MemberMap)
	} else if member.UserInfo == nil || member.UserInfo.IsBot == nil || *member.UserInfo.IsBot {
		t.Fatalf("expected Codex ghost without network bot flag, got %#v", member.UserInfo)
	} else if member.UserInfo.Avatar == nil || string(member.UserInfo.Avatar.MXC) != defaultCodexAvatarMXC {
		t.Fatalf("expected Codex ghost avatar %q, got %#v", defaultCodexAvatarMXC, member.UserInfo)
	} else if member.MemberEventExtra["com.beeper.exclude_from_timeline"] != true {
		t.Fatalf("expected Codex ghost member event to be hidden, got %#v", member.MemberEventExtra)
	}
	if members.PowerLevels == nil {
		t.Fatal("expected Codex chat power level overrides")
	}
	for _, evtType := range []event.Type{
		event.StateRoomName,
		event.StateTopic,
	} {
		if level, ok := members.PowerLevels.Events[evtType]; !ok || level != 0 {
			t.Fatalf("expected %s power level 0, got %d (present=%v)", evtType.Type, level, ok)
		}
	}
}

func TestRecentDirectoryContactUsesCanonicalProjectRoom(t *testing.T) {
	connector := &Connector{threadRooms: map[string]threadRoom{}}
	client := &Client{Main: connector, UserLogin: testUserLogin("sh-codex")}
	newProjectKey := projectPortalKey("", "sh-codex")
	connector.rememberThreadRoom("thread-1", client, newProjectKey, "/tmp/project")

	contacts := client.contactsForThreads(context.Background(), []appserver.Thread{
		{ID: "thread-1", Cwd: "/tmp/project", UpdatedAt: 20},
	})
	if len(contacts) != 3 {
		t.Fatalf("unexpected contacts: %#v", contacts)
	}
	project := contacts[2]
	if project.Chat != nil {
		t.Fatalf("project contact should not expose a starter chat: %#v", project.Chat)
	}
	room, ok := connector.threadRoom("thread-1")
	if !ok {
		t.Fatal("expected project contact to cache thread room")
	}
	if room.portalKey != projectPortalKey("/tmp/project", "sh-codex") {
		t.Fatalf("project contact cached non-canonical room: %#v", room)
	}
	chat := client.chatForProject(context.Background(), "/tmp/project", "project", "thread-1", nil)
	portal := &bridgev2.Portal{Portal: &database.Portal{PortalKey: chat.PortalKey}}
	if !chat.PortalInfo.ExtraUpdates(context.Background(), portal) {
		t.Fatal("expected portal metadata updater to report a change")
	}
	meta := portalMetadata(portal.Metadata)
	if meta.Cwd != "/tmp/project" || meta.ThreadID != "thread-1" {
		t.Fatalf("unexpected portal metadata: %#v", meta)
	}
}

func TestRecentDirectoryContactCachesThreadModel(t *testing.T) {
	connector := &Connector{threadRooms: map[string]threadRoom{}}
	client := &Client{Main: connector, UserLogin: testUserLogin("sh-codex")}
	contacts := client.contactsForThreads(context.Background(), []appserver.Thread{{
		ID:            "thread-1",
		Cwd:           "/tmp/project",
		ModelProvider: "openai",
		Raw:           map[string]any{"model": "gpt-5", "modelProvider": "openai"},
		UpdatedAt:     20,
	}})
	if len(contacts) != 3 || contacts[2].Chat != nil {
		t.Fatalf("unexpected contacts: %#v", contacts)
	}
	room, ok := connector.threadRoom("thread-1")
	if !ok {
		t.Fatal("expected project contact to cache thread room")
	}
	if room.modelProvider != "openai" || room.model != "openai/gpt-5" {
		t.Fatalf("project contact cached wrong model: %#v", room)
	}
}

func TestRememberThreadRoomTracksModel(t *testing.T) {
	connector := &Connector{threadRooms: map[string]threadRoom{}}
	client := &Client{Main: connector, UserLogin: testUserLogin("sh-codex")}
	key := projectPortalKey("/tmp/project", "sh-codex")

	connector.rememberThreadRoom("thread-1", client, key, "/tmp/project", "openai", "gpt-5")
	connector.rememberThreadRoom("thread-1", client, key, "", "")

	room, ok := connector.threadRoom("thread-1")
	if !ok {
		t.Fatal("expected cached thread room")
	}
	if room.cwd != "/tmp/project" || room.modelProvider != "openai" || room.model != "openai/gpt-5" {
		t.Fatalf("thread room cache lost metadata: %#v", room)
	}
}

func TestRememberThreadRoomKeepsOneThreadPerPortal(t *testing.T) {
	connector := &Connector{threadRooms: map[string]threadRoom{}}
	client := &Client{Main: connector, UserLogin: testUserLogin("sh-codex")}
	key := projectPortalKey("/tmp/project", "sh-codex")

	connector.rememberThreadRoom("old-thread", client, key, "/tmp/project", "openai", "gpt-4")
	connector.rememberThreadRoom("new-thread", client, key, "/tmp/project", "openai", "gpt-5")

	if _, ok := connector.threadRoom("old-thread"); ok {
		t.Fatal("old thread should not remain mapped to the same portal")
	}
	state := connector.modelStateForPortalKey(key)
	if state["model"] != "openai/gpt-5" {
		t.Fatalf("portal model should use the new thread, got %#v", state)
	}
}

func TestHandleMatrixMessageReturnsTurnStartError(t *testing.T) {
	connector := &Connector{}
	connector.Init(nil)
	client := &Client{Main: connector, UserLogin: testUserLogin("codex")}
	_, err := client.HandleMatrixMessage(context.Background(), testMatrixMessage("thread-1", "hello"))
	if err == nil || !strings.Contains(err.Error(), "Codex app-server is not running") {
		t.Fatalf("expected app-server error, got %v", err)
	}
	var status bridgev2.MessageStatus
	if !errors.As(err, &status) || status.Status != event.MessageStatusFail || status.ErrorReason != event.MessageStatusBridgeUnavailable {
		t.Fatalf("expected bridge-unavailable message status, got %#v", err)
	}
}

func TestHandleMatrixMessageReturnsTurnSteerError(t *testing.T) {
	connector := &Connector{}
	connector.Init(nil)
	client := &Client{Main: connector, UserLogin: testUserLogin("codex")}
	connector.setActive("thread-1", newActiveRun(client, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1"))
	_, err := client.HandleMatrixMessage(context.Background(), testMatrixMessage("thread-1", "hello"))
	if err == nil || !strings.Contains(err.Error(), "Codex app-server is not running") {
		t.Fatalf("expected app-server error, got %v", err)
	}
	var status bridgev2.MessageStatus
	if !errors.As(err, &status) || status.Status != event.MessageStatusFail || status.ErrorReason != event.MessageStatusBridgeUnavailable {
		t.Fatalf("expected bridge-unavailable message status, got %#v", err)
	}
}

func TestMatrixMessageStatusForCodexErrorIsUserVisible(t *testing.T) {
	err := &appserver.RPCError{
		Method: "turn/start",
		Code:   -32602,
		Msg:    "The 'openai/gpt-5.5' model is not supported when using Codex with a ChatGPT account.",
	}

	status := matrixMessageStatusForCodexError(err)
	if status.Status != event.MessageStatusFail || status.ErrorReason != event.MessageStatusUnsupported || !status.IsCertain || !status.SendNotice {
		t.Fatalf("unsupported Codex error should be a certain user-visible failure: %#v", status)
	}
	if !strings.Contains(status.Message, "gpt-5.5") || status.InternalError != err {
		t.Fatalf("unsupported Codex error lost user-facing detail: %#v", status)
	}
}

func TestHandleMatrixMessageInvalidStarterDirectoryIsHandledWithNotice(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing")
	client := &Client{UserLogin: testUserLogin("sh-codex")}
	msg := &bridgev2.MatrixMessage{MatrixEventBase: bridgev2.MatrixEventBase[*event.MessageEventContent]{
		Event: &event.Event{
			ID:        "$event",
			Timestamp: 1000,
		},
		Content: &event.MessageEventContent{
			MsgType: event.MsgText,
			Body:    missing,
		},
		Portal: &bridgev2.Portal{Portal: &database.Portal{
			PortalKey: newProjectPortalKey("sh-codex"),
			Metadata:  &PortalMetadata{},
		}},
	}}

	resp, err := client.HandleMatrixMessage(context.Background(), msg)
	if err != nil {
		t.Fatal(err)
	}
	meta, _ := resp.DB.Metadata.(*MessageMetadata)
	if meta == nil || meta.Role != "command" || meta.StreamStatus != "directory_error" || meta.ThreadID != "" {
		t.Fatalf("invalid starter directory should be handled as command metadata: resp=%#v meta=%#v", resp, meta)
	}
}

func TestHandleMatrixMessageEmptyStarterDirectoryAsksForDirectory(t *testing.T) {
	client := &Client{UserLogin: testUserLogin("sh-codex")}
	msg := &bridgev2.MatrixMessage{MatrixEventBase: bridgev2.MatrixEventBase[*event.MessageEventContent]{
		Event: &event.Event{
			ID:        "$event",
			Timestamp: 1000,
		},
		Content: &event.MessageEventContent{
			MsgType: event.MsgText,
			Body:    " ",
		},
		Portal: &bridgev2.Portal{Portal: &database.Portal{
			PortalKey: newProjectPortalKey("sh-codex"),
			Metadata:  &PortalMetadata{},
		}},
	}}

	resp, err := client.HandleMatrixMessage(context.Background(), msg)
	if err != nil {
		t.Fatal(err)
	}
	meta, _ := resp.DB.Metadata.(*MessageMetadata)
	if meta == nil || meta.Role != "command" || meta.StreamStatus != "directory_prompt" || meta.ThreadID != "" {
		t.Fatalf("empty starter directory should be handled as prompt metadata: resp=%#v meta=%#v", resp, meta)
	}
}

func TestHandleMatrixMessageStartsTurnThroughAppServerTransport(t *testing.T) {
	ctx := context.Background()
	publisher := &recordingBeeperStreamPublisher{}
	matrix := &streamStartMatrix{publisher: publisher, eventID: "$deterministic:example.com"}
	connector, br := testBridgeWithDB(t, matrix)
	br.Config.OutgoingMessageReID = true
	logPath := filepath.Join(t.TempDir(), "fake-appserver.jsonl")
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	app, err := appserver.Start(ctx, exe, map[string]string{
		fakeAppServerEnv:    "1",
		fakeAppServerLogEnv: logPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()
	connector.appMu.Lock()
	connector.app = app
	connector.appMu.Unlock()

	user, err := br.GetUserByMXID(ctx, "@alice:example.com")
	if err != nil {
		t.Fatal(err)
	}
	login, err := connector.ensureLoginID(ctx, user, "sh-codex", "alice@example.com")
	if err != nil {
		t.Fatal(err)
	}
	client := &Client{Main: connector, UserLogin: login, loggedIn: true}
	login.Client = client
	cwd := t.TempDir()
	key := projectPortalKey(cwd, login.ID)
	connector.rememberThreadRoom("thread-1", client, key, cwd, "openai", "gpt-5.5", "high")
	if err = br.DB.Portal.Insert(ctx, &database.Portal{
		PortalKey: key,
		MXID:      "!room:example.com",
		Name:      "project",
		RoomType:  database.RoomTypeDM,
		Metadata:  &PortalMetadata{ThreadID: "thread-1", Cwd: cwd},
	}); err != nil {
		t.Fatal(err)
	}
	portal, err := br.GetExistingPortalByKey(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	msg := &bridgev2.MatrixMessage{MatrixEventBase: bridgev2.MatrixEventBase[*event.MessageEventContent]{
		Event: &event.Event{
			ID:        "$matrix-event",
			Timestamp: 1000,
		},
		Content: &event.MessageEventContent{
			MsgType: event.MsgText,
			Body:    "hello from matrix",
		},
		Portal: portal,
	}}

	resp, err := client.HandleMatrixMessage(ctx, msg)
	if err != nil {
		t.Fatal(err)
	}
	if resp == nil || resp.DB == nil {
		t.Fatalf("missing Matrix response: %#v", resp)
	}
	meta, _ := resp.DB.Metadata.(*MessageMetadata)
	if resp.DB.Room != key || resp.DB.SenderID != networkid.UserID("login:sh-codex") || meta == nil || meta.Role != "user" || meta.ThreadID != "thread-1" || meta.TurnID != "turn-1" || meta.StreamStatus != "done" {
		t.Fatalf("unexpected Matrix response DB: db=%#v meta=%#v", resp.DB, meta)
	}
	run := connector.activeRun("thread-1")
	if run == nil || run.turnID != "turn-1" {
		t.Fatalf("active run was not registered: %#v", run)
	}
	if err = run.start(ctx); err != nil {
		t.Fatal(err)
	}
	if publisher.roomID != portal.MXID || publisher.eventID == "" || len(publisher.updates) == 0 {
		t.Fatalf("Beeper stream was not registered and published: room=%s event=%s updates=%#v", publisher.roomID, publisher.eventID, publisher.updates)
	}

	requests := readFakeAppServerRequests(t, logPath)
	resume, ok := findFakeAppServerRequest(requests, "thread/resume")
	if !ok {
		t.Fatalf("thread/resume request missing: %#v", requests)
	}
	if resume.Params["threadId"] != "thread-1" || resume.Params["cwd"] != cwd || resume.Params["approvalPolicy"] != "on-request" {
		t.Fatalf("bad thread/resume params: %#v", resume.Params)
	}
	start, ok := findFakeAppServerRequest(requests, "turn/start")
	if !ok {
		t.Fatalf("turn/start request missing: %#v", requests)
	}
	if start.Params["threadId"] != "thread-1" || start.Params["approvalPolicy"] != "on-request" || start.Params["clientUserMessageId"] != "$matrix-event" {
		t.Fatalf("bad turn/start params: %#v", start.Params)
	}
	if text := fakeAppServerInputText(start.Params); text != "hello from matrix" {
		t.Fatalf("turn/start prompt mismatch: %q", text)
	}
	if start.Params["model"] != "gpt-5" || start.Params["effort"] != "high" {
		t.Fatalf("turn/start should use hydrated room model settings: %#v", start.Params)
	}
}

func TestHandleMatrixMessageSteersActiveTurnWithTurnMetadata(t *testing.T) {
	ctx := context.Background()
	connector, _ := testBridgeWithDB(t, &fakeMatrixConnector{})
	logPath := filepath.Join(t.TempDir(), "fake-appserver.jsonl")
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	app, err := appserver.Start(ctx, exe, map[string]string{
		fakeAppServerEnv:    "1",
		fakeAppServerLogEnv: logPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()
	connector.appMu.Lock()
	connector.app = app
	connector.appMu.Unlock()

	login := testUserLogin("codex")
	client := &Client{Main: connector, UserLogin: login, loggedIn: true}
	login.Client = client
	msg := testMatrixMessage("thread-1", "steer this")
	run := newActiveRun(client, msg.Portal.PortalKey, "thread-1", "turn-1")
	connector.setActive("thread-1", run)

	resp, err := client.HandleMatrixMessage(ctx, msg)
	if err != nil {
		t.Fatal(err)
	}
	if resp == nil || resp.DB == nil {
		t.Fatalf("missing Matrix response: %#v", resp)
	}
	meta, _ := resp.DB.Metadata.(*MessageMetadata)
	if meta == nil || meta.Role != "user" || meta.ThreadID != "thread-1" || meta.TurnID != "turn-1" || meta.StreamStatus != "done" {
		t.Fatalf("unexpected Matrix response DB: resp=%#v meta=%#v", resp, meta)
	}

	requests := readFakeAppServerRequests(t, logPath)
	steer, ok := findFakeAppServerRequest(requests, "turn/steer")
	if !ok {
		t.Fatalf("turn/steer request missing: %#v", requests)
	}
	if steer.Params["threadId"] != "thread-1" || steer.Params["expectedTurnId"] != "turn-1" || steer.Params["clientUserMessageId"] != "$event" {
		t.Fatalf("bad turn/steer params: %#v", steer.Params)
	}
	if text := fakeAppServerInputText(steer.Params); text != "steer this" {
		t.Fatalf("turn/steer prompt mismatch: %q", text)
	}
}

func TestStartTurnClearsUnsupportedRoomModel(t *testing.T) {
	ctx := context.Background()
	oldPortalEventBuffer := bridgev2.PortalEventBuffer
	bridgev2.PortalEventBuffer = 0
	t.Cleanup(func() { bridgev2.PortalEventBuffer = oldPortalEventBuffer })

	matrix := &modelStateMatrix{
		modelEvt: &event.Event{Content: event.Content{Raw: map[string]any{
			"model":     "openai/gpt-5.5",
			"reasoning": "high",
		}}},
		publisher: &recordingBeeperStreamPublisher{},
	}
	connector, br := testBridgeWithDB(t, matrix)
	logPath := filepath.Join(t.TempDir(), "fake-appserver.jsonl")
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	app, err := appserver.Start(ctx, exe, map[string]string{
		fakeAppServerEnv:    "1",
		fakeAppServerLogEnv: logPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()
	connector.appMu.Lock()
	connector.app = app
	connector.appMu.Unlock()

	user, err := br.GetUserByMXID(ctx, "@alice:example.com")
	if err != nil {
		t.Fatal(err)
	}
	login, err := connector.ensureLoginID(ctx, user, "sh-codex", "alice@example.com")
	if err != nil {
		t.Fatal(err)
	}
	client := &Client{Main: connector, UserLogin: login, loggedIn: true}
	login.Client = client
	cwd := t.TempDir()
	key := projectPortalKey(cwd, login.ID)
	if err = br.DB.Portal.Insert(ctx, &database.Portal{
		PortalKey: key,
		MXID:      "!room:example.com",
		Name:      "project",
		RoomType:  database.RoomTypeDM,
		Metadata:  &PortalMetadata{ThreadID: "thread-1", Cwd: cwd},
	}); err != nil {
		t.Fatal(err)
	}

	if _, err = client.startTurn(ctx, key, "thread-1", "$matrix-event", "hello"); err != nil {
		t.Fatal(err)
	}
	requests := readFakeAppServerRequests(t, logPath)
	start, ok := findFakeAppServerRequest(requests, "turn/start")
	if !ok {
		t.Fatalf("turn/start request missing: %#v", requests)
	}
	if _, ok = start.Params["model"]; ok {
		t.Fatalf("unsupported room model should be omitted: %#v", start.Params)
	}
	if start.Params["effort"] != "high" {
		t.Fatalf("reasoning effort should still be applied: %#v", start.Params)
	}
	state := findFakeState(matrix.intent().states, beeperAIModelStateType)
	if state == nil || state.Content == nil || len(state.Content.Raw) != 0 {
		t.Fatalf("unsupported room model state was not cleared: %#v", state)
	}
	if state := connector.modelStateForPortalKey(key); state != nil {
		t.Fatalf("unsupported room model cache was not cleared: %#v", state)
	}
}

func TestHydrateThreadRoomsReadsMaterializedRoomState(t *testing.T) {
	ctx := context.Background()
	matrix := &fakeMatrixConnector{evt: &event.Event{Content: event.Content{
		Raw: map[string]any{
			"threadId":      "thread-1",
			"sessionId":     "session-1",
			"cwd":           "/tmp/project",
			"model":         "gpt-5",
			"modelProvider": "openai",
		},
	}}}
	connector, br := testBridgeWithDB(t, matrix)
	user, err := br.GetUserByMXID(ctx, "@alice:example.com")
	if err != nil {
		t.Fatal(err)
	}
	login, err := connector.ensureLoginID(ctx, user, "sh-codex", "alice@example.com")
	if err != nil {
		t.Fatal(err)
	}
	starterKey := networkid.PortalKey{ID: "project:", Receiver: login.ID}
	if err = br.DB.Portal.Insert(ctx, &database.Portal{
		PortalKey: starterKey,
		MXID:      "!room:example.com",
		Name:      "project",
		RoomType:  database.RoomTypeDM,
		Metadata:  &PortalMetadata{Cwd: "/tmp/project"},
	}); err != nil {
		t.Fatal(err)
	}

	connector.hydrateThreadRooms(ctx)

	targetKey := projectPortalKey("/tmp/project", login.ID)
	room, ok := connector.threadRoom("thread-1")
	if !ok {
		t.Fatal("expected thread room to be cached")
	}
	if room.portalKey != targetKey || room.cwd != "/tmp/project" || room.model != "openai/gpt-5" {
		t.Fatalf("unexpected hydrated room: %#v", room)
	}
	portal, err := br.GetExistingPortalByKey(ctx, targetKey)
	if err != nil {
		t.Fatal(err)
	}
	if portal == nil || portal.MXID != "!room:example.com" {
		t.Fatalf("canonical portal was not materialized: %#v", portal)
	}
	meta := portalMetadata(portal.Metadata)
	if meta.ThreadID != "thread-1" || meta.Cwd != "/tmp/project" {
		t.Fatalf("portal metadata was not hydrated: %#v", meta)
	}
	task, err := br.DB.BackfillTask.GetNextForPortal(ctx, targetKey, true)
	if err != nil {
		t.Fatal(err)
	}
	if task == nil || task.BatchCount != -1 || task.IsDone || task.QueueDone {
		t.Fatalf("hydrating a thread should reset backfill task: %#v", task)
	}
	if matrix.threadRoomID != "!room:example.com" || matrix.threadStateType.Type != codexThreadStateType || matrix.threadStateKey != "" {
		t.Fatalf("thread room state was not read correctly: %#v", matrix)
	}
}

func TestHydrateThreadRoomsCanonicalizesKnownMaterializedRoom(t *testing.T) {
	ctx := context.Background()
	connector, br := testBridgeWithDB(t, &fakeMatrixConnector{})
	user, err := br.GetUserByMXID(ctx, "@alice:example.com")
	if err != nil {
		t.Fatal(err)
	}
	login, err := connector.ensureLoginID(ctx, user, "sh-codex", "alice@example.com")
	if err != nil {
		t.Fatal(err)
	}
	starterKey := networkid.PortalKey{ID: "project:", Receiver: login.ID}
	if err = br.DB.Portal.Insert(ctx, &database.Portal{
		PortalKey: starterKey,
		MXID:      "!room:example.com",
		Name:      "project",
		RoomType:  database.RoomTypeDM,
		Metadata:  &PortalMetadata{ThreadID: "thread-1", Cwd: "/tmp/project"},
	}); err != nil {
		t.Fatal(err)
	}

	connector.hydrateThreadRooms(ctx)

	targetKey := projectPortalKey("/tmp/project", login.ID)
	room, ok := connector.threadRoom("thread-1")
	if !ok {
		t.Fatal("expected known materialized room to be cached")
	}
	if room.portalKey != targetKey || room.cwd != "/tmp/project" {
		t.Fatalf("known materialized room was not canonicalized: %#v", room)
	}
	if portal, err := br.GetExistingPortalByKey(ctx, starterKey); err != nil {
		t.Fatal(err)
	} else if portal != nil {
		t.Fatalf("starter portal key should have been re-id'd away: %#v", portal.PortalKey)
	}
	if portal, err := br.GetExistingPortalByKey(ctx, targetKey); err != nil {
		t.Fatal(err)
	} else if portal == nil || portal.MXID != "!room:example.com" {
		t.Fatalf("canonical portal missing after hydrate: %#v", portal)
	}
}

func TestHydrateThreadRoomsDeletesUnmaterializedProjectPortals(t *testing.T) {
	ctx := context.Background()
	connector, br := testBridgeWithDB(t, &fakeMatrixConnector{})
	user, err := br.GetUserByMXID(ctx, "@alice:example.com")
	if err != nil {
		t.Fatal(err)
	}
	login, err := connector.ensureLoginID(ctx, user, "sh-codex", "alice@example.com")
	if err != nil {
		t.Fatal(err)
	}
	staleKey := projectPortalKey("/tmp/stale", login.ID)
	realKey := projectPortalKey("/tmp/real", login.ID)
	otherKey := networkid.PortalKey{ID: "new:starter", Receiver: login.ID}
	for _, portal := range []*database.Portal{
		{PortalKey: staleKey, Metadata: &PortalMetadata{ThreadID: "thread-stale", Cwd: "/tmp/stale"}},
		{PortalKey: realKey, MXID: "!real:example.com", Metadata: &PortalMetadata{ThreadID: "thread-real", Cwd: "/tmp/real"}},
		{PortalKey: otherKey},
	} {
		if err = br.DB.Portal.Insert(ctx, portal); err != nil {
			t.Fatal(err)
		}
	}

	connector.hydrateThreadRooms(ctx)

	if portal, err := br.GetExistingPortalByKey(ctx, staleKey); err != nil {
		t.Fatal(err)
	} else if portal != nil {
		t.Fatalf("stale project portal should have been deleted: %#v", portal.PortalKey)
	}
	if portal, err := br.GetExistingPortalByKey(ctx, realKey); err != nil {
		t.Fatal(err)
	} else if portal == nil || portal.MXID != "!real:example.com" {
		t.Fatalf("materialized project portal should be preserved: %#v", portal)
	}
	if portal, err := br.GetExistingPortalByKey(ctx, otherKey); err != nil {
		t.Fatal(err)
	} else if portal == nil {
		t.Fatal("non-project portal should be preserved")
	}
}

func TestRememberPortalThreadRoomIgnoresUnmaterializedPortals(t *testing.T) {
	connector := &Connector{}
	connector.Init(&bridgev2.Bridge{})
	connector.rememberPortalThreadRoom(&bridgev2.Portal{Portal: &database.Portal{
		PortalKey: networkid.PortalKey{ID: "project:ghost", Receiver: "sh-codex"},
		Metadata:  &PortalMetadata{ThreadID: "thread-1", Cwd: "/tmp/project"},
	}})
	if _, ok := connector.threadRoom("thread-1"); ok {
		t.Fatal("unmaterialized portals must not be cached as thread rooms")
	}
}

func TestClearMissingThreadForgetsRuntimeMapping(t *testing.T) {
	ctx := context.Background()
	connector, br := testBridgeWithDB(t, &fakeMatrixConnector{})
	user, err := br.GetUserByMXID(ctx, "@alice:example.com")
	if err != nil {
		t.Fatal(err)
	}
	login, err := connector.ensureLoginID(ctx, user, "sh-codex", "alice@example.com")
	if err != nil {
		t.Fatal(err)
	}
	client := &Client{Main: connector, UserLogin: login}
	key := projectPortalKey("/tmp/project", login.ID)
	if err = br.DB.Portal.Insert(ctx, &database.Portal{
		PortalKey: key,
		MXID:      "!room:example.com",
		RoomType:  database.RoomTypeDM,
		Metadata:  &PortalMetadata{ThreadID: "thread-1", Cwd: "/tmp/project"},
	}); err != nil {
		t.Fatal(err)
	}
	portal, err := br.GetExistingPortalByKey(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	connector.rememberThreadRoom("thread-1", client, key, "/tmp/project", "openai", "gpt-5")
	connector.setActive("thread-1", newActiveRun(client, key, "thread-1", "turn-1"))

	client.clearMissingThread(ctx, portal, portalMetadata(portal.Metadata))

	meta := portalMetadata(portal.Metadata)
	if meta.ThreadID != "" || meta.Cwd != "/tmp/project" {
		t.Fatalf("portal metadata was not cleared correctly: %#v", meta)
	}
	if _, ok := connector.threadRoom("thread-1"); ok {
		t.Fatal("missing thread should be removed from thread room cache")
	}
	if active := connector.activeRun("thread-1"); active != nil {
		t.Fatalf("missing thread should clear active run: %#v", active)
	}
}

func TestForgetThreadRemovesStaleProcessMappingWithoutActiveRun(t *testing.T) {
	connector := &Connector{
		threadRooms: map[string]threadRoom{
			"thread-1": {portalKey: projectPortalKey("/tmp/project", "sh-codex")},
		},
		processes: map[string]*activeRun{
			"proc-1": {threadID: "thread-1"},
		},
	}

	connector.forgetThread("thread-1")

	if _, ok := connector.threadRoom("thread-1"); ok {
		t.Fatal("forgotten thread should be removed from thread room cache")
	}
	if run := connector.activeRunForProcess([]byte(`{"processId":"proc-1"}`)); run != nil {
		t.Fatalf("forgotten thread should remove stale process mapping: %#v", run)
	}
}

func TestEnsureBackfillVersionResetsOldCompletedTask(t *testing.T) {
	ctx := context.Background()
	connector, br := testBridgeWithDB(t, &fakeMatrixConnector{})
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
		RoomType:  database.RoomTypeDM,
		Metadata:  &PortalMetadata{ThreadID: "thread-1", Cwd: "/tmp/project", BackfillVersion: codexBackfillVersion - 1},
	}); err != nil {
		t.Fatal(err)
	}
	if err = br.DB.BackfillTask.Upsert(ctx, &database.BackfillTask{
		PortalKey:         key,
		UserLoginID:       login.ID,
		BatchCount:        1,
		IsDone:            true,
		QueueDone:         true,
		NextDispatchMinTS: time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	portal, err := br.GetExistingPortalByKey(ctx, key)
	if err != nil {
		t.Fatal(err)
	}

	(&Client{Main: connector, UserLogin: login}).ensureBackfillVersion(ctx, portal)

	meta := portalMetadata(portal.Metadata)
	if meta.BackfillVersion != codexBackfillVersion {
		t.Fatalf("backfill version was not saved: %#v", meta)
	}
	task, err := br.DB.BackfillTask.GetNextForPortal(ctx, key, true)
	if err != nil {
		t.Fatal(err)
	}
	if task == nil || task.BatchCount != -1 || task.IsDone || task.QueueDone {
		t.Fatalf("old completed backfill task was not reset: %#v", task)
	}
}

func TestLoginIDsUseConfiguredBridgeAlias(t *testing.T) {
	connector := &Connector{Bridge: &bridgev2.Bridge{
		ID: "codex",
		Matrix: &matrixconnector.Connector{Config: &bridgeconfig.Config{
			AppService: bridgeconfig.AppserviceConfig{
				Bot:              bridgeconfig.BotUserConfig{Username: "sh-codexbot"},
				UsernameTemplate: "sh-codex_{{.}}",
			},
		}},
	}}
	got := connector.loginIDsForUser("")
	want := []networkid.UserLoginID{"sh-codex"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected login IDs:\n got: %#v\nwant: %#v", got, want)
	}
}

func TestLoginIDsFallBackToBridgeID(t *testing.T) {
	connector := &Connector{Bridge: &bridgev2.Bridge{ID: "codex"}}
	got := connector.loginIDsForUser("")
	want := []networkid.UserLoginID{defaultLoginID}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected login IDs:\n got: %#v\nwant: %#v", got, want)
	}
}

func testBridgeWithDB(t *testing.T, matrix bridgev2.MatrixConnector) (*Connector, *bridgev2.Bridge) {
	t.Helper()
	rawDB, err := sql.Open("sqlite3", ":memory:?_busy_timeout=5000")
	if err != nil {
		t.Fatal(err)
	}
	rawDB.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = rawDB.Close() })
	db, err := dbutil.NewWithDB(rawDB, "sqlite3")
	if err != nil {
		t.Fatal(err)
	}
	connector := &Connector{}
	br := bridgev2.NewBridge("codex", db, zerolog.Nop(), &bridgeconfig.BridgeConfig{}, matrix, connector, func(*bridgev2.Bridge) bridgev2.CommandProcessor {
		return nil
	})
	br.BackgroundCtx = context.Background()
	if err = br.DB.Upgrade(context.Background()); err != nil {
		t.Fatal(err)
	}
	return connector, br
}

func testUserLogin(id networkid.UserLoginID) *bridgev2.UserLogin {
	return &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: id}}
}

func testGhost(id networkid.UserID) *bridgev2.Ghost {
	return &bridgev2.Ghost{Ghost: &database.Ghost{ID: id}}
}

func testMatrixMessage(threadID, body string) *bridgev2.MatrixMessage {
	return &bridgev2.MatrixMessage{
		MatrixEventBase: bridgev2.MatrixEventBase[*event.MessageEventContent]{
			Event: &event.Event{
				ID:        id.EventID("$event"),
				Timestamp: 1000,
			},
			Content: &event.MessageEventContent{
				MsgType: event.MsgText,
				Body:    body,
			},
			Portal: &bridgev2.Portal{Portal: &database.Portal{
				PortalKey: projectPortalKey("/tmp/project", "codex"),
				Metadata:  &PortalMetadata{ThreadID: threadID, Cwd: "/tmp/project"},
			}},
		},
	}
}

type fakeArbitraryRoomState struct {
	bridgev2.MatrixConnector
	evt       *event.Event
	err       error
	roomID    id.RoomID
	stateType event.Type
	stateKey  string
}

func (f *fakeArbitraryRoomState) GetStateEvent(ctx context.Context, roomID id.RoomID, eventType event.Type, stateKey string) (*event.Event, error) {
	f.roomID = roomID
	f.stateType = eventType
	f.stateKey = stateKey
	return f.evt, f.err
}

type fakeMatrixConnector struct {
	bridgev2.MatrixConnector
	api             *fakeMatrixAPI
	evt             *event.Event
	err             error
	threadRoomID    id.RoomID
	threadStateType event.Type
	threadStateKey  string
	bridgeStateCh   chan bridgestatus.BridgeState
}

func (f *fakeMatrixConnector) Init(*bridgev2.Bridge) {}

func (f *fakeMatrixConnector) GetCapabilities() *bridgev2.MatrixCapabilities {
	return &bridgev2.MatrixCapabilities{ExtraProfileMeta: true}
}

func (f *fakeMatrixConnector) GenerateDeterministicRoomID(portalKey networkid.PortalKey) id.RoomID {
	return id.RoomID("!" + sanitizeID(string(portalKey.ID)) + ":" + f.ServerName())
}

func (f *fakeMatrixConnector) ServerName() string {
	return "example.com"
}

func (f *fakeMatrixConnector) intent() *fakeMatrixAPI {
	if f.api == nil {
		f.api = &fakeMatrixAPI{}
	}
	return f.api
}

func (f *fakeMatrixConnector) BotIntent() bridgev2.MatrixAPI { return f.intent() }

func (f *fakeMatrixConnector) GhostIntent(userID networkid.UserID) bridgev2.MatrixAPI {
	return f.intent()
}

func (f *fakeMatrixConnector) ParseGhostMXID(userID id.UserID) (networkid.UserID, bool) {
	return "", false
}

func (f *fakeMatrixConnector) NewUserIntent(ctx context.Context, userID id.UserID, accessToken string) (bridgev2.MatrixAPI, string, error) {
	return f.intent(), accessToken, nil
}

func (f *fakeMatrixConnector) SendBridgeStatus(ctx context.Context, state *bridgestatus.BridgeState) error {
	if f.bridgeStateCh != nil && state != nil {
		select {
		case f.bridgeStateCh <- *state:
		default:
		}
	}
	return nil
}

func (f *fakeMatrixConnector) GetPowerLevels(ctx context.Context, roomID id.RoomID) (*event.PowerLevelsEventContent, error) {
	return &event.PowerLevelsEventContent{Users: map[id.UserID]int{}}, nil
}

func (f *fakeMatrixConnector) GetMembers(ctx context.Context, roomID id.RoomID) (map[id.UserID]*event.MemberEventContent, error) {
	return map[id.UserID]*event.MemberEventContent{}, nil
}

func (f *fakeMatrixConnector) GetMemberInfo(ctx context.Context, roomID id.RoomID, userID id.UserID) (*event.MemberEventContent, error) {
	return &event.MemberEventContent{Membership: event.MembershipJoin}, nil
}

func (f *fakeMatrixConnector) GetStateEvent(ctx context.Context, roomID id.RoomID, eventType event.Type, stateKey string) (*event.Event, error) {
	if eventType.Type != codexThreadStateType {
		return nil, mautrix.MNotFound
	}
	f.threadRoomID = roomID
	f.threadStateType = eventType
	f.threadStateKey = stateKey
	return f.evt, f.err
}

type modelStateMatrix struct {
	fakeMatrixConnector
	modelEvt  *event.Event
	publisher bridgev2.BeeperStreamPublisher
}

func (m *modelStateMatrix) GetStateEvent(ctx context.Context, roomID id.RoomID, eventType event.Type, stateKey string) (*event.Event, error) {
	if eventType.Type == beeperAIModelStateType {
		return m.modelEvt, nil
	}
	return m.fakeMatrixConnector.GetStateEvent(ctx, roomID, eventType, stateKey)
}

func (m *modelStateMatrix) GetBeeperStreamPublisher() bridgev2.BeeperStreamPublisher {
	return m.publisher
}

type fakeMatrixAPI struct {
	bridgev2.MatrixAPI
	states        []fakeStateEvent
	messages      []fakeMessageEvent
	roomCount     int
	onSendMessage func(roomID id.RoomID, eventType event.Type, content *event.Content, extra *bridgev2.MatrixSendExtra)
}

func (f *fakeMatrixAPI) GetMXID() id.UserID { return "@bot:example.com" }

func (f *fakeMatrixAPI) IsDoublePuppet() bool { return false }

func (f *fakeMatrixAPI) CreateRoom(ctx context.Context, req *mautrix.ReqCreateRoom) (id.RoomID, error) {
	f.roomCount++
	if req != nil && req.BeeperLocalRoomID != "" {
		return req.BeeperLocalRoomID, nil
	}
	return id.RoomID("!created" + sanitizeID(string(rune('0'+f.roomCount))) + ":example.com"), nil
}

func (f *fakeMatrixAPI) SendMessage(ctx context.Context, roomID id.RoomID, eventType event.Type, content *event.Content, extra *bridgev2.MatrixSendExtra) (*mautrix.RespSendEvent, error) {
	if f.onSendMessage != nil {
		f.onSendMessage(roomID, eventType, content, extra)
	}
	eventID := id.EventID("$message")
	if len(f.messages) > 0 {
		eventID = id.EventID(fmt.Sprintf("$message-%d", len(f.messages)+1))
	}
	f.messages = append(f.messages, fakeMessageEvent{RoomID: roomID, Type: eventType, Content: content, Extra: extra})
	return &mautrix.RespSendEvent{EventID: eventID}, nil
}

func (f *fakeMatrixAPI) SendState(ctx context.Context, roomID id.RoomID, eventType event.Type, stateKey string, content *event.Content, ts time.Time) (*mautrix.RespSendEvent, error) {
	f.states = append(f.states, fakeStateEvent{RoomID: roomID, Type: eventType, StateKey: stateKey, Content: content})
	return &mautrix.RespSendEvent{EventID: id.EventID("$state")}, nil
}

func (f *fakeMatrixAPI) MarkTyping(ctx context.Context, roomID id.RoomID, typingType bridgev2.TypingType, timeout time.Duration) error {
	return nil
}

func (f *fakeMatrixAPI) EnsureJoined(ctx context.Context, roomID id.RoomID, params ...bridgev2.EnsureJoinedParams) error {
	return nil
}

func (f *fakeMatrixAPI) SetDisplayName(ctx context.Context, name string) error { return nil }

func (f *fakeMatrixAPI) SetAvatarURL(ctx context.Context, avatarURL id.ContentURIString) error {
	return nil
}

func (f *fakeMatrixAPI) SetExtraProfileMeta(ctx context.Context, data any) error { return nil }

func (f *fakeMatrixAPI) SetProfile(ctx context.Context, data any) error { return nil }

type fakeStateEvent struct {
	RoomID   id.RoomID
	Type     event.Type
	StateKey string
	Content  *event.Content
}

type fakeMessageEvent struct {
	RoomID  id.RoomID
	Type    event.Type
	Content *event.Content
	Extra   *bridgev2.MatrixSendExtra
}
