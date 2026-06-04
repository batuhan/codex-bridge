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

	aistream "github.com/beeper/ai-bridge/pkg/ai-stream"
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

func TestThreadRollbackParams(t *testing.T) {
	got := threadRollbackParams("thread-1", 2)
	want := map[string]any{"threadId": "thread-1", "numTurns": 2}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected rollback params:\n got: %#v\nwant: %#v", got, want)
	}
}

func TestThreadReadParams(t *testing.T) {
	got := threadReadParams("thread-1", true)
	want := map[string]any{"threadId": "thread-1", "includeTurns": true}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected thread read params:\n got: %#v\nwant: %#v", got, want)
	}
}

func TestThreadIDParams(t *testing.T) {
	got := threadIDParams("thread-1")
	got["turnId"] = "turn-1"
	want := map[string]any{"threadId": "thread-1", "turnId": "turn-1"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected thread ID params:\n got: %#v\nwant: %#v", got, want)
	}
}

func TestThreadOpenParams(t *testing.T) {
	start := threadStartParams("/tmp/project")
	wantStart := map[string]any{
		"cwd":            "/tmp/project",
		"approvalPolicy": codexApprovalPolicyOnRequest,
		"threadSource":   "user",
	}
	if !reflect.DeepEqual(start, wantStart) {
		t.Fatalf("unexpected thread start params:\n got: %#v\nwant: %#v", start, wantStart)
	}

	resume := threadResumeParams("thread-1")
	wantResume := map[string]any{
		"threadId":       "thread-1",
		"approvalPolicy": codexApprovalPolicyOnRequest,
		"excludeTurns":   true,
	}
	if !reflect.DeepEqual(resume, wantResume) {
		t.Fatalf("unexpected thread resume params:\n got: %#v\nwant: %#v", resume, wantResume)
	}
}

func TestRequestLimitParams(t *testing.T) {
	got := requestLimitParams(100, "next")
	want := map[string]any{"limit": 100, "cursor": "next"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected limit params:\n got: %#v\nwant: %#v", got, want)
	}

	got = requestLimitParams(100, "")
	want = map[string]any{"limit": 100}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected limit params without cursor:\n got: %#v\nwant: %#v", got, want)
	}
}

func TestThreadListParams(t *testing.T) {
	got := threadListParams("next", 20)
	want := map[string]any{
		"limit":         20,
		"cursor":        "next",
		"sortKey":       threadListSortKey,
		"sortDirection": sortDirectionDescending,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected thread list params:\n got: %#v\nwant: %#v", got, want)
	}
}

func TestThreadTurnsListParams(t *testing.T) {
	got := threadTurnsListParams("thread-1", "next")
	want := map[string]any{
		"limit":         threadTurnsListLimit,
		"cursor":        "next",
		"threadId":      "thread-1",
		"sortDirection": sortDirectionAscending,
		"itemsView":     threadTurnsItemsView,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected thread turns list params:\n got: %#v\nwant: %#v", got, want)
	}
}

func TestModelListParams(t *testing.T) {
	got := modelListParams("next")
	want := map[string]any{"limit": modelListLimit, "cursor": "next", "includeHidden": true}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected model list params:\n got: %#v\nwant: %#v", got, want)
	}
}

func TestCodexClientTurnRequestStateOmitsEmptyOptionalFields(t *testing.T) {
	got := codexClientTurnRequestState("thread-1", "", "turn-1", "")
	want := map[string]any{"threadId": "thread-1", "expectedTurnId": "turn-1"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected client turn request state:\n got: %#v\nwant: %#v", got, want)
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
	if want, err := filepath.EvalSymlinks(dir); err == nil {
		if got != want {
			t.Fatalf("unexpected path: got %q want %q", got, want)
		}
	} else if got != dir {
		t.Fatalf("unexpected path: got %q want %q", got, dir)
	}
}

func TestCreateChatWithGhostMapsProjectGhosts(t *testing.T) {
	client := &Client{UserLogin: testUserLogin(defaultLoginID)}
	resp, err := client.CreateChatWithGhost(context.Background(), testGhost(projectUserID("/tmp/project")))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(resp.PortalKey.ID), "new:") || resp.PortalKey.Receiver != defaultLoginID {
		t.Fatalf("unexpected portal key: %#v", resp.PortalKey)
	}
	if resp.PortalInfo == nil || resp.PortalInfo.CanBackfill {
		t.Fatalf("project starter chat should not run synchronous room-create backfill: %#v", resp.PortalInfo)
	}
	portal := &bridgev2.Portal{Portal: &database.Portal{PortalKey: resp.PortalKey}}
	if !resp.PortalInfo.ExtraUpdates(context.Background(), portal) {
		t.Fatal("expected project starter metadata updater to report a change")
	}
	meta := portalMetadata(portal.Metadata)
	if meta.Cwd != "/tmp/project" || meta.ThreadID != "" {
		t.Fatalf("project starter should persist cwd without binding an existing thread: %#v", meta)
	}
}

func TestCreateChatWithGhostMapsCodexGhostToNewProjectChat(t *testing.T) {
	client := &Client{UserLogin: testUserLogin("login-1")}
	resp, err := client.CreateChatWithGhost(context.Background(), testGhost(codexUserID))
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
	second, err := client.CreateChatWithGhost(context.Background(), testGhost(codexUserID))
	if err != nil {
		t.Fatal(err)
	}
	if second.PortalKey == resp.PortalKey {
		t.Fatalf("new project chats should use unique starter keys: %#v", resp.PortalKey)
	}
}

func TestCreateChatWithGhostMapsNewProjectGhostToNewProjectChat(t *testing.T) {
	client := &Client{UserLogin: testUserLogin("login-1")}
	resp, err := client.CreateChatWithGhost(context.Background(), testGhost(newProjectUserID))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(resp.PortalKey.ID), "new:") || resp.PortalKey.Receiver != "login-1" {
		t.Fatalf("unexpected portal key: %#v", resp.PortalKey)
	}
	if resp.PortalInfo == nil || resp.PortalInfo.Name == nil || *resp.PortalInfo.Name != "New Project" {
		t.Fatalf("new project ghost should create a New Project room: %#v", resp.PortalInfo)
	}
}

func TestProjectStarterChatQueuesBackgroundBackfill(t *testing.T) {
	ctx := context.Background()
	connector, br := testBridgeWithDB(t, &fakeMatrixConnector{})
	login := &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "sh-codex"}, Bridge: br}
	client := &Client{Main: connector, UserLogin: login}
	login.Client = client

	resp := client.newProjectChatForCWD("/tmp/project")
	if err := br.DB.Portal.Insert(ctx, &database.Portal{
		PortalKey: resp.PortalKey,
		MXID:      "!project:example.com",
		RoomType:  database.RoomTypeDM,
		Metadata:  &PortalMetadata{},
	}); err != nil {
		t.Fatal(err)
	}
	portal, err := br.GetExistingPortalByKey(ctx, resp.PortalKey)
	if err != nil {
		t.Fatal(err)
	}
	if !resp.PortalInfo.ExtraUpdates(ctx, portal) {
		t.Fatal("expected project starter metadata updater to report a change")
	}
	meta := portalMetadata(portal.Metadata)
	if meta.Cwd != "/tmp/project" || meta.ThreadID != "" || meta.BackfillVersion != codexBackfillVersion {
		t.Fatalf("project starter should persist cwd and backfill version: %#v", meta)
	}
	task, err := br.DB.BackfillTask.GetNextForPortal(ctx, resp.PortalKey, true)
	if err != nil {
		t.Fatal(err)
	}
	if task == nil || task.UserLoginID != login.ID || task.BatchCount != -1 {
		t.Fatalf("project starter should queue background backfill task: %#v", task)
	}
}

func TestCreateChatWithGhostRejectsUnknownGhost(t *testing.T) {
	client := &Client{UserLogin: testUserLogin(defaultLoginID)}
	if _, err := client.CreateChatWithGhost(context.Background(), testGhost("other")); err == nil {
		t.Fatal("expected unknown ghost error")
	}
}

func TestProjectIDRoundTripsTrimmedCWD(t *testing.T) {
	projectID := projectID(" /tmp/project ")
	cwd, ok := parseProjectID(projectID)
	if !ok || cwd != "/tmp/project" {
		t.Fatalf("project ID did not round trip: cwd=%q ok=%v id=%q", cwd, ok, projectID)
	}
}

func TestProjectIDPayload(t *testing.T) {
	id := projectID("/tmp/project")
	payload, ok := projectIDPayload(id)
	if !ok || payload == "" {
		t.Fatalf("project ID payload was not extracted: payload=%q ok=%v", payload, ok)
	}
	if _, ok := projectIDPayload("subagent:thread-1"); ok {
		t.Fatal("non-project ID should not expose a project payload")
	}
	if _, ok := projectIDPayload(projectIDPrefix); ok {
		t.Fatal("empty project ID payload should be rejected")
	}
}

func TestRawURLDecoded(t *testing.T) {
	encoded := rawURLEncoded([]byte("/tmp/project"))
	decoded, err := rawURLDecoded(encoded)
	if err != nil || string(decoded) != "/tmp/project" {
		t.Fatalf("unexpected raw URL decode: decoded=%q err=%v", decoded, err)
	}
	if _, err := rawURLDecoded("***"); err == nil {
		t.Fatal("invalid raw URL payload should fail")
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

	ok, err = (&Client{}).HandleMatrixRoomTopic(context.Background(), &bridgev2.MatrixRoomTopic{
		MatrixEventBase: bridgev2.MatrixEventBase[*event.TopicEventContent]{
			Portal:  portal,
			Content: &event.TopicEventContent{Topic: ""},
		},
	})
	if err != nil || !ok {
		t.Fatalf("topic clear failed: ok=%v err=%v", ok, err)
	}
	if portal.Topic != "" || !portal.TopicSet {
		t.Fatalf("cleared topic should stay marked as Matrix-set: %#v", portal.Portal)
	}
}

func TestHandleMatrixRoomStateUpdatesAIModelCache(t *testing.T) {
	key := projectPortalKey("/tmp/project", "sh-codex")
	connector := &Connector{}
	connector.Init(&bridgev2.Bridge{})
	login := testUserLogin("sh-codex")
	client := &Client{Main: connector, UserLogin: login}
	connector.rememberThreadRoom("thread-1", client, key, "/tmp/project", map[string]any{"modelProvider": "openai", "model": "gpt-5", "reasoningEffort": "low"})
	run := newActiveRun(client, key, "thread-1", "turn-1")
	connector.setActive("thread-1", run)
	portal := &bridgev2.Portal{Portal: &database.Portal{PortalKey: key}}

	ok, err := client.HandleMatrixRoomState(context.Background(), &bridgev2.MatrixRoomState{
		MatrixEventBase: bridgev2.MatrixEventBase[map[string]any]{
			Portal: portal,
			Content: map[string]any{
				"model":          "claude-sonnet-4.5",
				"name":           "Claude Sonnet 4.5",
				"provider":       "anthropic",
				"reasoning":      "high",
				"reasoning_mode": "adaptive",
			},
		},
		Type: event.Type{Type: beeperAIModelStateType, Class: event.StateEventType},
	})
	if err != nil || !ok {
		t.Fatalf("room state update failed: ok=%v err=%v", ok, err)
	}
	state := connector.modelStateForPortalKey(key)
	if state["model"] != "anthropic/claude-sonnet-4.5" || state["name"] != "Claude Sonnet 4.5" || state["modelName"] != "Claude Sonnet 4.5" || state["reasoning"] != "high" || state["reasoning_mode"] != "adaptive" || state["reasoningMode"] != "adaptive" {
		t.Fatalf("model state cache not updated: %#v", state)
	}
	if run.run.Model != "anthropic/claude-sonnet-4.5" {
		t.Fatalf("active run model was not updated: %q", run.run.Model)
	}
	if !hasCodexRoomStateDelta(run.run.Events, beeperAIModelStateType, "model", "claude-sonnet-4.5") ||
		!hasCodexRoomStateDelta(run.run.Events, beeperAIModelStateType, "provider", "anthropic") {
		t.Fatalf("active run did not sync model room state: %#v", run.run.Events)
	}
	run.writer.Text("after room model")
	if got := run.run.Events[len(run.run.Events)-1].Get("model"); got != "anthropic/claude-sonnet-4.5" {
		t.Fatalf("future stream event kept stale model: %#v", got)
	}

	ok, err = client.HandleMatrixRoomState(context.Background(), &bridgev2.MatrixRoomState{
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
	if state["model"] != "anthropic/claude-sonnet-4.5" || state["modelProvider"] != "anthropic" || state["reasoning"] != "low" {
		t.Fatalf("effort-only model state should preserve cached model while updating reasoning: %#v", state)
	}

	ok, err = client.HandleMatrixRoomState(context.Background(), &bridgev2.MatrixRoomState{
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
	if !hasCodexRoomStateDelta(run.run.Events, beeperAIModelStateType, "model", "") {
		t.Fatalf("active run did not sync cleared model room state: %#v", run.run.Events)
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
	connector.rememberThreadRoom("old-thread", &Client{Main: connector, UserLogin: login}, key, "/old/project", map[string]any{"modelProvider": "openai", "model": "gpt-4.1", "reasoningEffort": "low"})

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
	if err != nil || ok {
		t.Fatalf("bridge-owned Codex thread room state should be ignored without claiming a change: ok=%v err=%v", ok, err)
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

	ok, err = (&Client{Main: connector}).HandleMatrixRoomName(ctx, &bridgev2.MatrixRoomName{
		MatrixEventBase: bridgev2.MatrixEventBase[*event.RoomNameEventContent]{
			Portal:  portal,
			Content: &event.RoomNameEventContent{Name: ""},
		},
	})
	if err != nil || !ok {
		t.Fatalf("room name clear failed: ok=%v err=%v", ok, err)
	}
	if portal.Name != "" || !portal.NameSet {
		t.Fatalf("cleared name should stay marked as Matrix-set: %#v", portal.Portal)
	}
}

func TestStartThreadAppliesRoomModelAndReasoningConfig(t *testing.T) {
	ctx := context.Background()
	matrix := &modelStateMatrix{modelEvt: &event.Event{Content: event.Content{Raw: map[string]any{
		"model":     "openai/gpt-5.1",
		"reasoning": "high",
	}}}}
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

	login := testUserLogin("sh-codex")
	login.Bridge = br
	client := &Client{Main: connector, UserLogin: login, loggedIn: true}
	cwd := t.TempDir()
	portal := &bridgev2.Portal{Portal: &database.Portal{
		PortalKey: projectPortalKey(cwd, login.ID),
		MXID:      "!room:example.com",
		Metadata:  &PortalMetadata{Cwd: cwd},
	}}
	if _, err = client.startThreadForPortal(ctx, cwd, portal); err != nil {
		t.Fatal(err)
	}

	requests := readFakeAppServerRequests(t, logPath)
	req, ok := findFakeAppServerRequest(requests, "thread/start")
	if !ok {
		t.Fatalf("thread/start request missing: %#v", requests)
	}
	if req.Params["model"] != "gpt-5.1" {
		t.Fatalf("thread/start did not apply room model: %#v", req.Params)
	}
	config, _ := req.Params["config"].(map[string]any)
	if config["model_reasoning_effort"] != "high" {
		t.Fatalf("thread/start did not apply room reasoning config: %#v", req.Params)
	}
	if _, hasEffort := req.Params["effort"]; hasEffort {
		t.Fatalf("thread/start must not use unsupported top-level effort: %#v", req.Params)
	}
}

func TestResumeThreadAppliesRoomModelAndReasoningConfig(t *testing.T) {
	ctx := context.Background()
	matrix := &modelStateMatrix{modelEvt: &event.Event{Content: event.Content{Raw: map[string]any{
		"model":           "openai/gpt-5.1",
		"reasoningEffort": "medium",
	}}}}
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

	login := testUserLogin("sh-codex")
	login.Bridge = br
	client := &Client{Main: connector, UserLogin: login, loggedIn: true}
	cwd := t.TempDir()
	portal := &bridgev2.Portal{Portal: &database.Portal{
		PortalKey: projectPortalKey(cwd, login.ID),
		MXID:      "!room:example.com",
		Metadata:  &PortalMetadata{ThreadID: "thread-1", Cwd: cwd},
	}}
	if _, err = client.resumeThreadForPortal(ctx, portal, portalMetadata(portal.Metadata)); err != nil {
		t.Fatal(err)
	}

	requests := readFakeAppServerRequests(t, logPath)
	req, ok := findFakeAppServerRequest(requests, "thread/resume")
	if !ok {
		t.Fatalf("thread/resume request missing: %#v", requests)
	}
	if req.Params["model"] != "gpt-5.1" {
		t.Fatalf("thread/resume did not apply room model: %#v", req.Params)
	}
	config, _ := req.Params["config"].(map[string]any)
	if config["model_reasoning_effort"] != "medium" {
		t.Fatalf("thread/resume did not apply room reasoning config: %#v", req.Params)
	}
	if _, hasEffort := req.Params["effort"]; hasEffort {
		t.Fatalf("thread/resume must not use unsupported top-level effort: %#v", req.Params)
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

func TestGetChatInfoPreservesRoomModelStateWhenThreadReadOmitsModel(t *testing.T) {
	ctx := context.Background()
	matrix := &modelStateMatrix{
		fakeMatrixConnector: fakeMatrixConnector{api: &fakeMatrixAPI{}},
		modelEvt: &event.Event{Content: event.Content{Raw: map[string]any{
			"model":     "openai/gpt-5.1",
			"name":      "GPT-5.1",
			"reasoning": "medium",
		}}},
	}
	connector, br := testBridgeWithDB(t, matrix)
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	app, err := appserver.Start(ctx, exe, map[string]string{fakeAppServerEnv: "1"})
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
	info, err := client.GetChatInfo(ctx, portal)
	if err != nil {
		t.Fatal(err)
	}
	if info.ExtraUpdates == nil || !info.ExtraUpdates(ctx, portal) {
		t.Fatal("expected chat info extra updates to sync preserved room model state")
	}
	state := findFakeState(matrix.intent().states, beeperAIModelStateType)
	if state == nil || state.Content == nil {
		t.Fatalf("missing Beeper AI model state update: %#v", matrix.intent().states)
	}
	if state.Content.Raw["model"] != "openai/gpt-5.1" || state.Content.Raw["name"] != "GPT-5.1" || state.Content.Raw["reasoning"] != "medium" {
		t.Fatalf("GetChatInfo did not preserve room model state: %#v", state.Content.Raw)
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

func TestNewProjectChatInfoQueuesIntroOnRoomCreate(t *testing.T) {
	ctx := context.Background()
	oldPortalEventBuffer := bridgev2.PortalEventBuffer
	bridgev2.PortalEventBuffer = 0
	t.Cleanup(func() { bridgev2.PortalEventBuffer = oldPortalEventBuffer })

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
	requireEventually(t, time.Second, func() bool { return len(matrix.intent().messages) == 1 })
	matrix.intent().messages = nil
	client := &Client{Main: connector, UserLogin: login, loggedIn: true}
	login.Client = client
	key := newProjectPortalKey(login.ID)
	if err = br.DB.Portal.Insert(ctx, &database.Portal{
		PortalKey: key,
		MXID:      "!starter:example.com",
		Name:      "New Project",
		RoomType:  database.RoomTypeDM,
		Metadata:  &PortalMetadata{},
	}); err != nil {
		t.Fatal(err)
	}
	portal, err := br.GetExistingPortalByKey(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	info := client.newProjectChatInfo()
	if info.ExtraUpdates == nil || !info.ExtraUpdates(ctx, portal) {
		t.Fatal("expected new project room creation to queue the intro message")
	}

	requireEventually(t, time.Second, func() bool { return len(matrix.intent().messages) == 1 })
	msg := matrix.intent().messages[0]
	content, ok := msg.Content.Parsed.(*event.MessageEventContent)
	if msg.RoomID != "!starter:example.com" || !ok {
		t.Fatalf("unexpected intro message: %#v", msg)
	}
	for _, want := range []string{
		"Send a project directory path",
		"first path message only picks the project",
		"import recent Codex context as backfill",
		"Beeper AI parts",
		"/approvals",
	} {
		if !strings.Contains(content.Body, want) {
			t.Fatalf("intro message is missing %q: %q", want, content.Body)
		}
	}
	meta := portalMetadata(portal.Metadata)
	if !meta.NewProjectIntroMessage || meta.ThreadID != "" || meta.Cwd != "" {
		t.Fatalf("unexpected new project metadata after intro: %#v", meta)
	}
	info.ExtraUpdates(ctx, portal)
	time.Sleep(20 * time.Millisecond)
	if len(matrix.intent().messages) != 1 {
		t.Fatalf("new project intro should only be queued once, got %#v", matrix.intent().messages)
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
	connector.rememberThreadRoom("missing-thread", client, key, "/tmp/project", map[string]any{"modelProvider": "openai", "model": "gpt-5"})

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
	if got := firstString(client.roomAIModelState(context.Background(), portal), "model"); got != "openai/gpt-5" {
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
	connector.rememberThreadRoom("thread-1", client, key, "/tmp/project", map[string]any{"modelProvider": "openai", "model": "gpt-5", "reasoningEffort": "high", "modelName": "GPT-5", "reasoningMode": "adaptive"})

	portal := &bridgev2.Portal{Portal: &database.Portal{PortalKey: key}}
	if got := firstString(client.roomAIModelState(context.Background(), portal), "model"); got != "openai/gpt-5" {
		t.Fatalf("unexpected cached room model: %q", got)
	}
	state := client.roomAIModelStateForPortalKey(context.Background(), key)
	if state["model"] != "openai/gpt-5" || state["name"] != "GPT-5" || state["modelName"] != "GPT-5" || state["modelProvider"] != "openai" || state["effort"] != "high" || state["reasoning"] != "high" || state["reasoning_mode"] != "adaptive" || state["reasoningMode"] != "adaptive" {
		t.Fatalf("unexpected cached model state: %#v", state)
	}
}

func TestRoomModelStateOverridesCachedThreadState(t *testing.T) {
	matrix := &fakeArbitraryRoomState{evt: &event.Event{Content: event.Content{
		Raw: map[string]any{"model": "openai/gpt-5.1", "reasoning": "high", "reasoning_mode": "adaptive"},
	}}}
	connector := &Connector{Bridge: &bridgev2.Bridge{Matrix: matrix}, threadRooms: map[string]threadRoom{}}
	client := &Client{Main: connector, UserLogin: testUserLogin("sh-codex")}
	key := projectPortalKey("/tmp/project", "sh-codex")
	connector.rememberThreadRoom("thread-1", client, key, "/tmp/project", map[string]any{"modelProvider": "openai", "model": "gpt-5"})

	portal := &bridgev2.Portal{Portal: &database.Portal{PortalKey: key, MXID: "!room:example.com"}}
	state := client.roomAIModelState(context.Background(), portal)
	if state["model"] != "openai/gpt-5.1" || state["reasoning"] != "high" || state["reasoning_mode"] != "adaptive" || state["modelProvider"] != "openai" {
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
	if got := firstString(client.roomAIModelState(context.Background(), portal), "model"); got != "anthropic/claude-sonnet-4.5" {
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
	if got := firstString(client.roomAIModelState(context.Background(), portal), "model"); got != "" {
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

func TestRequestModelCandidatePrefersRoomModel(t *testing.T) {
	roomModel, model, fromRoomState := requestModelCandidate(map[string]any{"provider": "openai", "model": "gpt-5"}, "openai/gpt-4")
	if roomModel != "openai/gpt-5" || model != "gpt-5" || !fromRoomState {
		t.Fatalf("unexpected room model candidate: room=%q model=%q fromRoom=%v", roomModel, model, fromRoomState)
	}

	roomModel, model, fromRoomState = requestModelCandidate(nil, "openai/gpt-4")
	if roomModel != "" || model != "gpt-4" || fromRoomState {
		t.Fatalf("unexpected default model candidate: room=%q model=%q fromRoom=%v", roomModel, model, fromRoomState)
	}
}

func TestAdvanceCursorStopsOnEmptyAndRepeatedCursor(t *testing.T) {
	seen := map[string]bool{}
	cursor := "start"
	if !advanceCursor(seen, &cursor, "next") || cursor != "next" {
		t.Fatalf("cursor did not advance: cursor=%q seen=%#v", cursor, seen)
	}
	if advanceCursor(seen, &cursor, "next") {
		t.Fatalf("repeated cursor should stop pagination: cursor=%q seen=%#v", cursor, seen)
	}
	if advanceCursor(seen, &cursor, "") {
		t.Fatalf("empty cursor should stop pagination: cursor=%q seen=%#v", cursor, seen)
	}
}

func TestValidatedRoomTurnSettingsUsesProviderQualifiedRoomModel(t *testing.T) {
	ctx := context.Background()
	connector, br := testBridgeWithDB(t, &fakeMatrixConnector{})
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

	login := testUserLogin("sh-codex")
	login.Bridge = br
	client := &Client{Main: connector, UserLogin: login, loggedIn: true}
	params := map[string]any{"threadId": "thread-1"}
	client.applyValidatedRoomTurnSettings(ctx, params, map[string]any{
		"provider":  "anthropic",
		"model":     "claude-sonnet-4.5",
		"reasoning": "high",
	}, nil, networkid.PortalKey{})

	if params["model"] != "anthropic/claude-sonnet-4.5" {
		t.Fatalf("split provider/model room state was not normalized for Codex: %#v", params)
	}
	if params["effort"] != "high" {
		t.Fatalf("reasoning effort should still be applied: %#v", params)
	}
	if state := connector.modelStateForPortalKey(networkid.PortalKey{}); state != nil {
		t.Fatalf("valid split provider/model state should not be cleared: %#v", state)
	}
}

func TestContactsIncludeOneNewProjectGhost(t *testing.T) {
	client := &Client{UserLogin: testUserLogin("sh-codex")}
	contacts := client.contactsForThreads(context.Background(), nil)
	got := make([]networkid.UserID, 0, len(contacts))
	for _, contact := range contacts {
		got = append(got, contact.UserID)
	}
	want := []networkid.UserID{newProjectUserID}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected contacts:\n got: %#v\nwant: %#v", got, want)
	}
	for _, contact := range contacts {
		if contact.Chat != nil {
			t.Fatalf("base contact %s should not allocate random chat portals: %#v", contact.UserID, contact.Chat)
		}
	}
	if contacts[0].UserInfo == nil || contacts[0].UserInfo.IsBot == nil || *contacts[0].UserInfo.IsBot {
		t.Fatalf("New Project contact should not be marked as a network bot: %#v", contacts[0].UserInfo)
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
	if len(contacts) != 2 {
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
	if contacts[0].Ghost.ID != newProjectUserID {
		t.Fatalf("unexpected base contact ghost: %#v", contacts[0].Ghost.ID)
	}
	if contacts[1].Ghost.ID != projectUserID("/tmp/project") {
		t.Fatalf("unexpected project contact ghost: %#v", contacts[1].Ghost.ID)
	}

	listed, err := provisionutil.GetContactList(context.Background(), login)
	if err != nil {
		t.Fatal(err)
	}
	if len(listed.Contacts) < 1 || listed.Contacts[0].MXID == "" {
		t.Fatalf("provisioned base contact should include ghost MXID: %#v", listed.Contacts)
	}
	searched, err := provisionutil.SearchUsers(context.Background(), login, "codex")
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
	cleanProjectDir, err := cleanProjectDir(projectDir)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.ID != projectUserID(cleanProjectDir) || resolved.MXID == "" || resolved.AvatarURL == "" {
		t.Fatalf("provisioned resolve should expose project ghost: %#v", resolved)
	}
}

func TestSearchUsersFindsBaseContactsWithoutAllocatingChats(t *testing.T) {
	client := &Client{UserLogin: testUserLogin("sh-codex")}
	results, err := client.SearchUsers(context.Background(), "codex")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatalf("expected one result, got %#v", results)
	}
	if results[0].Chat != nil {
		t.Fatalf("base search result should not allocate random chat portals: %#v", results[0].Chat)
	}
}

func TestGetContactListFallsBackToCachedThreadRooms(t *testing.T) {
	connector := &Connector{threadRooms: map[string]threadRoom{}}
	login := testUserLogin("sh-codex")
	client := &Client{Main: connector, UserLogin: login}
	connector.rememberThreadRoom("thread-1", client, projectPortalKey("/tmp/project", "sh-codex"), "/tmp/project", map[string]any{"modelProvider": "openai", "model": "gpt-5", "reasoningEffort": "high", "modelName": "GPT-5"})

	contacts, err := client.GetContactList(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(contacts) != 2 {
		t.Fatalf("cached contact fallback should include base contact and project, got %#v", contacts)
	}
	project := contacts[1]
	if project.UserID != projectUserID("/tmp/project") {
		t.Fatalf("unexpected cached project contact: %#v", project)
	}
	if !identifiersContain(project.UserInfo, "/tmp/project") || !identifiersContain(project.UserInfo, "thread-1") {
		t.Fatalf("cached project contact identifiers should include cwd and thread id: %#v", project.UserInfo)
	}
	room, ok := connector.threadRoom("thread-1")
	if !ok {
		t.Fatal("cached contact fallback should preserve thread room")
	}
	if room.modelProvider != "openai" || room.model != "openai/gpt-5" || room.reasoningEffort != "high" || room.modelName != "GPT-5" {
		t.Fatalf("cached contact fallback lost model state: %#v", room)
	}
}

func TestContactsIgnoreDetachedThreads(t *testing.T) {
	client := &Client{}
	contacts := client.contactsForThreads(context.Background(), []appserver.Thread{
		{ID: "active-thread", Cwd: "/tmp/project", UpdatedAt: 10},
		{ID: "archived-thread", Cwd: "/tmp/project", UpdatedAt: 20, Raw: map[string]any{"archived": true}},
		{ID: "closed-thread", Cwd: "/tmp/closed", UpdatedAt: 30, Raw: map[string]any{"closed": true}},
	})
	if len(contacts) != 2 {
		t.Fatalf("expected base contact plus one active project, got %#v", contacts)
	}
	project := contacts[1]
	if project.UserID != projectUserID("/tmp/project") || !identifiersContain(project.UserInfo, "active-thread") || identifiersContain(project.UserInfo, "archived-thread") {
		t.Fatalf("contacts should use the active project thread only: %#v", project)
	}
}

func TestResolveIdentifierDecodesProjectIDs(t *testing.T) {
	dir := t.TempDir()
	client := &Client{UserLogin: testUserLogin("sh-codex")}
	resp, err := client.ResolveIdentifier(context.Background(), string(projectPortalKey(dir, "sh-codex").ID), true)
	if err != nil {
		t.Fatal(err)
	}
	cwd, err := cleanProjectDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if resp.UserID != projectUserID(cwd) || resp.Chat == nil || !strings.HasPrefix(string(resp.Chat.PortalKey.ID), "new:") {
		t.Fatalf("unexpected project identifier response: %#v", resp)
	}
	portal := &bridgev2.Portal{Portal: &database.Portal{PortalKey: resp.Chat.PortalKey}}
	if !resp.Chat.PortalInfo.ExtraUpdates(context.Background(), portal) {
		t.Fatal("expected project identifier chat metadata updater to report a change")
	}
	if meta := portalMetadata(portal.Metadata); meta.Cwd != cwd || meta.ThreadID != "" {
		t.Fatalf("project identifier chat should persist cwd without binding an existing thread: %#v", meta)
	}
}

func TestResolveIdentifierCodexUsesNewProjectGhostProfile(t *testing.T) {
	client := &Client{UserLogin: testUserLogin("sh-codex")}
	resp, err := client.ResolveIdentifier(context.Background(), "codex", false)
	if err != nil {
		t.Fatal(err)
	}
	if resp.UserID != newProjectUserID || resp.UserInfo == nil || resp.UserInfo.Name == nil || *resp.UserInfo.Name != "New Project" || resp.UserInfo.IsBot == nil || *resp.UserInfo.IsBot {
		t.Fatalf("Codex identifier should resolve to New Project ghost profile: %#v", resp)
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
	requireEventually(t, time.Second, func() bool { return len(matrix.intent().messages) == 1 })
	matrix.intent().messages = nil
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

func TestNewProjectRoomQueuesIntroMessage(t *testing.T) {
	ctx := context.Background()
	oldPortalEventBuffer := bridgev2.PortalEventBuffer
	bridgev2.PortalEventBuffer = 0
	t.Cleanup(func() { bridgev2.PortalEventBuffer = oldPortalEventBuffer })

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
	requireEventually(t, time.Second, func() bool { return len(matrix.intent().messages) == 1 })
	matrix.intent().messages = nil
	client := &Client{Main: connector, UserLogin: login, loggedIn: true}
	login.Client = client

	resp, err := provisionutil.ResolveIdentifier(ctx, login, "new project", true)
	if err != nil {
		t.Fatal(err)
	}
	if resp == nil || resp.Portal == nil {
		t.Fatalf("new project DM was not created: %#v", resp)
	}
	requireEventually(t, time.Second, func() bool { return len(matrix.intent().messages) == 1 })
	msg := matrix.intent().messages[0]
	if msg.RoomID != resp.Portal.MXID || msg.Type != event.EventMessage {
		t.Fatalf("intro sent to wrong Matrix target: %#v", msg)
	}
	content, ok := msg.Content.Parsed.(*event.MessageEventContent)
	for _, want := range []string{
		"Send a project directory path",
		"Your first path message only picks the project",
		"import recent Codex context as backfill",
		"turn this room into that project chat",
		"responses stream into the room as Beeper AI parts",
		"long final output is attached as a file",
		"/approvals",
		"/approve <id> approve|always|deny",
		"/answer <id> <text>",
		"/stop",
	} {
		if !ok || !strings.Contains(content.Body, want) {
			t.Fatalf("intro missing %q in Matrix content: %#v", want, msg.Content)
		}
	}
	ai, ok := msg.Content.Raw[aistream.BeeperAIKey].(aistream.BeeperAI)
	if !ok || ai.Kind != aistream.AIKindFinal || ai.ThreadID != string(resp.Portal.ID) || ai.Model == "" {
		t.Fatalf("intro missing final Beeper AI payload: %#v", msg.Content.Raw)
	}
	meta := portalMetadata(resp.Portal.Metadata)
	if !meta.NewProjectIntroMessage || meta.ThreadID != "" || meta.Cwd != "" {
		t.Fatalf("intro metadata was not saved correctly: %#v", meta)
	}
	if state := findFakeState(matrix.intent().states, codexThreadStateType); state != nil {
		t.Fatalf("new project room should not send placeholder Codex thread state: %#v", state)
	}
	if state := findFakeState(matrix.intent().states, beeperAIModelStateType); state != nil {
		t.Fatalf("new project room should not send placeholder Beeper AI model state: %#v", state)
	}
	if state := findFakeState(matrix.intent().states, event.StateMSC4391BotCommand.Type); state == nil {
		t.Fatalf("new project room should still advertise command state: %#v", matrix.intent().states)
	}

	info, err := client.GetChatInfo(ctx, resp.Portal)
	if err != nil {
		t.Fatal(err)
	}
	resp.Portal.UpdateInfo(ctx, info, login, nil, time.Now())
	time.Sleep(20 * time.Millisecond)
	if len(matrix.intent().messages) != 1 {
		t.Fatalf("intro should only be queued once, got %#v", matrix.intent().messages)
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
	cleanCWD, err := cleanProjectDir(cwd)
	if err != nil {
		t.Fatal(err)
	}
	if resp.ID != projectUserID(cleanCWD) || !strings.HasPrefix(string(resp.Portal.PortalKey.ID), "new:") || resp.Portal.PortalKey.Receiver != login.ID {
		t.Fatalf("project create DM used wrong identity: %#v portal=%#v", resp, resp.Portal.Portal)
	}
	meta := portalMetadata(resp.Portal.Metadata)
	if meta.Cwd != cleanCWD || meta.ThreadID != "" {
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
	if !isThreadTurnsListUnavailable(&appserver.RPCError{Method: "thread/turns/list", Msg: "thread is not materialized yet; thread/turns/list is unavailable before first user message"}) {
		t.Fatal("unmaterialized new threads should use thread/read fallback")
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
	if len(contacts) != 2 {
		t.Fatalf("unexpected contacts: %#v", contacts)
	}
	if contacts[0].Chat != nil {
		t.Fatalf("base contact should not include random chats: %#v", contacts[0])
	}
	project := contacts[1]
	if project.UserID != projectUserID("/tmp/project") {
		t.Fatalf("unexpected project contact: %#v", project)
	}
	if !identifiersContain(project.UserInfo, "/tmp/project") || !identifiersContain(project.UserInfo, "new-thread") {
		t.Fatalf("project contact identifiers should include cwd and latest thread id: %#v", project.UserInfo)
	}
	if project.UserInfo == nil || project.UserInfo.Name == nil || *project.UserInfo.Name != "/tmp/project" {
		t.Fatalf("project contact should use full path display name: %#v", project.UserInfo)
	}
	if project.Chat != nil {
		t.Fatalf("project contact should not allocate a chat before opening it: %#v", project.Chat)
	}
	chat := client.chatForProject(context.Background(), "/tmp/project", "new-thread", nil)
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
		{ID: "thread-1", Cwd: "/tmp/codex-bridge", Name: "Fix streaming", UpdatedAt: 20},
	})
	if len(contacts) != 2 {
		t.Fatalf("unexpected contacts: %#v", contacts)
	}
	project := contacts[1]
	for _, query := range []string{"codex-bridge", "/tmp/codex-bridge", "thread-1", "fix streaming"} {
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

func TestUniqueIdentifiersTrimsAndDeduplicatesCaseInsensitive(t *testing.T) {
	got := uniqueIdentifiers(" Codex ", "codex", "", "Thread-1", "thread-1", "/tmp/project")
	want := []string{"Codex", "Thread-1", "/tmp/project"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v want %#v", got, want)
	}
}

func TestSortedRecentDirectoriesKeepsNewestAttachedThreadPerDirectory(t *testing.T) {
	threads := sortedRecentDirectories([]appserver.Thread{
		{ID: "old-project", Cwd: "/tmp/project", UpdatedAt: 10},
		{ID: "new-project", Cwd: "/tmp/project", UpdatedAt: 30},
		{ID: "archived-project", Cwd: "/tmp/archive", UpdatedAt: 40, Raw: map[string]any{"archived": true}},
		{ID: "other-project", Cwd: "/tmp/other", UpdatedAt: 20},
		{ID: "missing-cwd", UpdatedAt: 50},
	})
	if len(threads) != 2 || threads[0].ID != "new-project" || threads[1].ID != "other-project" {
		t.Fatalf("unexpected recent directories: %#v", threads)
	}
}

func TestSortedRecentDirectoriesCanonicalizesEquivalentPaths(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "project")
	if err := os.Mkdir(target, 0o755); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "project-link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}

	threads := sortedRecentDirectories([]appserver.Thread{
		{ID: "target-project", Cwd: target, UpdatedAt: 10},
		{ID: "link-project", Cwd: link, UpdatedAt: 20},
	})
	canonical := projectCanonicalPath(target)
	if len(threads) != 1 || threads[0].ID != "link-project" || threads[0].Cwd != canonical {
		t.Fatalf("equivalent paths should collapse to the newest canonical project: %#v want cwd %q", threads, canonical)
	}
}

func TestSearchUsersReturnsExistingDirectoryPathAsProjectGhost(t *testing.T) {
	cwd := t.TempDir()
	client := &Client{UserLogin: testUserLogin("sh-codex")}

	results, err := client.SearchUsers(context.Background(), cwd)
	if err != nil {
		t.Fatal(err)
	}
	cleanCWD, err := cleanProjectDir(cwd)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) == 0 || results[0].UserID != projectUserID(cleanCWD) {
		t.Fatalf("directory search did not return direct project ghost: %#v", results)
	}
	if results[0].UserInfo == nil || results[0].UserInfo.Name == nil || *results[0].UserInfo.Name != projectDisplayPath(cleanCWD) {
		t.Fatalf("direct project ghost should use compact path display name: %#v", results[0].UserInfo)
	}
}

func TestSearchUsersReturnsDirectoryPathWhenContactListFails(t *testing.T) {
	cwd := t.TempDir()
	client := &Client{Main: &Connector{}, UserLogin: testUserLogin("sh-codex")}

	results, err := client.SearchUsers(context.Background(), cwd)
	if err != nil {
		t.Fatal(err)
	}
	cleanCWD, err := cleanProjectDir(cwd)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].UserID != projectUserID(cleanCWD) {
		t.Fatalf("directory search should survive contact list failure: %#v", results)
	}
}

func TestSyncContactGhostsCreatesBaseAndRecentDirectoryGhosts(t *testing.T) {
	ctx := context.Background()
	connector, br := testBridgeWithDB(t, &fakeMatrixConnector{})
	connector.syncContactGhostsForThreads(ctx, []appserver.Thread{
		{ID: "old-thread", Cwd: "/tmp/project", Preview: "old", UpdatedAt: 10},
		{ID: "new-thread", Cwd: "/tmp/project", Name: "Build bridge", Preview: strings.Repeat("preview ", 100), UpdatedAt: 20},
	})

	for _, userID := range []networkid.UserID{newProjectUserID, projectUserID("/tmp/project")} {
		ghost, err := br.GetExistingGhostByID(ctx, userID)
		if err != nil {
			t.Fatal(err)
		}
		if ghost == nil || ghost.Name == "" || ghost.AvatarMXC != id.ContentURIString(defaultCodexAvatarMXC) {
			t.Fatalf("contact ghost %s was not synced: %#v", userID, ghost)
		}
		if ghost.IsBot {
			t.Fatalf("contact ghost %s should not be marked as a network bot", userID)
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
	if project.Name != "/tmp/project" {
		t.Fatalf("project contact should use full path display name, got %q", project.Name)
	}
	portals, err := br.GetAllPortals(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(portals) != 0 {
		t.Fatalf("contact ghost sync must not create portals: %#v", portals)
	}
}

func TestSyncRecentContactGhostsFallsBackToCachedThreadRooms(t *testing.T) {
	ctx := context.Background()
	connector, br := testBridgeWithDB(t, &fakeMatrixConnector{})
	login := &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "sh-codex"}, Bridge: br}
	client := &Client{Main: connector, UserLogin: login}
	login.Client = client
	connector.rememberThreadRoom("thread-1", client, projectPortalKey("/tmp/project", login.ID), "/tmp/project", nil)

	connector.syncRecentContactGhosts(ctx)

	project, err := br.GetExistingGhostByID(ctx, projectUserID("/tmp/project"))
	if err != nil {
		t.Fatal(err)
	}
	if project == nil || project.Name != "/tmp/project" || !identifiersContain(&bridgev2.UserInfo{Identifiers: project.Identifiers}, "thread-1") {
		t.Fatalf("cached project contact ghost was not synced: %#v", project)
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

func TestRecentDirectoryContactDoesNotRewriteCachedProjectRoom(t *testing.T) {
	connector := &Connector{threadRooms: map[string]threadRoom{}}
	client := &Client{Main: connector, UserLogin: testUserLogin("sh-codex")}
	newProjectKey := projectPortalKey("", "sh-codex")
	connector.rememberThreadRoom("thread-1", client, newProjectKey, "/tmp/project", nil)

	contacts := client.contactsForThreads(context.Background(), []appserver.Thread{
		{ID: "thread-1", Cwd: "/tmp/project", UpdatedAt: 20},
	})
	if len(contacts) != 2 {
		t.Fatalf("unexpected contacts: %#v", contacts)
	}
	project := contacts[1]
	if project.Chat != nil {
		t.Fatalf("project contact should not expose a starter chat: %#v", project.Chat)
	}
	room, ok := connector.threadRoom("thread-1")
	if !ok {
		t.Fatal("expected existing thread room cache to remain available")
	}
	if room.portalKey != newProjectKey {
		t.Fatalf("project contact listing should not rewrite cached room portals: %#v", room)
	}
	chat := client.chatForProject(context.Background(), "/tmp/project", "thread-1", nil)
	portal := &bridgev2.Portal{Portal: &database.Portal{PortalKey: chat.PortalKey}}
	if !chat.PortalInfo.ExtraUpdates(context.Background(), portal) {
		t.Fatal("expected portal metadata updater to report a change")
	}
	meta := portalMetadata(portal.Metadata)
	if meta.Cwd != "/tmp/project" || meta.ThreadID != "thread-1" {
		t.Fatalf("unexpected portal metadata: %#v", meta)
	}
}

func TestRecentDirectoryContactDoesNotCacheThreadModel(t *testing.T) {
	connector := &Connector{threadRooms: map[string]threadRoom{}}
	client := &Client{Main: connector, UserLogin: testUserLogin("sh-codex")}
	contacts := client.contactsForThreads(context.Background(), []appserver.Thread{{
		ID:            "thread-1",
		Cwd:           "/tmp/project",
		ModelProvider: "openai",
		Raw:           map[string]any{"model": "gpt-5", "modelProvider": "openai"},
		UpdatedAt:     20,
	}})
	if len(contacts) != 2 || contacts[1].Chat != nil {
		t.Fatalf("unexpected contacts: %#v", contacts)
	}
	if room, ok := connector.threadRoom("thread-1"); ok {
		t.Fatalf("contact listing should not cache thread room model state: %#v", room)
	}
}

func TestRememberThreadRoomTracksModel(t *testing.T) {
	connector := &Connector{threadRooms: map[string]threadRoom{}}
	client := &Client{Main: connector, UserLogin: testUserLogin("sh-codex")}
	key := projectPortalKey("/tmp/project", "sh-codex")

	connector.rememberThreadRoom("thread-1", client, key, "/tmp/project", map[string]any{"modelProvider": "openai", "model": "gpt-5"})
	connector.rememberThreadRoom("thread-1", client, key, "", nil)

	room, ok := connector.threadRoom("thread-1")
	if !ok {
		t.Fatal("expected cached thread room")
	}
	if room.cwd != "/tmp/project" || room.modelProvider != "openai" || room.model != "openai/gpt-5" {
		t.Fatalf("thread room cache lost metadata: %#v", room)
	}
}

func TestThreadRoomFillStatePreservesModelAliases(t *testing.T) {
	room := threadRoom{
		model:           "openai/gpt-5",
		modelProvider:   "openai",
		modelName:       "GPT-5",
		reasoningEffort: "high",
		reasoningMode:   "adaptive",
	}

	state := map[string]any{"cwd": "/tmp/project"}
	room.fillState(state)
	if state["model"] != "openai/gpt-5" || state["modelProvider"] != "openai" || state["modelName"] != "GPT-5" || state["reasoningEffort"] != "high" || state["reasoning_mode"] != "adaptive" {
		t.Fatalf("thread room did not fill missing model fields: %#v", state)
	}

	aliasState := map[string]any{
		"toModel":       "anthropic/claude-sonnet-4.5",
		"provider":      "anthropic",
		"reasoning":     "medium",
		"modelName":     "Claude",
		"reasoningMode": "manual",
	}
	room.fillState(aliasState)
	for _, key := range []string{"model", "modelProvider", "reasoningEffort", "reasoning_mode"} {
		if _, exists := aliasState[key]; exists {
			t.Fatalf("thread room overwrote existing alias for %q: %#v", key, aliasState)
		}
	}
	if aliasState["modelName"] != "Claude" {
		t.Fatalf("thread room overwrote model name: %#v", aliasState)
	}
}

func TestRememberThreadRoomKeepsOneThreadPerPortal(t *testing.T) {
	connector := &Connector{threadRooms: map[string]threadRoom{}}
	client := &Client{Main: connector, UserLogin: testUserLogin("sh-codex")}
	key := projectPortalKey("/tmp/project", "sh-codex")

	connector.rememberThreadRoom("old-thread", client, key, "/tmp/project", map[string]any{"modelProvider": "openai", "model": "gpt-4"})
	connector.rememberThreadRoom("new-thread", client, key, "/tmp/project", map[string]any{"modelProvider": "openai", "model": "gpt-5"})

	if _, ok := connector.threadRoom("old-thread"); ok {
		t.Fatal("old thread should not remain mapped to the same portal")
	}
	state := connector.modelStateForPortalKey(key)
	if state["model"] != "openai/gpt-5" {
		t.Fatalf("portal model should use the new thread, got %#v", state)
	}
}

func TestHandleMatrixMessageAcksTurnStartBeforeAppServerWork(t *testing.T) {
	connector := &Connector{}
	connector.Init(nil)
	client := &Client{Main: connector, UserLogin: testUserLogin("codex")}
	resp, err := client.HandleMatrixMessage(context.Background(), testMatrixMessage("thread-1", "hello"))
	if err != nil {
		t.Fatalf("Matrix message should ack before async app-server work: %v", err)
	}
	meta, _ := resp.DB.Metadata.(*MessageMetadata)
	if meta == nil || meta.Role != "user" || meta.ThreadID != "thread-1" || meta.TurnID != "" || meta.StreamStatus != "done" {
		t.Fatalf("unexpected ack metadata: %#v", resp.DB.Metadata)
	}
}

func TestHandleMatrixMessageAcksTurnSteerBeforeAppServerWork(t *testing.T) {
	connector := &Connector{}
	connector.Init(nil)
	client := &Client{Main: connector, UserLogin: testUserLogin("codex")}
	connector.setActive("thread-1", newActiveRun(client, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1"))
	resp, err := client.HandleMatrixMessage(context.Background(), testMatrixMessage("thread-1", "hello"))
	if err != nil {
		t.Fatalf("Matrix steer should ack before async app-server work: %v", err)
	}
	meta, _ := resp.DB.Metadata.(*MessageMetadata)
	if meta == nil || meta.Role != "user" || meta.ThreadID != "thread-1" || meta.TurnID != "turn-1" || meta.StreamStatus != "done" {
		t.Fatalf("unexpected ack metadata: %#v", resp.DB.Metadata)
	}
}

func TestHandleMatrixMessageRejectsUnsupportedPromptContent(t *testing.T) {
	connector := &Connector{}
	connector.Init(nil)
	client := &Client{Main: connector, UserLogin: testUserLogin("codex")}

	tests := []struct {
		name       string
		msg        *bridgev2.MatrixMessage
		wantReason event.MessageStatusReason
	}{
		{
			name:       "media",
			msg:        testMatrixMessage("thread-1", "image.png"),
			wantReason: event.MessageStatusUnsupported,
		},
		{
			name:       "reply",
			msg:        testMatrixMessage("thread-1", "reply text"),
			wantReason: event.MessageStatusUnsupported,
		},
		{
			name: "notice",
			msg:  testMatrixMessage("thread-1", "bridge notice"),
		},
	}
	tests[0].msg.Content.MsgType = event.MsgImage
	tests[1].msg.ReplyTo = &database.Message{ID: "codex:turn-1:assistant"}
	tests[2].msg.Content.MsgType = event.MsgNotice

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := client.HandleMatrixMessage(context.Background(), tc.msg)
			if err == nil {
				t.Fatal("expected unsupported Matrix prompt content to be rejected")
			}
			if strings.Contains(err.Error(), "app-server") {
				t.Fatalf("unsupported prompt content should be rejected before app-server work: %v", err)
			}
			var status bridgev2.MessageStatus
			if !errors.As(err, &status) {
				t.Fatalf("expected message status error, got %T: %v", err, err)
			}
			if tc.wantReason != "" && status.ErrorReason != tc.wantReason {
				t.Fatalf("error reason = %s, want %s: %#v", status.ErrorReason, tc.wantReason, status)
			}
			if tc.name == "notice" && status.SendNotice {
				t.Fatalf("m.notice should be ignored without a user notice: %#v", status)
			}
		})
	}
}

func TestHandleMatrixMessageRejectsReadOnlySubagentPortal(t *testing.T) {
	connector := &Connector{}
	connector.Init(nil)
	client := &Client{Main: connector, UserLogin: testUserLogin("codex")}
	msg := testMatrixMessage("thread-2", "hello")
	msg.Portal.Metadata = &PortalMetadata{
		Kind:           portalKindSubagent,
		ThreadID:       "thread-2",
		ParentThreadID: "thread-1",
		ReadOnly:       true,
	}

	_, err := client.HandleMatrixMessage(context.Background(), msg)
	if err == nil {
		t.Fatal("expected read-only subagent prompt to be rejected")
	}
	var status bridgev2.MessageStatus
	if !errors.As(err, &status) || status.ErrorReason != event.MessageStatusUnsupported {
		t.Fatalf("expected unsupported message status, got %T: %v", err, err)
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

func TestMatrixMessageStatusForCodexErrorMapsBridgeUnavailable(t *testing.T) {
	err := errors.New("Codex app-server is not running")

	status := matrixMessageStatusForCodexError(err)
	if status.Status != event.MessageStatusFail || status.ErrorReason != event.MessageStatusBridgeUnavailable || status.Message != "Codex bridge is not ready" || !status.IsCertain {
		t.Fatalf("unexpected bridge unavailable status: %#v", status)
	}
}

func TestMatrixMessageStatusForCodexErrorMapsCanceled(t *testing.T) {
	status := matrixMessageStatusForCodexError(context.Canceled)
	if status.Status != event.MessageStatusFail || status.ErrorReason != event.MessageStatusBridgeUnavailable || status.Message != "Codex request was cancelled" || !status.IsCertain {
		t.Fatalf("unexpected canceled status: %#v", status)
	}
}

func TestIsCodexCanceledError(t *testing.T) {
	if !isCodexCanceledError(context.Canceled) {
		t.Fatal("context cancellation should match")
	}
	if isCodexCanceledError(context.DeadlineExceeded) {
		t.Fatal("deadline exceeded should not match cancellation")
	}
}

func TestIsCodexBridgeUnavailableError(t *testing.T) {
	if !isCodexBridgeUnavailableError("codex app-server is not running") {
		t.Fatal("expected bridge-unavailable hint to match")
	}
	if isCodexBridgeUnavailableError("permission denied") {
		t.Fatal("permission errors should not match bridge-unavailable hints")
	}
}

func TestMatrixMessageStatusForCodexErrorMapsPermission(t *testing.T) {
	err := errors.New("permission denied while starting Codex")

	status := matrixMessageStatusForCodexError(err)
	if status.Status != event.MessageStatusFail || status.ErrorReason != event.MessageStatusNoPermission || status.Message != err.Error() || !status.IsCertain {
		t.Fatalf("unexpected permission status: %#v", status)
	}
}

func TestIsCodexPermissionError(t *testing.T) {
	if !isCodexPermissionError("unauthorized request") {
		t.Fatal("expected permission hint to match")
	}
	if isCodexPermissionError("codex app-server is not running") {
		t.Fatal("bridge-unavailable errors should not match permission hints")
	}
}

func TestIsCodexUnsupportedError(t *testing.T) {
	if !isCodexUnsupportedError("invalid project directory") {
		t.Fatal("expected unsupported hint to match")
	}
	if isCodexUnsupportedError("permission denied") {
		t.Fatal("permission errors should not match unsupported hints")
	}
}

func TestMatrixMessageStatusForCodexErrorMapsTimeout(t *testing.T) {
	err := context.DeadlineExceeded

	status := matrixMessageStatusForCodexError(err)
	if status.Status != event.MessageStatusRetriable || status.ErrorReason != event.MessageStatusNetworkError || status.Message != "Codex request timed out" || !status.SendNotice {
		t.Fatalf("unexpected timeout status: %#v", status)
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

func TestHandleMatrixMessageAcksNewProjectDirectoryBeforeAppServerWork(t *testing.T) {
	connector := &Connector{}
	connector.Init(nil)
	client := &Client{Main: connector, UserLogin: testUserLogin("sh-codex")}
	cwd := t.TempDir()
	msg := &bridgev2.MatrixMessage{MatrixEventBase: bridgev2.MatrixEventBase[*event.MessageEventContent]{
		Event: &event.Event{
			ID:        "$event",
			Timestamp: 1000,
		},
		Content: &event.MessageEventContent{
			MsgType: event.MsgText,
			Body:    cwd,
		},
		Portal: &bridgev2.Portal{Portal: &database.Portal{
			PortalKey: newProjectPortalKey("sh-codex"),
			Metadata:  &PortalMetadata{},
		}},
	}}

	resp, err := client.HandleMatrixMessage(context.Background(), msg)
	if err != nil {
		t.Fatalf("directory selection should ack before app-server work: %v", err)
	}
	if resp.PostSave == nil {
		t.Fatal("directory selection should schedule async thread start after DB save")
	}
	meta, _ := resp.DB.Metadata.(*MessageMetadata)
	if meta == nil || meta.Role != "command" || meta.StreamStatus != "directory" || meta.ThreadID != "" {
		t.Fatalf("directory selection should ack as command metadata before thread exists: resp=%#v meta=%#v", resp, meta)
	}
}

func TestHandleMatrixMessageDefersTurnStartUntilMessageSave(t *testing.T) {
	ctx := context.Background()
	matrix := &streamStartMatrix{publisher: &recordingBeeperStreamPublisher{}, eventID: "$deterministic:example.com"}
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
	connector.rememberThreadRoom("thread-1", client, key, cwd, nil)
	connector.rememberWarmThread("thread-1")
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
			Body:    "hello after save",
		},
		Portal: portal,
	}}

	resp, err := client.HandleMatrixMessage(ctx, msg)
	if err != nil {
		t.Fatal(err)
	}
	if resp.PostSave == nil {
		t.Fatal("normal prompt should start Codex from PostSave after DB insert")
	}
	if _, err = os.Stat(logPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("app-server work started before message save: stat err=%v requests=%#v", err, readFakeAppServerRequestsIfExists(t, logPath))
	}
	if err = br.DB.Message.Insert(ctx, resp.DB); err != nil {
		t.Fatal(err)
	}
	resp.PostSave(ctx, resp.DB)

	requireEventually(t, time.Second, func() bool {
		return countFakeAppServerRequests(readFakeAppServerRequestsIfExists(t, logPath), "turn/start") == 1
	})
}

func TestHandleMatrixMessageDefersTurnSteerUntilMessageSave(t *testing.T) {
	ctx := context.Background()
	connector, br := testBridgeWithDB(t, &fakeMatrixConnector{})
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
	portal, err := br.GetExistingPortalByKey(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	run := newActiveRun(client, key, "thread-1", "turn-1")
	connector.setActive("thread-1", run)
	msg := &bridgev2.MatrixMessage{MatrixEventBase: bridgev2.MatrixEventBase[*event.MessageEventContent]{
		Event: &event.Event{
			ID:        "$matrix-event",
			Timestamp: 1000,
		},
		Content: &event.MessageEventContent{
			MsgType: event.MsgText,
			Body:    "steer after save",
		},
		Portal: portal,
	}}

	resp, err := client.HandleMatrixMessage(ctx, msg)
	if err != nil {
		t.Fatal(err)
	}
	if resp.PostSave == nil {
		t.Fatal("steer should be sent from PostSave after DB insert")
	}
	if _, err = os.Stat(logPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("app-server steer started before message save: stat err=%v requests=%#v", err, readFakeAppServerRequestsIfExists(t, logPath))
	}
	if err = br.DB.Message.Insert(ctx, resp.DB); err != nil {
		t.Fatal(err)
	}
	resp.PostSave(ctx, resp.DB)

	requireEventually(t, time.Second, func() bool {
		return countFakeAppServerRequests(readFakeAppServerRequestsIfExists(t, logPath), "turn/steer") == 1
	})
}

func TestHandleMatrixEditRollsBackDeletesFollowingMessagesAndStartsReplacementTurn(t *testing.T) {
	ctx := context.Background()
	matrixAPI := &fakeMatrixAPI{}
	connector, br := testBridgeWithDB(t, &fakeMatrixConnector{api: matrixAPI})
	logPath := filepath.Join(t.TempDir(), "fake-appserver.jsonl")
	app := startTestFakeAppServer(t, ctx, connector, logPath)
	defer app.Close()

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
	portal, err := br.GetExistingPortalByKey(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	target := &database.Message{
		ID:        "user:$prompt-1",
		PartID:    partID("text"),
		MXID:      "$prompt-1",
		Room:      key,
		SenderID:  client.GetUserID(),
		Timestamp: time.Unix(1, 0),
		Metadata:  &MessageMetadata{Role: "user", ThreadID: "thread-1", TurnID: "turn-1", StreamStatus: "done"},
	}
	following := []*database.Message{
		{
			ID:        "codex:turn-1:assistant",
			PartID:    partID("text"),
			MXID:      "$assistant-1",
			Room:      key,
			SenderID:  codexUserID,
			Timestamp: time.Unix(2, 0),
			Metadata:  &MessageMetadata{Role: "assistant", ThreadID: "thread-1", TurnID: "turn-1", StreamStatus: "complete"},
		},
		{
			ID:        "user:$prompt-2",
			PartID:    partID("text"),
			MXID:      "$prompt-2",
			Room:      key,
			SenderID:  client.GetUserID(),
			Timestamp: time.Unix(3, 0),
			Metadata:  &MessageMetadata{Role: "user", ThreadID: "thread-1", TurnID: "turn-2", StreamStatus: "done"},
		},
		{
			ID:        "codex:turn-2:assistant",
			PartID:    partID("text"),
			MXID:      "$assistant-2",
			Room:      key,
			SenderID:  codexUserID,
			Timestamp: time.Unix(4, 0),
			Metadata:  &MessageMetadata{Role: "assistant", ThreadID: "thread-1", TurnID: "turn-2", StreamStatus: "complete"},
		},
	}
	if err = br.DB.Message.Insert(ctx, target); err != nil {
		t.Fatal(err)
	}
	for _, msg := range following {
		if err = br.DB.Message.Insert(ctx, msg); err != nil {
			t.Fatal(err)
		}
	}
	target, err = br.DB.Message.GetPartByID(ctx, key.Receiver, target.ID, target.PartID)
	if err != nil {
		t.Fatal(err)
	}
	edit := &bridgev2.MatrixEdit{
		MatrixEventBase: bridgev2.MatrixEventBase[*event.MessageEventContent]{
			Event: &event.Event{ID: "$edit-1", Timestamp: 5000},
			Content: &event.MessageEventContent{
				MsgType: event.MsgText,
				Body:    "edited prompt",
			},
			Portal: portal,
		},
		EditTarget: target,
	}

	if err = client.HandleMatrixEdit(ctx, edit); err != nil {
		t.Fatal(err)
	}
	if err = br.DB.Message.Update(ctx, edit.EditTarget); err != nil {
		t.Fatal(err)
	}
	requireEventually(t, time.Second, func() bool {
		return countFakeAppServerRequests(readFakeAppServerRequestsIfExists(t, logPath), "turn/start") == 1
	})

	requests := readFakeAppServerRequests(t, logPath)
	if read, ok := findFakeAppServerRequest(requests, "thread/read"); !ok || read.Params["threadId"] != "thread-1" || read.Params["includeTurns"] != true {
		t.Fatalf("thread/read with turns missing or bad: req=%#v requests=%#v", read, requests)
	}
	rollback, ok := findFakeAppServerRequest(requests, "thread/rollback")
	if !ok {
		t.Fatalf("thread/rollback request missing: %#v", requests)
	}
	if rollback.Params["threadId"] != "thread-1" || rollback.Params["numTurns"] != float64(2) {
		t.Fatalf("bad thread/rollback params: %#v", rollback.Params)
	}
	start, ok := findFakeAppServerRequest(requests, "turn/start")
	if !ok {
		t.Fatalf("turn/start request missing: %#v", requests)
	}
	if start.Params["clientUserMessageId"] != "$edit-1" || fakeAppServerInputText(start.Params) != "edited prompt" {
		t.Fatalf("replacement turn/start used wrong edit input: %#v", start.Params)
	}
	redactions := 0
	for _, sent := range matrixAPI.messages {
		if sent.Type == event.EventRedaction {
			redactions++
		}
	}
	if redactions != len(following) {
		t.Fatalf("expected stale messages to be redacted, got %d events=%#v", redactions, matrixAPI.messages)
	}
	for _, msg := range following {
		got, err := br.DB.Message.GetPartByID(ctx, key.Receiver, msg.ID, msg.PartID)
		if err != nil {
			t.Fatal(err)
		}
		if got != nil {
			t.Fatalf("stale message was not deleted: %#v", got)
		}
	}
	updated, err := br.DB.Message.GetPartByID(ctx, key.Receiver, target.ID, target.PartID)
	if err != nil {
		t.Fatal(err)
	}
	meta, _ := updated.Metadata.(*MessageMetadata)
	if meta == nil || meta.Role != "user" || meta.ThreadID != "thread-1" || meta.TurnID != "turn-1" || meta.StreamStatus != "edited" {
		t.Fatalf("edited target metadata not preserved: %#v", meta)
	}
}

func TestHandleMatrixEditRejectsNonPromptTargetsBeforeCodexRollback(t *testing.T) {
	ctx := context.Background()
	connector, _ := testBridgeWithDB(t, &fakeMatrixConnector{})
	logPath := filepath.Join(t.TempDir(), "fake-appserver.jsonl")
	app := startTestFakeAppServer(t, ctx, connector, logPath)
	defer app.Close()
	client := &Client{Main: connector, UserLogin: testUserLogin("sh-codex"), loggedIn: true}
	portal := &bridgev2.Portal{Portal: &database.Portal{
		PortalKey: projectPortalKey("/tmp/project", "sh-codex"),
		MXID:      "!room:example.com",
		Metadata:  &PortalMetadata{ThreadID: "thread-1", Cwd: "/tmp/project"},
	}}
	err := client.HandleMatrixEdit(ctx, &bridgev2.MatrixEdit{
		MatrixEventBase: bridgev2.MatrixEventBase[*event.MessageEventContent]{
			Event:   &event.Event{ID: "$edit-1"},
			Content: &event.MessageEventContent{MsgType: event.MsgText, Body: "edited"},
			Portal:  portal,
		},
		EditTarget: &database.Message{Metadata: &MessageMetadata{Role: "assistant", ThreadID: "thread-1", TurnID: "turn-1"}},
	})
	if err == nil {
		t.Fatal("expected assistant edit target to be rejected")
	}
	if requests := readFakeAppServerRequestsIfExists(t, logPath); len(requests) != 0 {
		t.Fatalf("rejected edit should not touch Codex app-server: %#v", requests)
	}
}

func TestHandleMatrixEditRejectsWhenCodexRollbackFails(t *testing.T) {
	ctx := context.Background()
	matrixAPI := &fakeMatrixAPI{}
	connector, br := testBridgeWithDB(t, &fakeMatrixConnector{api: matrixAPI})
	logPath := filepath.Join(t.TempDir(), "fake-appserver.jsonl")
	app := startTestFakeAppServer(t, ctx, connector, logPath)
	defer app.Close()

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
	portal := &bridgev2.Portal{Portal: &database.Portal{
		PortalKey: key,
		MXID:      "!room:example.com",
		Metadata:  &PortalMetadata{ThreadID: "missing-thread", Cwd: "/tmp/project"},
	}}
	target := &database.Message{
		ID:        "user:$prompt-1",
		PartID:    partID("text"),
		MXID:      "$prompt-1",
		Room:      key,
		SenderID:  client.GetUserID(),
		Timestamp: time.Unix(1, 0),
		Metadata:  &MessageMetadata{Role: "user", ThreadID: "missing-thread", TurnID: "turn-1", StreamStatus: "done"},
	}
	following := &database.Message{
		ID:        "codex:turn-1:assistant",
		PartID:    partID("text"),
		MXID:      "$assistant-1",
		Room:      key,
		SenderID:  codexUserID,
		Timestamp: time.Unix(2, 0),
		Metadata:  &MessageMetadata{Role: "assistant", ThreadID: "missing-thread", TurnID: "turn-1", StreamStatus: "complete"},
	}
	for _, msg := range []*database.Message{target, following} {
		if err = br.DB.Message.Insert(ctx, msg); err != nil {
			t.Fatal(err)
		}
	}

	err = client.HandleMatrixEdit(ctx, &bridgev2.MatrixEdit{
		MatrixEventBase: bridgev2.MatrixEventBase[*event.MessageEventContent]{
			Event:   &event.Event{ID: "$edit-1"},
			Content: &event.MessageEventContent{MsgType: event.MsgText, Body: "edited prompt"},
			Portal:  portal,
		},
		EditTarget: target,
	})
	if err == nil {
		t.Fatal("expected Codex rollback failure to reject edit")
	}
	requests := readFakeAppServerRequests(t, logPath)
	if read, ok := findFakeAppServerRequest(requests, "thread/read"); !ok || read.Params["threadId"] != "missing-thread" {
		t.Fatalf("thread/read failure path missing or bad: req=%#v requests=%#v", read, requests)
	}
	if _, ok := findFakeAppServerRequest(requests, "thread/rollback"); ok {
		t.Fatalf("failed thread/read should not roll back: %#v", requests)
	}
	if _, ok := findFakeAppServerRequest(requests, "turn/start"); ok {
		t.Fatalf("rejected edit should not start replacement turn: %#v", requests)
	}
	redactions := 0
	for _, sent := range matrixAPI.messages {
		if sent.Type == event.EventRedaction {
			redactions++
		}
	}
	if redactions != 0 {
		t.Fatalf("rejected edit should not redact stale messages, got %d events=%#v", redactions, matrixAPI.messages)
	}
	got, err := br.DB.Message.GetPartByID(ctx, key.Receiver, following.ID, following.PartID)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatal("rejected edit deleted stale message before Codex accepted rollback")
	}
}

func TestHandleMatrixDeleteChatDetachesWithoutDeletingCodexSession(t *testing.T) {
	ctx := context.Background()
	connector := &Connector{
		active:      map[string]*activeRun{},
		threadRooms: map[string]threadRoom{},
		processes:   map[string]*activeRun{},
		warmThreads: map[string]struct{}{"thread-1": {}},
	}
	login := testUserLogin("sh-codex")
	client := &Client{Main: connector, UserLogin: login}
	key := projectPortalKey("/tmp/project", login.ID)
	run := newActiveRun(client, key, "thread-1", "turn-1")
	connector.setActive("thread-1", run)
	connector.rememberThreadRoom("thread-1", client, key, "/tmp/project", nil)
	connector.rememberProcess("proc-1", run)

	err := client.HandleMatrixDeleteChat(ctx, &bridgev2.MatrixDeleteChat{
		Event: &event.Event{ID: "$delete"},
		Portal: &bridgev2.Portal{Portal: &database.Portal{
			PortalKey: key,
			MXID:      "!room:example.com",
			Metadata:  &PortalMetadata{ThreadID: "thread-1", Cwd: "/tmp/project"},
		}},
		Content: &event.BeeperChatDeleteEventContent{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if connector.activeRun("thread-1") != nil {
		t.Fatal("deleted Matrix chat should stop active bridging")
	}
	if _, ok := connector.threadRoom("thread-1"); ok {
		t.Fatal("deleted Matrix chat should remove thread room mapping")
	}
	if run := connector.activeRunForProcess([]byte(`{"processId":"proc-1"}`)); run != nil {
		t.Fatal("deleted Matrix chat should remove process bridge mapping")
	}
	if _, warm := connector.warmThreads["thread-1"]; warm {
		t.Fatal("deleted Matrix chat should forget warm thread state")
	}
}

func TestDirectorySelectionPostSaveStartsThreadAndCanonicalizesMessage(t *testing.T) {
	ctx := context.Background()
	connector, br := testBridgeWithDB(t, &fakeMatrixConnector{})
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
	starterKey := newProjectPortalKey(login.ID)
	if err = br.DB.Portal.Insert(ctx, &database.Portal{
		PortalKey: starterKey,
		MXID:      "!starter:example.com",
		Name:      "New Project",
		RoomType:  database.RoomTypeDM,
		Metadata:  &PortalMetadata{},
	}); err != nil {
		t.Fatal(err)
	}
	portal, err := br.GetExistingPortalByKey(ctx, starterKey)
	if err != nil {
		t.Fatal(err)
	}
	msg := &bridgev2.MatrixMessage{MatrixEventBase: bridgev2.MatrixEventBase[*event.MessageEventContent]{
		Event: &event.Event{
			ID:        "$directory-event",
			Timestamp: 1000,
		},
		Content: &event.MessageEventContent{
			MsgType: event.MsgText,
			Body:    cwd,
		},
		Portal: portal,
	}}

	resp, err := client.HandleMatrixMessage(ctx, msg)
	if err != nil {
		t.Fatal(err)
	}
	if err = br.DB.Message.Insert(ctx, resp.DB); err != nil {
		t.Fatal(err)
	}
	resp.PostSave(ctx, resp.DB)

	cleanCWD, err := cleanProjectDir(cwd)
	if err != nil {
		t.Fatal(err)
	}
	projectKey := projectPortalKey(cleanCWD, login.ID)
	requireEventually(t, time.Second, func() bool {
		projectPortal, err := br.GetExistingPortalByKey(ctx, projectKey)
		if err != nil || projectPortal == nil || projectPortal.MXID != "!starter:example.com" {
			return false
		}
		meta := portalMetadata(projectPortal.Metadata)
		return meta.ThreadID != "" && meta.Cwd == cleanCWD
	})
	requireEventually(t, time.Second, func() bool {
		dbMsg, err := br.DB.Message.GetPartByID(ctx, login.ID, resp.DB.ID, resp.DB.PartID)
		if err != nil {
			return false
		}
		meta, _ := dbMsg.Metadata.(*MessageMetadata)
		return dbMsg.Room == projectKey && meta != nil && meta.Role == "command" && meta.ThreadID != "" && meta.StreamStatus == "directory"
	})
	requests := readFakeAppServerRequests(t, logPath)
	if got := countFakeAppServerRequests(requests, "thread/start"); got != 1 {
		t.Fatalf("directory selection should start exactly one thread, got %d requests=%#v", got, requests)
	}
	if got := countFakeAppServerRequests(requests, "turn/start"); got != 0 {
		t.Fatalf("directory selection should not consume the path as a prompt, got %d turn/start requests=%#v", got, requests)
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
	connector.rememberThreadRoom("thread-1", client, key, cwd, map[string]any{"modelProvider": "openai", "model": "gpt-5.5", "reasoningEffort": "high"})
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
	if resp.DB.Room != key || resp.DB.SenderID != networkid.UserID("login:sh-codex") || meta == nil || meta.Role != "user" || meta.ThreadID != "thread-1" || meta.TurnID != "" || meta.StreamStatus != "done" {
		t.Fatalf("unexpected Matrix response DB: db=%#v meta=%#v", resp.DB, meta)
	}
	if err = br.DB.Message.Insert(ctx, resp.DB); err != nil {
		t.Fatal(err)
	}
	resp.PostSave(ctx, resp.DB)
	requireEventually(t, time.Second, func() bool {
		run := connector.activeRun("thread-1")
		return run != nil && run.turnID == "turn-1"
	})
	requireEventually(t, time.Second, func() bool {
		publisher.mu.Lock()
		defer publisher.mu.Unlock()
		return publisher.roomID == portal.MXID && publisher.eventID != "" && len(publisher.updates) > 0
	})
	requireEventually(t, time.Second, func() bool {
		dbMsg, err := br.DB.Message.GetPartByID(ctx, key.Receiver, resp.DB.ID, resp.DB.PartID)
		if err != nil {
			return false
		}
		dbMeta, _ := dbMsg.Metadata.(*MessageMetadata)
		return dbMeta != nil && dbMeta.TurnID == "turn-1"
	})

	requireEventually(t, time.Second, func() bool {
		if _, err := os.Stat(logPath); err != nil {
			return false
		}
		requests := readFakeAppServerRequests(t, logPath)
		_, resumeOK := findFakeAppServerRequest(requests, "thread/resume")
		_, startOK := findFakeAppServerRequest(requests, "turn/start")
		return resumeOK && startOK
	})
	requests := readFakeAppServerRequests(t, logPath)
	resume, ok := findFakeAppServerRequest(requests, "thread/resume")
	if !ok {
		t.Fatalf("thread/resume request missing: %#v", requests)
	}
	cleanCWD, err := cleanProjectDir(cwd)
	if err != nil {
		t.Fatal(err)
	}
	if resume.Params["threadId"] != "thread-1" || resume.Params["cwd"] != cleanCWD || resume.Params["approvalPolicy"] != "on-request" {
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

func TestHandleMatrixMessageSkipsResumeForWarmThread(t *testing.T) {
	ctx := context.Background()
	publisher := &recordingBeeperStreamPublisher{}
	matrix := &streamStartMatrix{publisher: publisher, eventID: "$deterministic:example.com"}
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
	connector.rememberThreadRoom("thread-1", client, key, cwd, nil)
	connector.rememberWarmThread("thread-1")
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
			Body:    "hello warm thread",
		},
		Portal: portal,
	}}

	resp, err := client.HandleMatrixMessage(ctx, msg)
	if err != nil {
		t.Fatal(err)
	}
	if err = br.DB.Message.Insert(ctx, resp.DB); err != nil {
		t.Fatal(err)
	}
	resp.PostSave(ctx, resp.DB)
	requireEventually(t, time.Second, func() bool {
		if _, err := os.Stat(logPath); err != nil {
			return false
		}
		requests := readFakeAppServerRequests(t, logPath)
		_, ok := findFakeAppServerRequest(requests, "turn/start")
		return ok
	})
	requests := readFakeAppServerRequests(t, logPath)
	if _, ok := findFakeAppServerRequest(requests, "thread/resume"); ok {
		t.Fatalf("warm thread should not be resumed before turn/start: %#v", requests)
	}
}

func TestHandleMatrixMessageDeduplicatesRetryWhileTurnStartInFlight(t *testing.T) {
	ctx := context.Background()
	matrix := &streamStartMatrix{publisher: &recordingBeeperStreamPublisher{}, eventID: "$deterministic:example.com"}
	connector, br := testBridgeWithDB(t, matrix)
	logPath := filepath.Join(t.TempDir(), "fake-appserver.jsonl")
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	app, err := appserver.Start(ctx, exe, map[string]string{
		fakeAppServerEnv:      "1",
		fakeAppServerLogEnv:   logPath,
		fakeAppServerDelayEnv: "150",
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
	connector.rememberThreadRoom("thread-1", client, key, cwd, nil)
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
			ID:        "$retry-event",
			Timestamp: 1000,
		},
		Content: &event.MessageEventContent{
			MsgType: event.MsgText,
			Body:    "hello once",
		},
		Portal: portal,
	}}

	resp, err := client.HandleMatrixMessage(ctx, msg)
	if err != nil {
		t.Fatal(err)
	}
	if err = br.DB.Message.Insert(ctx, resp.DB); err != nil {
		t.Fatal(err)
	}
	resp.PostSave(ctx, resp.DB)
	duplicate, err := client.HandleMatrixMessage(ctx, msg)
	if err != nil {
		t.Fatal(err)
	}
	if duplicate == nil || duplicate.DB == nil || duplicate.DB.ID != resp.DB.ID {
		t.Fatalf("duplicate Matrix retry should ack the same user message ID: first=%#v duplicate=%#v", resp, duplicate)
	}

	requireEventually(t, 2*time.Second, func() bool {
		if _, err := os.Stat(logPath); err != nil {
			return false
		}
		requests := readFakeAppServerRequests(t, logPath)
		return countFakeAppServerRequests(requests, "thread/resume") == 1 && countFakeAppServerRequests(requests, "turn/start") == 1
	})
	requests := readFakeAppServerRequests(t, logPath)
	if got := countFakeAppServerRequests(requests, "thread/resume"); got != 1 {
		t.Fatalf("Matrix retry started duplicate thread/resume requests: got %d requests=%#v", got, requests)
	}
	if got := countFakeAppServerRequests(requests, "turn/start"); got != 1 {
		t.Fatalf("Matrix retry started duplicate turn/start requests: got %d requests=%#v", got, requests)
	}
}

func TestHandleMatrixMessageSteersActiveTurnWithTurnMetadata(t *testing.T) {
	ctx := context.Background()
	connector, br := testBridgeWithDB(t, &fakeMatrixConnector{})
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
	if err = br.DB.Portal.Insert(ctx, &database.Portal{
		PortalKey: msg.Portal.PortalKey,
		MXID:      "!room:example.com",
		Name:      "project",
		RoomType:  database.RoomTypeDM,
		Metadata:  &PortalMetadata{ThreadID: "thread-1", Cwd: "/tmp/project"},
	}); err != nil {
		t.Fatal(err)
	}
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
	if err = br.DB.Message.Insert(ctx, resp.DB); err != nil {
		t.Fatal(err)
	}
	resp.PostSave(ctx, resp.DB)

	requireEventually(t, time.Second, func() bool {
		if _, err := os.Stat(logPath); err != nil {
			return false
		}
		requests := readFakeAppServerRequests(t, logPath)
		_, ok := findFakeAppServerRequest(requests, "turn/steer")
		return ok
	})
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
	requireEventually(t, time.Second, func() bool {
		return hasCodexRunStateDelta(run.run.Events, "turn/steer", "expectedTurnId", "turn-1") &&
			hasCodexRunStateDelta(run.run.Events, "turn/steer", "clientUserMessageId", "$event")
	})
}

func TestHandleMatrixMessageStartsNewTurnAfterStaleSteer(t *testing.T) {
	ctx := context.Background()
	connector, br := testBridgeWithDB(t, &fakeMatrixConnector{})
	logPath := filepath.Join(t.TempDir(), "fake-appserver.jsonl")
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	app, err := appserver.Start(ctx, exe, map[string]string{
		fakeAppServerEnv:              "1",
		fakeAppServerLogEnv:           logPath,
		fakeAppServerNoActiveSteerEnv: "1",
		fakeAppServerTurnStartIDEnv:   "turn-2",
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
	msg := testMatrixMessage("thread-1", "recover this")
	msg.Portal.PortalKey = projectPortalKey("/tmp/project", login.ID)
	if err = br.DB.Portal.Insert(ctx, &database.Portal{
		PortalKey: msg.Portal.PortalKey,
		MXID:      "!room:example.com",
		Name:      "project",
		RoomType:  database.RoomTypeDM,
		Metadata:  &PortalMetadata{ThreadID: "thread-1", Cwd: "/tmp/project"},
	}); err != nil {
		t.Fatal(err)
	}
	staleRun := newActiveRun(client, msg.Portal.PortalKey, "thread-1", "turn-1")
	connector.setActive("thread-1", staleRun)

	resp, err := client.HandleMatrixMessage(ctx, msg)
	if err != nil {
		t.Fatal(err)
	}
	if resp == nil || resp.DB == nil {
		t.Fatalf("missing Matrix response: %#v", resp)
	}
	if err = br.DB.Message.Insert(ctx, resp.DB); err != nil {
		t.Fatal(err)
	}
	resp.PostSave(ctx, resp.DB)

	requireEventually(t, time.Second, func() bool {
		requests := readFakeAppServerRequestsIfExists(t, logPath)
		return countFakeAppServerRequests(requests, "turn/steer") == 1 &&
			countFakeAppServerRequests(requests, "turn/start") == 1
	})
	if active := connector.activeRun("thread-1"); active != nil && active.turnID == "turn-1" {
		t.Fatalf("stale active run was not cleared: %#v", active)
	}
}

func TestInterruptTurnSyncsClientRequestState(t *testing.T) {
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
	msg := testMatrixMessage("thread-1", "stop")
	run := newActiveRun(client, msg.Portal.PortalKey, "thread-1", "turn-1")
	connector.setActive("thread-1", run)

	client.interruptTurn(ctx, msg.Portal, "thread-1", "turn-1")

	requireEventually(t, time.Second, func() bool {
		if _, err := os.Stat(logPath); err != nil {
			return false
		}
		requests := readFakeAppServerRequests(t, logPath)
		_, ok := findFakeAppServerRequest(requests, "turn/interrupt")
		return ok
	})
	if !hasCodexRunStateDelta(run.run.Events, "turn/interrupt", "turnId", "turn-1") {
		t.Fatalf("turn/interrupt client request was not synced to AG-UI state: %#v", run.run.Events)
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

	run, err := client.startTurn(ctx, key, "thread-1", "$matrix-event", "hello")
	if err != nil {
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
	if !hasCodexRunStateDelta(run.run.Events, "turn/start", "clientUserMessageId", "$matrix-event") {
		t.Fatalf("turn/start client request was not synced to AG-UI state: %#v", run.run.Events)
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

func TestClientConnectReconcilesOnlyItsLoginPortals(t *testing.T) {
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
	client := login.Client.(*Client)
	t.Cleanup(client.Disconnect)

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
	otherKey := projectPortalKey("/tmp/other", "other-login")
	if err = br.DB.Portal.Insert(ctx, &database.Portal{PortalKey: otherKey}); err != nil {
		t.Fatal(err)
	}

	client.Connect(ctx)

	targetKey := projectPortalKey("/tmp/project", login.ID)
	if portal, err := br.GetExistingPortalByKey(ctx, targetKey); err != nil {
		t.Fatal(err)
	} else if portal == nil || portal.MXID != "!room:example.com" {
		t.Fatalf("login reconnect did not canonicalize its portal: %#v", portal)
	}
	if portal, err := br.GetExistingPortalByKey(ctx, otherKey); err != nil {
		t.Fatal(err)
	} else if portal == nil {
		t.Fatal("login reconnect should not reconcile another login's portal")
	}
	task, err := br.DB.BackfillTask.GetNextForPortal(ctx, targetKey, true)
	if err != nil {
		t.Fatal(err)
	}
	if task == nil || task.BatchCount != -1 || task.IsDone || task.QueueDone {
		t.Fatalf("login reconnect should reset backfill for reconciled portal: %#v", task)
	}
}

func TestClientConnectMaterializesDefaultNewProjectRoom(t *testing.T) {
	ctx := context.Background()
	oldPortalEventBuffer := bridgev2.PortalEventBuffer
	bridgev2.PortalEventBuffer = 0
	t.Cleanup(func() { bridgev2.PortalEventBuffer = oldPortalEventBuffer })

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
	t.Cleanup(login.Client.Disconnect)

	key := defaultNewProjectPortalKey(login.ID)
	portal, err := br.GetExistingPortalByKey(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if portal == nil || portal.MXID == "" || portal.RoomType != database.RoomTypeDM {
		t.Fatalf("login connect did not materialize the default New Project room: %#v", portal)
	}
	if portal.Name != "New Project" || portal.Topic != newProjectPrompt {
		t.Fatalf("default New Project room has wrong info: %#v", portal.Portal)
	}
	requireEventually(t, time.Second, func() bool { return len(matrix.intent().messages) == 1 })
	msg := matrix.intent().messages[0]
	content, ok := msg.Content.Parsed.(*event.MessageEventContent)
	if msg.RoomID != portal.MXID || !ok || !strings.Contains(content.Body, "Send a project directory path") || !strings.Contains(content.Body, "/approvals") {
		t.Fatalf("default New Project room intro was not queued: %#v", msg)
	}
}

func TestClientConnectDoesNotCreateDefaultNewProjectRoomWhenStarterExists(t *testing.T) {
	ctx := context.Background()
	oldPortalEventBuffer := bridgev2.PortalEventBuffer
	bridgev2.PortalEventBuffer = 0
	t.Cleanup(func() { bridgev2.PortalEventBuffer = oldPortalEventBuffer })

	matrix := &fakeMatrixConnector{}
	connector, br := testBridgeWithDB(t, matrix)
	existingKey := newProjectPortalKey("sh-codex")
	if err := br.DB.Portal.Insert(ctx, &database.Portal{
		PortalKey: existingKey,
		MXID:      "!existing-starter:example.com",
		Name:      "New Project",
		RoomType:  database.RoomTypeDM,
		Metadata:  &PortalMetadata{NewProjectIntroMessage: true},
	}); err != nil {
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
	t.Cleanup(login.Client.Disconnect)

	if portal, err := br.GetExistingPortalByKey(ctx, defaultNewProjectPortalKey(login.ID)); err != nil {
		t.Fatal(err)
	} else if portal != nil && portal.MXID != "" {
		t.Fatalf("connect should not create a second default starter room when one exists: %#v", portal.Portal)
	}
	if len(matrix.intent().messages) != 0 {
		t.Fatalf("existing starter with intro metadata should not queue another intro: %#v", matrix.intent().messages)
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

func TestHydrateThreadRoomsDetachesArchivedCodexThread(t *testing.T) {
	ctx := context.Background()
	oldPortalEventBuffer := bridgev2.PortalEventBuffer
	bridgev2.PortalEventBuffer = 0
	t.Cleanup(func() { bridgev2.PortalEventBuffer = oldPortalEventBuffer })

	connector, br := testBridgeWithDB(t, &fakeMatrixConnector{})
	logPath := filepath.Join(t.TempDir(), "fake-appserver.jsonl")
	app := startTestFakeAppServer(t, ctx, connector, logPath)
	defer app.Close()
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
		Metadata:  &PortalMetadata{ThreadID: "archived-thread", Cwd: "/tmp/project"},
	}); err != nil {
		t.Fatal(err)
	}

	connector.hydrateThreadRooms(ctx)

	requireEventually(t, time.Second, func() bool {
		portal, err := br.GetExistingPortalByKey(ctx, key)
		return err == nil && portal == nil
	})
	if _, ok := connector.threadRoom("archived-thread"); ok {
		t.Fatal("archived thread should not remain cached after startup reconciliation")
	}
	requests := readFakeAppServerRequests(t, logPath)
	if read, ok := findFakeAppServerRequest(requests, "thread/read"); !ok || read.Params["threadId"] != "archived-thread" {
		t.Fatalf("startup should read Codex thread before detaching: req=%#v requests=%#v", read, requests)
	}
}

func TestHydrateThreadRoomsKeepsSubagentPortalReadOnly(t *testing.T) {
	ctx := context.Background()
	connector, br := testBridgeWithDB(t, &fakeMatrixConnector{})
	logPath := filepath.Join(t.TempDir(), "fake-appserver.jsonl")
	app := startTestFakeAppServer(t, ctx, connector, logPath)
	defer app.Close()
	user, err := br.GetUserByMXID(ctx, "@alice:example.com")
	if err != nil {
		t.Fatal(err)
	}
	login, err := connector.ensureLoginID(ctx, user, "sh-codex", "alice@example.com")
	if err != nil {
		t.Fatal(err)
	}
	cwd := os.TempDir()
	key := subagentPortalKey("thread-2", login.ID)
	if err = br.DB.Portal.Insert(ctx, &database.Portal{
		PortalKey: key,
		MXID:      "!subagent:example.com",
		Name:      "Subagent thread-2",
		RoomType:  database.RoomTypeDM,
		Metadata: &PortalMetadata{
			Kind:           portalKindSubagent,
			ThreadID:       "thread-2",
			ParentThreadID: "thread-1",
			Cwd:            cwd,
			ReadOnly:       true,
		},
	}); err != nil {
		t.Fatal(err)
	}

	connector.hydrateThreadRooms(ctx)

	portal, err := br.GetExistingPortalByKey(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if portal == nil || portal.PortalKey != key {
		t.Fatalf("subagent portal should not be canonicalized into a project portal: %#v", portal)
	}
	meta := portalMetadata(portal.Metadata)
	if meta.Kind != portalKindSubagent || meta.ThreadID != "thread-2" || meta.ParentThreadID != "thread-1" || !meta.ReadOnly {
		t.Fatalf("subagent metadata was not preserved: %#v", meta)
	}
	if _, ok := connector.threadRoom("thread-2"); !ok {
		t.Fatal("subagent thread should be cached after startup reconciliation")
	}
}

func TestHydrateThreadRoomsDetachesMissingSubagentPortal(t *testing.T) {
	ctx := context.Background()
	oldPortalEventBuffer := bridgev2.PortalEventBuffer
	bridgev2.PortalEventBuffer = 0
	t.Cleanup(func() { bridgev2.PortalEventBuffer = oldPortalEventBuffer })

	connector, br := testBridgeWithDB(t, &fakeMatrixConnector{})
	logPath := filepath.Join(t.TempDir(), "fake-appserver.jsonl")
	app := startTestFakeAppServer(t, ctx, connector, logPath)
	defer app.Close()
	user, err := br.GetUserByMXID(ctx, "@alice:example.com")
	if err != nil {
		t.Fatal(err)
	}
	login, err := connector.ensureLoginID(ctx, user, "sh-codex", "alice@example.com")
	if err != nil {
		t.Fatal(err)
	}
	key := subagentPortalKey("missing-thread", login.ID)
	if err = br.DB.Portal.Insert(ctx, &database.Portal{
		PortalKey: key,
		MXID:      "!subagent:example.com",
		Name:      "Subagent missing-thread",
		RoomType:  database.RoomTypeDM,
		Metadata: &PortalMetadata{
			Kind:           portalKindSubagent,
			ThreadID:       "missing-thread",
			ParentThreadID: "thread-1",
			Cwd:            "/tmp/project",
			ReadOnly:       true,
		},
	}); err != nil {
		t.Fatal(err)
	}

	connector.hydrateThreadRooms(ctx)

	requireEventually(t, time.Second, func() bool {
		portal, err := br.GetExistingPortalByKey(ctx, key)
		return err == nil && portal == nil
	})
}

func TestFetchMessagesBackfillsSubagentThreadOnly(t *testing.T) {
	ctx := context.Background()
	connector, br := testBridgeWithDB(t, &fakeMatrixConnector{})
	logPath := filepath.Join(t.TempDir(), "fake-appserver.jsonl")
	app := startTestFakeAppServer(t, ctx, connector, logPath)
	defer app.Close()
	user, err := br.GetUserByMXID(ctx, "@alice:example.com")
	if err != nil {
		t.Fatal(err)
	}
	login, err := connector.ensureLoginID(ctx, user, "sh-codex", "alice@example.com")
	if err != nil {
		t.Fatal(err)
	}
	key := subagentPortalKey("thread-2", login.ID)
	if err = br.DB.Portal.Insert(ctx, &database.Portal{
		PortalKey: key,
		MXID:      "!subagent:example.com",
		RoomType:  database.RoomTypeDM,
		Metadata: &PortalMetadata{
			Kind:           portalKindSubagent,
			ThreadID:       "thread-2",
			ParentThreadID: "thread-1",
			Cwd:            "/tmp/project",
			ReadOnly:       true,
		},
	}); err != nil {
		t.Fatal(err)
	}
	portal, err := br.GetExistingPortalByKey(ctx, key)
	if err != nil {
		t.Fatal(err)
	}

	_, err = login.Client.(*Client).FetchMessages(ctx, bridgev2.FetchMessagesParams{Portal: portal, Count: 10})
	if err != nil {
		t.Fatal(err)
	}

	requests := readFakeAppServerRequests(t, logPath)
	if _, ok := findFakeAppServerRequest(requests, "thread/list"); ok {
		t.Fatalf("subagent backfill should not list/backfill the entire cwd: %#v", requests)
	}
	if read, ok := findFakeAppServerRequest(requests, "thread/read"); !ok || read.Params["threadId"] != "thread-2" {
		t.Fatalf("subagent backfill should read its own thread: req=%#v requests=%#v", read, requests)
	}
}

func TestHydrateThreadRoomsCleansStaleRoomFeaturePowerLevels(t *testing.T) {
	ctx := context.Background()
	matrix := &fakeMatrixConnector{powerLevels: &event.PowerLevelsEventContent{
		Events: map[string]int{
			event.StateBeeperDisappearingTimer.Type:       0,
			event.StateMSC4391BotCommand.Type:             0,
			roomStateEventType(codexThreadStateType).Type: 0,
			event.StateRoomName.Type:                      0,
		},
		Users: map[id.UserID]int{},
	}}
	connector, br := testBridgeWithDB(t, matrix)
	user, err := br.GetUserByMXID(ctx, "@alice:example.com")
	if err != nil {
		t.Fatal(err)
	}
	login, err := connector.ensureLoginID(ctx, user, "sh-codex", "alice@example.com")
	if err != nil {
		t.Fatal(err)
	}
	key := newProjectPortalKey(login.ID)
	if err = br.DB.Portal.Insert(ctx, &database.Portal{
		PortalKey: key,
		MXID:      "!starter:example.com",
		Name:      "New Project",
		RoomType:  database.RoomTypeDM,
		Metadata:  &PortalMetadata{},
	}); err != nil {
		t.Fatal(err)
	}

	connector.hydrateThreadRooms(ctx)

	state := findFakeState(matrix.intent().states, event.StatePowerLevels.Type)
	if state == nil || state.Content == nil {
		t.Fatalf("expected startup to resend cleaned power levels, got %#v", matrix.intent().states)
	}
	content, _ := state.Content.Parsed.(*event.PowerLevelsEventContent)
	if content == nil {
		t.Fatalf("power level state was not parsed content: %#v", state.Content)
	}
	if _, ok := content.Events[event.StateBeeperDisappearingTimer.Type]; ok {
		t.Fatalf("disappearing timer power level was not removed: %#v", content.Events)
	}
	if _, ok := content.Events[event.StateMSC4391BotCommand.Type]; ok {
		t.Fatalf("command state power level was not removed: %#v", content.Events)
	}
	if _, ok := content.Events[roomStateEventType(codexThreadStateType).Type]; ok {
		t.Fatalf("Codex thread power level was not removed: %#v", content.Events)
	}
	if content.Events[event.StateRoomName.Type] != 0 || content.Events[beeperAIModelStateType] != 0 {
		t.Fatalf("user-editable room state power levels were not preserved/applied: %#v", content.Events)
	}
}

func TestHydrateThreadRoomsQueuesExistingNewProjectIntro(t *testing.T) {
	ctx := context.Background()
	oldPortalEventBuffer := bridgev2.PortalEventBuffer
	bridgev2.PortalEventBuffer = 0
	t.Cleanup(func() { bridgev2.PortalEventBuffer = oldPortalEventBuffer })

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
	requireEventually(t, time.Second, func() bool { return len(matrix.intent().messages) == 1 })
	matrix.intent().messages = nil
	login.Client = &Client{Main: connector, UserLogin: login, loggedIn: true}
	key := newProjectPortalKey(login.ID)
	if err = br.DB.Portal.Insert(ctx, &database.Portal{
		PortalKey: key,
		MXID:      "!starter:example.com",
		Name:      "New Project",
		RoomType:  database.RoomTypeDM,
		Metadata:  &PortalMetadata{},
	}); err != nil {
		t.Fatal(err)
	}

	connector.hydrateThreadRooms(ctx)

	requireEventually(t, time.Second, func() bool { return len(matrix.intent().messages) == 1 })
	msg := matrix.intent().messages[0]
	content, ok := msg.Content.Parsed.(*event.MessageEventContent)
	if msg.RoomID != "!starter:example.com" || !ok || !strings.Contains(content.Body, "import recent Codex context as backfill") || !strings.Contains(content.Body, "/approvals") {
		t.Fatalf("unexpected hydrated intro message: %#v", msg)
	}
	portal, err := br.GetExistingPortalByKey(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if meta := portalMetadata(portal.Metadata); !meta.NewProjectIntroMessage || meta.ThreadID != "" || meta.Cwd != "" {
		t.Fatalf("hydrated intro metadata was not saved: %#v", meta)
	}
	if state := findFakeState(matrix.intent().states, codexThreadStateType); state != nil {
		t.Fatalf("new project hydration should not send placeholder Codex thread state: %#v", state)
	}
	if state := findFakeState(matrix.intent().states, beeperAIModelStateType); state != nil {
		t.Fatalf("new project hydration should not send placeholder Beeper AI model state: %#v", state)
	}
	if state := findFakeState(matrix.intent().states, event.StateMSC4391BotCommand.Type); state == nil {
		t.Fatalf("new project hydration should still send command state: %#v", matrix.intent().states)
	}

	connector.hydrateThreadRooms(ctx)
	time.Sleep(20 * time.Millisecond)
	if len(matrix.intent().messages) != 1 {
		t.Fatalf("hydration intro should only be sent once, got %#v", matrix.intent().messages)
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
	otherKey := networkid.PortalKey{ID: "new:other", Receiver: login.ID}
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
	connector.rememberThreadRoom("thread-1", client, key, "/tmp/project", map[string]any{"modelProvider": "openai", "model": "gpt-5"})
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

func TestSyncPortalBackfillResetsOldCompletedTask(t *testing.T) {
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

	(&Client{Main: connector, UserLogin: login}).syncPortalBackfill(ctx, portal, "thread-1")

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

func TestSyncPortalBackfillKeepsCurrentTask(t *testing.T) {
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
		Metadata:  &PortalMetadata{ThreadID: "thread-1", Cwd: "/tmp/project", BackfillVersion: codexBackfillVersion},
	}); err != nil {
		t.Fatal(err)
	}
	if err = br.DB.BackfillTask.Upsert(ctx, &database.BackfillTask{
		PortalKey:   key,
		UserLoginID: login.ID,
		BatchCount:  3,
	}); err != nil {
		t.Fatal(err)
	}
	portal, err := br.GetExistingPortalByKey(ctx, key)
	if err != nil {
		t.Fatal(err)
	}

	(&Client{Main: connector, UserLogin: login}).syncPortalBackfill(ctx, portal, "thread-1")

	task, err := br.DB.BackfillTask.GetNextForPortal(ctx, key, true)
	if err != nil {
		t.Fatal(err)
	}
	if task == nil || task.BatchCount != 3 {
		t.Fatalf("current backfill task should not be reset: %#v", task)
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
	if got := connector.loginIDForUser(); got != "sh-codex" {
		t.Fatalf("unexpected login ID: got %q, want sh-codex", got)
	}
}

func TestLoginRemoteProfileIncludesHostAndBridgeAlias(t *testing.T) {
	host := codexHostname()
	if host == "" {
		t.Skip("hostname unavailable")
	}
	profile := loginRemoteProfile("alice@example.com", "sh-codex-2")
	wantName := "alice@example.com on " + host
	if profile.Name != wantName || profile.Email != "alice@example.com" || profile.Username != "sh-codex-2" || string(profile.Avatar) != defaultCodexAvatarMXC {
		t.Fatalf("unexpected remote profile: %#v", profile)
	}
}

func TestApplyRemoteProfileEmail(t *testing.T) {
	profile := bridgestatus.RemoteProfile{}
	applyRemoteProfileEmail(&profile, "alice@example.com")
	if profile.Email != "alice@example.com" {
		t.Fatalf("email display name should populate profile email: %#v", profile)
	}
	applyRemoteProfileEmail(&profile, "Alice Example")
	if profile.Email != "alice@example.com" {
		t.Fatalf("non-email display name should not replace profile email: %#v", profile)
	}
}

func TestShouldAppendHost(t *testing.T) {
	if !shouldAppendHost("alice@example.com", "workstation") {
		t.Fatal("display name without host should append host")
	}
	if shouldAppendHost("alice@example.com on workstation", "workstation") {
		t.Fatal("display name with host suffix should not append host")
	}
	if shouldAppendHost("alice@example.com", "") {
		t.Fatal("empty host should not be appended")
	}
}

func TestCodexUserLoginRecordUsesRemoteProfile(t *testing.T) {
	record := codexUserLoginRecord("sh-codex-2", "alice@example.com")
	if record.ID != "sh-codex-2" || record.RemoteName == "" || record.RemoteName != record.RemoteProfile.Name || record.RemoteProfile.Username != "sh-codex-2" {
		t.Fatalf("unexpected login record: %#v", record)
	}
}

func TestConfiguredLoginUserID(t *testing.T) {
	userID, ok := configuredLoginUserID("@alice:example.com")
	if !ok || userID != "@alice:example.com" {
		t.Fatalf("unexpected configured login user ID: %q ok=%v", userID, ok)
	}
	if isConfiguredLoginUserID("example.com") {
		t.Fatal("non-Matrix user IDs should not pass the prefix check")
	}
	if _, ok := configuredLoginUserID("@invalid"); ok {
		t.Fatal("malformed Matrix user ID should not parse")
	}
}

func TestGlobalBridgeInfoIncludesInstallIdentity(t *testing.T) {
	connector := &Connector{Bridge: &bridgev2.Bridge{
		ID: "codex",
		Matrix: &matrixconnector.Connector{Config: &bridgeconfig.Config{
			AppService: bridgeconfig.AppserviceConfig{
				Bot:              bridgeconfig.BotUserConfig{Username: "sh-codex-2bot"},
				UsernameTemplate: "sh-codex-2_{{.}}",
			},
		}},
	}}
	info := connector.globalBridgeInfo()
	if info["bridge_name"] != "sh-codex-2" {
		t.Fatalf("bridge_name = %#v, want sh-codex-2: %#v", info["bridge_name"], info)
	}
	if codexHostname() != "" && info["hostname"] == "" {
		t.Fatalf("missing hostname in install info: %#v", info)
	}
}

func TestAccountGlobalStateIncludesInstallIdentity(t *testing.T) {
	connector := &Connector{Bridge: &bridgev2.Bridge{
		ID: "codex",
		Matrix: &matrixconnector.Connector{Config: &bridgeconfig.Config{
			AppService: bridgeconfig.AppserviceConfig{
				Bot: bridgeconfig.BotUserConfig{Username: "sh-codex-2bot"},
			},
		}},
	}}
	state := connector.accountGlobalState(&appserver.Account{Type: "chatgpt", Email: "alice@example.com"})
	if state["type"] != "chatgpt" || state["email"] != "alice@example.com" || state["bridge_name"] != "sh-codex-2" {
		t.Fatalf("unexpected account global state: %#v", state)
	}
	if codexHostname() != "" && state["hostname"] == "" {
		t.Fatalf("missing hostname in account state: %#v", state)
	}
	state = connector.accountGlobalState(nil)
	if _, ok := state["type"]; ok {
		t.Fatalf("nil account should not set type: %#v", state)
	}
	if _, ok := state["email"]; ok {
		t.Fatalf("nil account should not set email: %#v", state)
	}
}

func TestAccountName(t *testing.T) {
	if got := accountName(nil); got != "Codex" {
		t.Fatalf("accountName(nil) = %q, want Codex", got)
	}
	account := &appserver.Account{Email: "alice@example.com", Type: "chatgpt"}
	if got := accountName(account); got != "alice@example.com" {
		t.Fatalf("accountName(email account) = %q, want alice@example.com", got)
	}
	account.Email = ""
	if got := accountName(account); got != "Codex chatgpt" {
		t.Fatalf("accountName(type account) = %q, want Codex chatgpt", got)
	}
}

func TestLoginIDsFallBackToBridgeID(t *testing.T) {
	connector := &Connector{Bridge: &bridgev2.Bridge{ID: "codex"}}
	if got := connector.loginIDForUser(); got != defaultLoginID {
		t.Fatalf("unexpected login ID: got %q, want %q", got, defaultLoginID)
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

func startTestFakeAppServer(t *testing.T, ctx context.Context, connector *Connector, logPath string) *appserver.Client {
	t.Helper()
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
	connector.appMu.Lock()
	connector.app = app
	connector.appMu.Unlock()
	return app
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

func identifiersContain(info *bridgev2.UserInfo, query string) bool {
	if info == nil {
		return false
	}
	for _, identifier := range info.Identifiers {
		if strings.Contains(strings.ToLower(identifier), query) {
			return true
		}
	}
	return false
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
	powerLevels     *event.PowerLevelsEventContent
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
	if f.powerLevels != nil {
		return f.powerLevels, nil
	}
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
	typings       []fakeTypingEvent
	roomCount     int
	onSendMessage func(roomID id.RoomID, eventType event.Type, content *event.Content, extra *bridgev2.MatrixSendExtra)
}

type fakeTypingEvent struct {
	RoomID  id.RoomID
	Type    bridgev2.TypingType
	Timeout time.Duration
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

func (f *fakeMatrixAPI) DeleteRoom(ctx context.Context, roomID id.RoomID, puppetsOnly bool) error {
	return nil
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
	f.typings = append(f.typings, fakeTypingEvent{RoomID: roomID, Type: typingType, Timeout: timeout})
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
