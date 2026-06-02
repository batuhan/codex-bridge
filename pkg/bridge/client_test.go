package bridge

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/bridgeconfig"
	"maunium.net/go/mautrix/bridgev2/database"
	matrixconnector "maunium.net/go/mautrix/bridgev2/matrix"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"github.com/beeper/codex-bridge/pkg/appserver"
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
	if project.Chat == nil || project.Chat.PortalKey != projectPortalKey("/tmp/project", "sh-codex") {
		t.Fatalf("project contact has unexpected chat: %#v", project.Chat)
	}
	if project.Chat.DMRedirectedTo != codexUserID {
		t.Fatalf("project contact DM should redirect to Codex ghost, got %q", project.Chat.DMRedirectedTo)
	}
	if !project.Chat.PortalInfo.CanBackfill {
		t.Fatalf("project contact should advertise backfill: %#v", project.Chat.PortalInfo)
	}
	portal := &bridgev2.Portal{Portal: &database.Portal{PortalKey: project.Chat.PortalKey}}
	if !project.Chat.PortalInfo.ExtraUpdates(context.Background(), portal) {
		t.Fatal("expected portal metadata updater to report a change")
	}
	meta := portalMetadata(portal.Metadata)
	if meta.Cwd != "/tmp/project" || meta.ThreadID != "new-thread" {
		t.Fatalf("unexpected portal metadata: %#v", meta)
	}
}

func TestRecentDirectoryContactReusesNewProjectSessionRoom(t *testing.T) {
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
	if project.Chat == nil || project.Chat.PortalKey != newProjectKey {
		t.Fatalf("project contact should reuse existing session room: %#v", project.Chat)
	}
	if project.Chat.DMRedirectedTo != codexUserID {
		t.Fatalf("reused session DM should redirect to Codex ghost, got %q", project.Chat.DMRedirectedTo)
	}
	portal := &bridgev2.Portal{Portal: &database.Portal{PortalKey: project.Chat.PortalKey}}
	if !project.Chat.PortalInfo.ExtraUpdates(context.Background(), portal) {
		t.Fatal("expected portal metadata updater to report a change")
	}
	meta := portalMetadata(portal.Metadata)
	if meta.Cwd != "/tmp/project" || meta.ThreadID != "thread-1" {
		t.Fatalf("unexpected portal metadata: %#v", meta)
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
}

func TestLoginIDsIncludeBridgeAlias(t *testing.T) {
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
	want := []networkid.UserLoginID{defaultLoginID, "sh-codex"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected login IDs:\n got: %#v\nwant: %#v", got, want)
	}
}

func TestLoginIDsDoNotDuplicateDefault(t *testing.T) {
	connector := &Connector{Bridge: &bridgev2.Bridge{ID: "codex"}}
	got := connector.loginIDsForUser("")
	want := []networkid.UserLoginID{defaultLoginID}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected login IDs:\n got: %#v\nwant: %#v", got, want)
	}
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
