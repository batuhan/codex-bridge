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

	"maunium.net/go/mautrix/bridgev2/status"
)

const codexGeneratedSchemaPath = "/Users/batuhan/projects/codex/sdk/python/src/openai_codex/generated/v2_all.py"
const codexTypeScriptNotificationPath = "/Users/batuhan/projects/codex/codex-rs/app-server-protocol/schema/typescript/ServerNotification.ts"
const codexTypeScriptServerRequestPath = "/Users/batuhan/projects/codex/codex-rs/app-server-protocol/schema/typescript/ServerRequest.ts"
const codexJSONNotificationPath = "/Users/batuhan/projects/codex/codex-rs/app-server-protocol/schema/json/ServerNotification.json"
const codexJSONServerRequestPath = "/Users/batuhan/projects/codex/codex-rs/app-server-protocol/schema/json/ServerRequest.json"
const codexTypeScriptV2ThreadItemPath = "/Users/batuhan/projects/codex/codex-rs/app-server-protocol/schema/typescript/v2/ThreadItem.ts"

func TestClientFillsBridgeStateWithCodexGlobalState(t *testing.T) {
	connector := &Connector{globalState: map[string]any{
		"lastNotification":                "account/rateLimits/updated",
		"account/rateLimits/updated":      map[string]any{"rateLimits": map[string]any{"planType": "plus"}},
		"mcpServer/startupStatus/updated": map[string]any{"name": "github", "status": "ready"},
	}}
	client := &Client{Main: connector}

	state := client.FillBridgeState(status.BridgeState{StateEvent: status.StateConnected})
	codex, ok := state.Info["codex"].(map[string]any)
	if !ok {
		t.Fatalf("missing codex bridge state: %#v", state.Info)
	}
	if codex["lastNotification"] != "account/rateLimits/updated" {
		t.Fatalf("unexpected codex bridge state: %#v", codex)
	}
}

func TestGlobalCodexNotificationsBroadcastBridgeState(t *testing.T) {
	tests := []struct {
		method    string
		params    string
		wantState status.BridgeStateEvent
		wantMsg   string
	}{
		{
			method:    "account/rateLimits/updated",
			params:    `{"marker":"rate-limits","rateLimits":{"planType":"plus"}}`,
			wantState: status.StateConnected,
		},
		{
			method:    "configWarning",
			params:    `{"marker":"config-warning","message":"bad config","details":"line 4"}`,
			wantState: status.StateConnected,
			wantMsg:   "bad config",
		},
		{
			method:    "error",
			params:    `{"marker":"error","message":"boom"}`,
			wantState: status.StateUnknownError,
			wantMsg:   "boom",
		},
		{
			method:    "account/login/completed",
			params:    `{"marker":"login-failed","success":false,"error":"bad login"}`,
			wantState: status.StateLoggedOut,
			wantMsg:   "bad login",
		},
	}
	for _, tt := range tests {
		t.Run(tt.method, func(t *testing.T) {
			ctx := context.Background()
			matrix := &fakeMatrixConnector{bridgeStateCh: make(chan status.BridgeState, 8)}
			connector, br := testBridgeWithDB(t, matrix)
			user, err := br.GetUserByMXID(ctx, "@alice:example.com")
			if err != nil {
				t.Fatal(err)
			}
			login, err := connector.ensureLoginID(ctx, user, "sh-codex", "alice@example.com")
			if err != nil {
				t.Fatal(err)
			}
			login.Client = &Client{Main: connector, UserLogin: login, loggedIn: true}
			drainBridgeStates(matrix.bridgeStateCh)

			connector.handleGlobalNotification(tt.method, []byte(tt.params))
			state := waitBridgeStateForNotification(t, matrix.bridgeStateCh, tt.method)
			if state.StateEvent != tt.wantState {
				t.Fatalf("state event = %s, want %s: %#v", state.StateEvent, tt.wantState, state)
			}
			if tt.wantMsg != "" && !strings.Contains(state.Message, tt.wantMsg) {
				t.Fatalf("state message %q did not contain %q", state.Message, tt.wantMsg)
			}
			codex, ok := state.Info["codex"].(map[string]any)
			if !ok {
				t.Fatalf("bridge state missing Codex info: %#v", state.Info)
			}
			if codex["lastNotification"] != tt.method {
				t.Fatalf("last notification = %#v, want %s: %#v", codex["lastNotification"], tt.method, codex)
			}
			payload, ok := codex[tt.method].(map[string]any)
			if !ok || payload["marker"] == "" {
				t.Fatalf("bridge state did not preserve global Codex payload: %#v", codex)
			}
		})
	}
}

func waitBridgeStateForNotification(t *testing.T, ch <-chan status.BridgeState, method string) status.BridgeState {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		select {
		case state := <-ch:
			codex, _ := state.Info["codex"].(map[string]any)
			if codex["lastNotification"] == method {
				return state
			}
		case <-deadline:
			t.Fatalf("timed out waiting for bridge state for %s", method)
			return status.BridgeState{}
		}
	}
}

func TestGlobalCodexNotificationsPreserveRawBridgeInfo(t *testing.T) {
	for _, method := range generatedTypeScriptMethods(t, codexTypeScriptNotificationPath) {
		if !handledAsGlobalNotification(method) {
			continue
		}
		t.Run(method, func(t *testing.T) {
			connector := &Connector{globalState: map[string]any{}}
			connector.handleGlobalNotification(method, []byte(`{"marker":"raw-global"}`))

			codex := connector.globalBridgeInfo()
			if codex["lastNotification"] != method {
				t.Fatalf("last notification = %#v, want %s: %#v", codex["lastNotification"], method, codex)
			}
			payload, ok := codex[method].(map[string]any)
			if !ok || payload["marker"] != "raw-global" {
				t.Fatalf("global bridge info did not preserve raw payload: %#v", codex)
			}
		})
	}
}

func handledAsGlobalNotification(method string) bool {
	switch method {
	case "warning", "configWarning", "deprecationNotice", "guardianWarning", "error":
		return true
	default:
		return isGlobalNotification(method)
	}
}

func drainBridgeStates(ch <-chan status.BridgeState) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

func waitBridgeState(t *testing.T, ch <-chan status.BridgeState) status.BridgeState {
	t.Helper()
	select {
	case state := <-ch:
		return state
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for bridge state")
		return status.BridgeState{}
	}
}

func TestCurrentCodexServerNotificationsAreClassified(t *testing.T) {
	for _, method := range generatedTypeScriptMethods(t, codexTypeScriptNotificationPath) {
		if !isThreadNotification(method) && !isGlobalNotification(method) {
			t.Fatalf("Codex server notification %q is not classified", method)
		}
	}
}

func TestGeneratedCodexServerNotificationsAreClassified(t *testing.T) {
	methods := generatedCodexNotificationMethods(t)
	if len(methods) == 0 {
		t.Fatal("generated Codex schema did not contain notification methods")
	}
	for _, method := range methods {
		if !isThreadNotification(method) && !isGlobalNotification(method) {
			t.Fatalf("generated Codex server notification %q is not classified", method)
		}
	}
}

func TestTypeScriptCodexServerNotificationsAreClassified(t *testing.T) {
	methods := generatedTypeScriptMethods(t, codexTypeScriptNotificationPath)
	if len(methods) == 0 {
		t.Fatal("TypeScript Codex schema did not contain notification methods")
	}
	for _, method := range methods {
		if !isThreadNotification(method) && !isGlobalNotification(method) {
			t.Fatalf("TypeScript Codex server notification %q is not classified", method)
		}
	}
}

func TestCurrentCodexServerNotificationsMatchGeneratedSchemas(t *testing.T) {
	assertStringSetEqual(t, currentCodexServerNotifications, generatedTypeScriptMethods(t, codexTypeScriptNotificationPath), "TypeScript")
}

func TestTypeScriptCodexThreadNotificationsHaveDispatchLane(t *testing.T) {
	methods := generatedTypeScriptMethods(t, codexTypeScriptNotificationPath)
	if len(methods) == 0 {
		t.Fatal("TypeScript Codex schema did not contain notification methods")
	}
	for _, method := range methods {
		if !isThreadNotification(method) {
			continue
		}
		if !isThreadMetadataNotification(method) && !isThreadNoticeNotification(method) && !isActiveRunNotification(method) {
			t.Fatalf("TypeScript Codex thread notification %q has no metadata, notice, or active-run dispatch lane", method)
		}
	}
}

func TestJSONCodexServerNotificationsAreClassified(t *testing.T) {
	methods := generatedJSONMethods(t, codexJSONNotificationPath)
	if len(methods) == 0 {
		t.Fatal("JSON Codex schema did not contain notification methods")
	}
	for _, method := range methods {
		if !isThreadNotification(method) && !isGlobalNotification(method) {
			t.Fatalf("JSON Codex server notification %q is not classified", method)
		}
	}
}

func TestJSONCodexThreadNotificationsHaveDispatchLane(t *testing.T) {
	methods := generatedJSONMethods(t, codexJSONNotificationPath)
	if len(methods) == 0 {
		t.Fatal("JSON Codex schema did not contain notification methods")
	}
	for _, method := range methods {
		if !isThreadNotification(method) {
			continue
		}
		if !isThreadMetadataNotification(method) && !isThreadNoticeNotification(method) && !isActiveRunNotification(method) {
			t.Fatalf("JSON Codex thread notification %q has no metadata, notice, or active-run dispatch lane", method)
		}
	}
}

func TestTypeScriptCodexServerRequestsAreHandled(t *testing.T) {
	methods := generatedTypeScriptMethods(t, codexTypeScriptServerRequestPath)
	if len(methods) == 0 {
		t.Fatal("TypeScript Codex schema did not contain server request methods")
	}
	for _, method := range methods {
		if !isHandledCodexServerRequest(method) {
			t.Fatalf("TypeScript Codex server request %q is not handled", method)
		}
	}
}

func TestJSONCodexServerRequestsAreHandled(t *testing.T) {
	methods := generatedJSONMethods(t, codexJSONServerRequestPath)
	if len(methods) == 0 {
		t.Fatal("JSON Codex schema did not contain server request methods")
	}
	for _, method := range methods {
		if !isHandledCodexServerRequest(method) {
			t.Fatalf("JSON Codex server request %q is not handled", method)
		}
	}
}

func generatedCodexNotificationMethods(t *testing.T) []string {
	t.Helper()
	raw, err := os.ReadFile(codexGeneratedSchemaPath)
	if err != nil {
		t.Fatalf("read generated Codex schema: %v", err)
	}
	re := regexp.MustCompile(`(?s)method:\s*Annotated\[\s*Literal\["([^"]+)"\][^\]]*NotificationMethod`)
	seen := map[string]bool{}
	for _, match := range re.FindAllSubmatch(raw, -1) {
		seen[string(match[1])] = true
	}
	methods := make([]string, 0, len(seen))
	for method := range seen {
		methods = append(methods, method)
	}
	sort.Strings(methods)
	return methods
}

func generatedJSONMethods(t *testing.T, path string) []string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read generated Codex JSON schema: %v", err)
	}
	var schema struct {
		OneOf []struct {
			Properties struct {
				Method struct {
					Enum []string `json:"enum"`
				} `json:"method"`
			} `json:"properties"`
		} `json:"oneOf"`
	}
	if err = json.Unmarshal(raw, &schema); err != nil {
		t.Fatalf("parse generated Codex JSON schema: %v", err)
	}
	seen := map[string]bool{}
	for _, item := range schema.OneOf {
		for _, method := range item.Properties.Method.Enum {
			if method != "" {
				seen[method] = true
			}
		}
	}
	methods := make([]string, 0, len(seen))
	for method := range seen {
		methods = append(methods, method)
	}
	sort.Strings(methods)
	return methods
}

func generatedTypeScriptMethods(t *testing.T, path string) []string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read generated Codex TypeScript schema: %v", err)
	}
	re := regexp.MustCompile(`"method":\s*"([^"]+)"`)
	seen := map[string]bool{}
	for _, match := range re.FindAllSubmatch(raw, -1) {
		seen[string(match[1])] = true
	}
	methods := make([]string, 0, len(seen))
	for method := range seen {
		methods = append(methods, method)
	}
	sort.Strings(methods)
	return methods
}

func assertStringSetEqual(t *testing.T, got, want []string, label string) {
	t.Helper()
	gotSet := map[string]bool{}
	wantSet := map[string]bool{}
	for _, value := range got {
		gotSet[value] = true
	}
	for _, value := range want {
		wantSet[value] = true
	}
	var missing, extra []string
	for value := range wantSet {
		if !gotSet[value] {
			missing = append(missing, value)
		}
	}
	for value := range gotSet {
		if !wantSet[value] {
			extra = append(extra, value)
		}
	}
	sort.Strings(missing)
	sort.Strings(extra)
	if len(missing) > 0 || len(extra) > 0 {
		t.Fatalf("current Codex notification list differs from %s schema: missing=%v extra=%v", label, missing, extra)
	}
}

func isThreadNotification(method string) bool {
	switch method {
	case "error",
		"thread/started",
		"thread/status/changed",
		"thread/archived",
		"thread/unarchived",
		"thread/closed",
		"thread/name/updated",
		"thread/goal/updated",
		"thread/goal/cleared",
		"thread/settings/updated",
		"thread/tokenUsage/updated",
		"turn/started",
		"hook/started",
		"turn/completed",
		"hook/completed",
		"turn/diff/updated",
		"turn/plan/updated",
		"item/started",
		"item/autoApprovalReview/started",
		"item/autoApprovalReview/completed",
		"item/completed",
		"rawResponseItem/completed",
		"item/agentMessage/delta",
		"item/plan/delta",
		"command/exec/outputDelta",
		"process/outputDelta",
		"process/exited",
		"item/commandExecution/outputDelta",
		"item/commandExecution/terminalInteraction",
		"item/fileChange/outputDelta",
		"item/fileChange/patchUpdated",
		"serverRequest/resolved",
		"item/mcpToolCall/progress",
		"item/reasoning/summaryTextDelta",
		"item/reasoning/summaryPartAdded",
		"item/reasoning/textDelta",
		"thread/compacted",
		"model/rerouted",
		"model/verification",
		"warning",
		"guardianWarning",
		"deprecationNotice",
		"configWarning",
		"thread/realtime/started",
		"thread/realtime/itemAdded",
		"thread/realtime/transcript/delta",
		"thread/realtime/transcript/done",
		"thread/realtime/outputAudio/delta",
		"thread/realtime/sdp",
		"thread/realtime/error",
		"thread/realtime/closed":
		return true
	default:
		return false
	}
}

func isGlobalNotification(method string) bool {
	switch method {
	case "account/updated",
		"account/rateLimits/updated",
		"account/login/completed",
		"mcpServer/oauthLogin/completed",
		"mcpServer/startupStatus/updated",
		"app/list/updated",
		"remoteControl/status/changed",
		"externalAgentConfig/import/completed",
		"fs/changed",
		"skills/changed",
		"fuzzyFileSearch/sessionUpdated",
		"fuzzyFileSearch/sessionCompleted",
		"windows/worldWritableWarning",
		"windowsSandbox/setupCompleted":
		return true
	default:
		return false
	}
}

var currentCodexServerNotifications = []string{
	"error",
	"thread/started",
	"thread/status/changed",
	"thread/archived",
	"thread/unarchived",
	"thread/closed",
	"skills/changed",
	"thread/name/updated",
	"thread/goal/updated",
	"thread/goal/cleared",
	"thread/settings/updated",
	"thread/tokenUsage/updated",
	"turn/started",
	"hook/started",
	"turn/completed",
	"hook/completed",
	"turn/diff/updated",
	"turn/plan/updated",
	"item/started",
	"item/autoApprovalReview/started",
	"item/autoApprovalReview/completed",
	"item/completed",
	"rawResponseItem/completed",
	"item/agentMessage/delta",
	"item/plan/delta",
	"command/exec/outputDelta",
	"process/outputDelta",
	"process/exited",
	"item/commandExecution/outputDelta",
	"item/commandExecution/terminalInteraction",
	"item/fileChange/outputDelta",
	"item/fileChange/patchUpdated",
	"serverRequest/resolved",
	"item/mcpToolCall/progress",
	"mcpServer/oauthLogin/completed",
	"mcpServer/startupStatus/updated",
	"account/updated",
	"account/rateLimits/updated",
	"app/list/updated",
	"remoteControl/status/changed",
	"externalAgentConfig/import/completed",
	"fs/changed",
	"item/reasoning/summaryTextDelta",
	"item/reasoning/summaryPartAdded",
	"item/reasoning/textDelta",
	"thread/compacted",
	"model/rerouted",
	"model/verification",
	"warning",
	"guardianWarning",
	"deprecationNotice",
	"configWarning",
	"fuzzyFileSearch/sessionUpdated",
	"fuzzyFileSearch/sessionCompleted",
	"thread/realtime/started",
	"thread/realtime/itemAdded",
	"thread/realtime/transcript/delta",
	"thread/realtime/transcript/done",
	"thread/realtime/outputAudio/delta",
	"thread/realtime/sdp",
	"thread/realtime/error",
	"thread/realtime/closed",
	"windows/worldWritableWarning",
	"windowsSandbox/setupCompleted",
	"account/login/completed",
}
