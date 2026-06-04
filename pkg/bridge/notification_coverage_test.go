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
const codexCombinedJSONSchemaPath = "/Users/batuhan/projects/codex/codex-rs/app-server-protocol/schema/json/codex_app_server_protocol.schemas.json"
const codexTypeScriptResponseItemPath = "/Users/batuhan/projects/codex/codex-rs/app-server-protocol/schema/typescript/ResponseItem.ts"
const codexTypeScriptV2NotificationDir = "/Users/batuhan/projects/codex/codex-rs/app-server-protocol/schema/typescript/v2"
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
			method:    "mcpServer/startupStatus/updated",
			params:    `{"marker":"mcp-startup","name":"github","status":"failed","error":"not logged in"}`,
			wantState: status.StateConnected,
			wantMsg:   "github failed to start: not logged in",
		},
		{
			method:    "mcpServer/oauthLogin/completed",
			params:    `{"marker":"mcp-oauth","name":"github","success":false,"error":"browser closed"}`,
			wantState: status.StateConnected,
			wantMsg:   "github OAuth login failed: browser closed",
		},
		{
			method:    "windows/worldWritableWarning",
			params:    `{"marker":"windows-world-writable","samplePaths":["C:\\tmp","D:\\work"],"extraCount":2,"failedScan":true}`,
			wantState: status.StateConnected,
			wantMsg:   "C:\\tmp",
		},
		{
			method:    "windowsSandbox/setupCompleted",
			params:    `{"marker":"windows-sandbox","mode":"wsl","success":false,"error":"missing kernel"}`,
			wantState: status.StateConnected,
			wantMsg:   "Windows sandbox wsl setup failed: missing kernel",
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
			for drained := false; !drained; {
				select {
				case <-matrix.bridgeStateCh:
				default:
					drained = true
				}
			}

			connector.handleGlobalNotification(tt.method, []byte(tt.params))
			var state status.BridgeState
			deadline := time.After(time.Second)
			for received := false; !received; {
				select {
				case candidate := <-matrix.bridgeStateCh:
					codex, _ := candidate.Info["codex"].(map[string]any)
					if codex["lastNotification"] == tt.method {
						state = candidate
						received = true
					}
				case <-deadline:
					t.Fatalf("timed out waiting for bridge state for %s", tt.method)
				}
			}
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

func TestWindowsWorldWritablePathsPrefersSamplePaths(t *testing.T) {
	got := windowsWorldWritablePaths(map[string]any{
		"samplePaths": []any{"C:\\tmp"},
		"paths":       []any{"D:\\work"},
	})
	if strings.Join(got, ",") != "C:\\tmp" {
		t.Fatalf("unexpected sampled paths: %#v", got)
	}
	got = windowsWorldWritablePaths(map[string]any{
		"paths": []any{"D:\\work"},
	})
	if strings.Join(got, ",") != "D:\\work" {
		t.Fatalf("unexpected fallback paths: %#v", got)
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

func TestGeneratedCodexServerNotificationsAreClassified(t *testing.T) {
	raw, err := os.ReadFile(codexGeneratedSchemaPath)
	if err != nil {
		t.Fatalf("read generated Codex schema: %v", err)
	}
	methods := sortedUniqueMatches(regexp.MustCompile(`(?s)method:\s*Annotated\[\s*Literal\["([^"]+)"\][^\]]*NotificationMethod`), raw)
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

func TestTypeScriptV2NotificationFilesAreInServerNotificationUnion(t *testing.T) {
	entries, err := os.ReadDir(codexTypeScriptV2NotificationDir)
	if err != nil {
		t.Fatalf("read generated Codex TypeScript v2 schema dir: %v", err)
	}
	var files []string
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, "Notification.ts") {
			continue
		}
		files = append(files, strings.TrimSuffix(name, ".ts"))
	}
	sort.Strings(files)
	raw, err := os.ReadFile(codexTypeScriptNotificationPath)
	if err != nil {
		t.Fatalf("read generated Codex TypeScript schema: %v", err)
	}
	imports := sortedUniqueMatches(regexp.MustCompile(`import type \{ ([A-Za-z0-9_]+Notification) \} from "\./v2/[A-Za-z0-9_]+Notification";`), raw)
	params := sortedUniqueMatches(regexp.MustCompile(`"params":\s*([A-Za-z0-9_]+Notification)`), raw)
	if len(files) == 0 {
		t.Fatal("TypeScript v2 schema did not contain notification files")
	}
	assertStringSetEqual(t, imports, files, "TypeScript v2 notification files")
	paramsSet := map[string]bool{}
	for _, param := range params {
		paramsSet[param] = true
	}
	var missing []string
	for _, name := range imports {
		if !paramsSet[name] {
			missing = append(missing, name)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Fatalf("TypeScript server notification union is missing values: %v", missing)
	}
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

func TestCombinedJSONCodexServerNotificationsAreClassified(t *testing.T) {
	methods := generatedCombinedJSONMethods(t, "ServerNotification")
	if len(methods) == 0 {
		t.Fatal("combined JSON Codex schema did not contain notification methods")
	}
	for _, method := range methods {
		if !isThreadNotification(method) && !isGlobalNotification(method) {
			t.Fatalf("combined JSON Codex server notification %q is not classified", method)
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

func TestTypeScriptActiveRunNotificationsHaveMappingDecision(t *testing.T) {
	methods := generatedTypeScriptMethods(t, codexTypeScriptNotificationPath)
	if len(methods) == 0 {
		t.Fatal("TypeScript Codex schema did not contain notification methods")
	}
	for _, method := range methods {
		if !isActiveRunNotification(method) {
			continue
		}
		if !isActiveRunNotificationMapped(method) {
			t.Fatalf("active-run notification %q has no visible AG-UI mapping", method)
		}
	}
}

func TestActiveRunMappedNotificationsAreExplicitlyHandled(t *testing.T) {
	handled := sourceSwitchCases(t, "stream.go", "func (r *activeRun) handle(", "func finishCodexTurn")
	for _, method := range generatedTypeScriptMethods(t, codexTypeScriptNotificationPath) {
		if !isActiveRunNotificationMapped(method) {
			continue
		}
		if !handled[method] {
			t.Fatalf("active-run notification %q is marked mapped but has no explicit activeRun.handle case", method)
		}
	}
}

func TestGlobalNotificationsAreExplicitlyHandled(t *testing.T) {
	handled := sourceSwitchCases(t, "connector.go", "func (c *Connector) handleGlobalNotification(", "func (c *Connector) updateGlobalState")
	for _, method := range generatedTypeScriptMethods(t, codexTypeScriptNotificationPath) {
		if !handledAsGlobalNotification(method) {
			continue
		}
		if !handled[method] {
			t.Fatalf("global notification %q is classified but has no explicit handleGlobalNotification case", method)
		}
	}
}

func TestTypeScriptRawResponseItemTypesHaveMappingDecision(t *testing.T) {
	raw, err := os.ReadFile(codexTypeScriptResponseItemPath)
	if err != nil {
		t.Fatalf("read generated Codex TypeScript response item schema: %v", err)
	}
	itemTypes := sortedUniqueMatches(regexp.MustCompile(`"type":\s*"([^"]+)"`), raw)
	if len(itemTypes) == 0 {
		t.Fatal("TypeScript Codex schema did not contain response item types")
	}
	for _, itemType := range itemTypes {
		mapped := false
		switch itemType {
		case "message",
			"reasoning",
			"function_call",
			"custom_tool_call",
			"tool_search_call",
			"function_call_output",
			"custom_tool_call_output",
			"tool_search_output",
			"local_shell_call",
			"web_search_call",
			"image_generation_call",
			"compaction",
			"compaction_trigger",
			"context_compaction":
			mapped = true
		}
		if !mapped && itemType != "other" {
			t.Fatalf("raw response item type %q has no visible mapping", itemType)
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

func TestCombinedJSONCodexServerRequestsAreHandled(t *testing.T) {
	methods := generatedCombinedJSONMethods(t, "ServerRequest")
	if len(methods) == 0 {
		t.Fatal("combined JSON Codex schema did not contain server request methods")
	}
	for _, method := range methods {
		if !isHandledCodexServerRequest(method) {
			t.Fatalf("combined JSON Codex server request %q is not handled", method)
		}
	}
}

func isHandledCodexServerRequest(method string) bool {
	if _, _, ok := directServerRequestError(method); ok {
		return true
	}
	if isApprovalRequestMethod(method) {
		return true
	}
	switch method {
	case "item/tool/requestUserInput",
		"mcpServer/elicitation/request",
		"item/tool/call":
		return true
	default:
		return false
	}
}

func generatedJSONMethods(t *testing.T, path string) []string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read generated Codex JSON schema: %v", err)
	}
	var schema struct {
		OneOf jsonMethodUnion `json:"oneOf"`
	}
	if err = json.Unmarshal(raw, &schema); err != nil {
		t.Fatalf("parse generated Codex JSON schema: %v", err)
	}
	return uniqueMethodEnums(schema.OneOf)
}

type jsonMethodUnion []struct {
	Properties struct {
		Method struct {
			Enum []string `json:"enum"`
		} `json:"method"`
	} `json:"properties"`
}

func uniqueMethodEnums(items jsonMethodUnion) []string {
	seen := map[string]bool{}
	for _, item := range items {
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

func generatedCombinedJSONMethods(t *testing.T, definition string) []string {
	t.Helper()
	raw, err := os.ReadFile(codexCombinedJSONSchemaPath)
	if err != nil {
		t.Fatalf("read combined Codex JSON schema: %v", err)
	}
	var schema struct {
		Definitions map[string]struct {
			OneOf jsonMethodUnion `json:"oneOf"`
		} `json:"definitions"`
	}
	if err = json.Unmarshal(raw, &schema); err != nil {
		t.Fatalf("parse combined Codex JSON schema: %v", err)
	}
	definitionSchema, ok := schema.Definitions[definition]
	if !ok {
		t.Fatalf("combined Codex JSON schema missing %s definition", definition)
	}
	return uniqueMethodEnums(definitionSchema.OneOf)
}

func isActiveRunNotificationMapped(method string) bool {
	switch method {
	case "thread/started",
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
		"thread/compacted",
		"turn/completed",
		"turn/diff/updated",
		"turn/plan/updated",
		"hook/started",
		"hook/completed",
		"item/started",
		"item/completed",
		"item/agentMessage/delta",
		"item/reasoning/summaryTextDelta",
		"item/reasoning/textDelta",
		"item/reasoning/summaryPartAdded",
		"item/commandExecution/outputDelta",
		"item/fileChange/outputDelta",
		"item/commandExecution/terminalInteraction",
		"item/plan/delta",
		"item/mcpToolCall/progress",
		"item/fileChange/patchUpdated",
		"serverRequest/resolved",
		"item/autoApprovalReview/started",
		"item/autoApprovalReview/completed",
		"command/exec/outputDelta",
		"process/outputDelta",
		"process/exited",
		"rawResponseItem/completed",
		"model/rerouted",
		"model/verification",
		"warning",
		"guardianWarning",
		"deprecationNotice",
		"configWarning",
		"error",
		"thread/realtime/transcript/delta",
		"thread/realtime/transcript/done",
		"thread/realtime/started",
		"thread/realtime/itemAdded",
		"thread/realtime/outputAudio/delta",
		"thread/realtime/sdp",
		"thread/realtime/error",
		"thread/realtime/closed":
		return true
	default:
		return false
	}
}

func generatedTypeScriptMethods(t *testing.T, path string) []string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read generated Codex TypeScript schema: %v", err)
	}
	return sortedUniqueMatches(regexp.MustCompile(`"method":\s*"([^"]+)"`), raw)
}

func sourceSwitchCases(t *testing.T, path, start, end string) map[string]bool {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read source %s: %v", path, err)
	}
	text := string(raw)
	startIndex := strings.Index(text, start)
	if startIndex < 0 {
		t.Fatalf("source %s missing start marker %q", path, start)
	}
	endIndex := strings.Index(text[startIndex:], end)
	if endIndex < 0 {
		t.Fatalf("source %s missing end marker %q", path, end)
	}
	body := text[startIndex : startIndex+endIndex]
	re := regexp.MustCompile(`"([^"]+/[^"]+|error|warning|guardianWarning|deprecationNotice|configWarning)"`)
	handled := map[string]bool{}
	for _, match := range re.FindAllStringSubmatch(body, -1) {
		handled[match[1]] = true
	}
	return handled
}

func sortedUniqueMatches(re *regexp.Regexp, raw []byte) []string {
	seen := map[string]bool{}
	for _, match := range re.FindAllSubmatch(raw, -1) {
		seen[string(match[1])] = true
	}
	values := make([]string, 0, len(seen))
	for value := range seen {
		values = append(values, value)
	}
	sort.Strings(values)
	return values
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
		t.Fatalf("string set differs from %s: missing=%v extra=%v", label, missing, extra)
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
