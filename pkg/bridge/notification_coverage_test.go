package bridge

import (
	"os"
	"regexp"
	"sort"
	"testing"

	"maunium.net/go/mautrix/bridgev2/status"
)

const codexGeneratedSchemaPath = "/Users/batuhan/projects/codex/sdk/python/src/openai_codex/generated/v2_all.py"
const codexTypeScriptNotificationPath = "/Users/batuhan/projects/codex/codex-rs/app-server-protocol/schema/typescript/ServerNotification.ts"
const codexTypeScriptServerRequestPath = "/Users/batuhan/projects/codex/codex-rs/app-server-protocol/schema/typescript/ServerRequest.ts"

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

func TestCurrentCodexServerNotificationsAreClassified(t *testing.T) {
	for _, method := range currentCodexServerNotifications {
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
