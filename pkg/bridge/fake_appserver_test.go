package bridge

import (
	"bufio"
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/beeper/codex-bridge/pkg/appserver"
)

const fakeAppServerEnv = "CODEX_BRIDGE_FAKE_APPSERVER"
const fakeAppServerLogEnv = "CODEX_BRIDGE_FAKE_APPSERVER_LOG"
const fakeAppServerDelayEnv = "CODEX_BRIDGE_FAKE_APPSERVER_DELAY_MS"
const fakeAppServerNoActiveSteerEnv = "CODEX_BRIDGE_FAKE_APPSERVER_NO_ACTIVE_STEER"
const fakeAppServerTurnStartIDEnv = "CODEX_BRIDGE_FAKE_APPSERVER_TURN_START_ID"

func TestMain(m *testing.M) {
	if os.Getenv(fakeAppServerEnv) == "1" {
		runFakeAppServer()
		return
	}
	os.Exit(m.Run())
}

type fakeAppServerMessage struct {
	ID     any             `json:"id,omitempty"`
	Method string          `json:"method,omitempty"`
	Params json.RawMessage `json:"params,omitempty"`
	Result any             `json:"result,omitempty"`
	Error  any             `json:"error,omitempty"`
}

func runFakeAppServer() {
	scanner := bufio.NewScanner(os.Stdin)
	writer := bufio.NewWriter(os.Stdout)
	defer writer.Flush()
	for scanner.Scan() {
		var msg fakeAppServerMessage
		if err := json.Unmarshal(scanner.Bytes(), &msg); err != nil || msg.Method == "" {
			continue
		}
		logFakeAppServerRequest(msg)
		payload := map[string]any{}
		_ = json.Unmarshal(msg.Params, &payload)
		resp := fakeAppServerMessage{ID: msg.ID}
		if msg.Method == "thread/read" {
			if firstString(payload, "threadId") == "missing-thread" {
				resp.Error = map[string]any{"code": -32004, "message": "thread not loaded: missing-thread"}
			}
		}
		if resp.Error == nil {
			threadID := firstString(payload, "threadId")
			if threadID == "" {
				threadID = "thread-1"
			}
			cwd := firstString(payload, "cwd")
			switch msg.Method {
			case "account/read":
				resp.Result = fakeAppServerAccountReadResponse()
			case "model/list":
				resp.Result = map[string]any{
					"data": []any{
						fakeAppServerModel("gpt-5", "gpt-5", "GPT-5", true),
						fakeAppServerModel("gpt-5.1", "gpt-5.1", "GPT-5.1", false),
						fakeAppServerModel("claude-sonnet-4.5", "anthropic/claude-sonnet-4.5", "Claude Sonnet 4.5", false),
					},
					"nextCursor": nil,
				}
			case "thread/list":
				cwdFilter := firstString(payload, "cwd")
				if cwdFilter == "" {
					cwdFilter = "/tmp/project"
				}
				resp.Result = map[string]any{
					"data": []any{
						fakeAppServerThread("thread-1", cwdFilter, false),
						fakeAppServerThread("thread-2", cwdFilter, false),
					},
					"nextCursor":      nil,
					"backwardsCursor": "thread-1",
				}
			case "thread/read", "thread/resume", "thread/start", "thread/rollback":
				if cwd == "" {
					cwd = os.TempDir()
				}
				thread := map[string]any{
					"thread":             fakeAppServerThread(threadID, cwd, msg.Method == "thread/read" && payload["includeTurns"] == true),
					"cwd":                cwd,
					"model":              "gpt-5",
					"modelProvider":      "openai",
					"serviceTier":        nil,
					"instructionSources": []string{},
					"approvalPolicy":     "on-request",
					"approvalsReviewer":  map[string]any{"type": "user"},
					"sandbox":            map[string]any{"mode": "workspace-write"},
					"reasoningEffort":    "high",
				}
				if threadID == "archived-thread" {
					thread["thread"].(map[string]any)["archivedAt"] = "2026-06-01T00:00:00Z"
				}
				resp.Result = thread
			case "turn/start":
				turnID := firstTrimmedNonEmpty(os.Getenv(fakeAppServerTurnStartIDEnv), "turn-1")
				resp.Result = map[string]any{
					"turn": fakeAppServerTurn(turnID, "inProgress"),
				}
			case "turn/steer":
				if os.Getenv(fakeAppServerNoActiveSteerEnv) == "1" {
					resp.Error = map[string]any{"code": -32600, "message": "no active turn to steer"}
				} else {
					resp.Result = map[string]any{"turnId": firstString(payload, "expectedTurnId")}
				}
			case "turn/interrupt":
				resp.Result = map[string]any{}
			default:
				resp.Result = map[string]any{}
			}
		}
		delay, err := strconv.Atoi(os.Getenv(fakeAppServerDelayEnv))
		if err == nil && delay > 0 {
			time.Sleep(time.Duration(delay) * time.Millisecond)
		}
		raw, _ := json.Marshal(resp)
		_, _ = writer.Write(append(raw, '\n'))
		_ = writer.Flush()
	}
}

func fakeAppServerAccountReadResponse() map[string]any {
	return map[string]any{
		"account": map[string]any{
			"type":     "chatgpt",
			"email":    "alice@example.com",
			"planType": "pro",
		},
		"requiresOpenaiAuth": true,
	}
}

func fakeAppServerModel(id, model, displayName string, isDefault bool) map[string]any {
	return map[string]any{
		"id":                        id,
		"model":                     model,
		"upgrade":                   nil,
		"upgradeInfo":               nil,
		"availabilityNux":           nil,
		"displayName":               displayName,
		"description":               displayName,
		"hidden":                    false,
		"supportedReasoningEfforts": []any{map[string]any{"reasoningEffort": "high", "description": "high"}},
		"defaultReasoningEffort":    "high",
		"inputModalities":           []string{"text", "image"},
		"supportsPersonality":       true,
		"additionalSpeedTiers":      []string{},
		"serviceTiers":              []any{},
		"defaultServiceTier":        nil,
		"isDefault":                 isDefault,
	}
}

func fakeAppServerThread(threadID, cwd string, includeTurns bool) map[string]any {
	turns := []any{}
	if includeTurns {
		turns = []any{
			fakeAppServerTurn("turn-1", "completed"),
			fakeAppServerTurn("turn-2", "completed"),
		}
	}
	return map[string]any{
		"id":             threadID,
		"sessionId":      threadID,
		"forkedFromId":   nil,
		"parentThreadId": nil,
		"preview":        "",
		"ephemeral":      false,
		"modelProvider":  "openai",
		"createdAt":      int64(1_717_200_000),
		"updatedAt":      int64(1_717_200_060),
		"status":         map[string]any{"type": "idle"},
		"path":           nil,
		"cwd":            cwd,
		"cliVersion":     "0.136.0",
		"source":         "appServer",
		"threadSource":   "user",
		"agentNickname":  nil,
		"agentRole":      nil,
		"gitInfo":        nil,
		"name":           nil,
		"turns":          turns,
	}
}

func fakeAppServerTurn(turnID, status string) map[string]any {
	return map[string]any{
		"id":          turnID,
		"items":       []any{},
		"itemsView":   "full",
		"status":      status,
		"error":       nil,
		"startedAt":   int64(1_717_200_000),
		"completedAt": nil,
		"durationMs":  nil,
	}
}

func TestFakeAppServerContractShapes(t *testing.T) {
	raw, err := json.Marshal(fakeAppServerAccountReadResponse())
	if err != nil {
		t.Fatal(err)
	}
	var account appserver.AccountReadResponse
	if err = json.Unmarshal(raw, &account); err != nil {
		t.Fatal(err)
	}
	if account.Account == nil || account.Account.Type != "chatgpt" || account.Account.PlanType != "pro" || !account.RequiresOpenAIAuth {
		t.Fatalf("bad fake account/read shape: %#v", account)
	}

	raw, err = json.Marshal(map[string]any{
		"data":       []any{fakeAppServerModel("gpt-5", "gpt-5", "GPT-5", true)},
		"nextCursor": nil,
	})
	if err != nil {
		t.Fatal(err)
	}
	var models appserver.ModelListResponse
	if err = json.Unmarshal(raw, &models); err != nil {
		t.Fatal(err)
	}
	if len(models.Data) != 1 || models.Data[0].DisplayName != "GPT-5" || !models.Data[0].IsDefault {
		t.Fatalf("bad fake model/list shape: %#v", models)
	}

	raw, err = json.Marshal(map[string]any{"thread": fakeAppServerThread("thread-1", "/tmp/project", true)})
	if err != nil {
		t.Fatal(err)
	}
	var thread appserver.ThreadReadResponse
	if err = json.Unmarshal(raw, &thread); err != nil {
		t.Fatal(err)
	}
	if thread.Thread.ID != "thread-1" || thread.Thread.Cwd != "/tmp/project" || len(thread.Thread.Turns) != 2 || thread.Thread.Raw["status"] == nil {
		t.Fatalf("bad fake thread/read shape: %#v", thread.Thread)
	}

	raw, err = json.Marshal(map[string]any{"turn": fakeAppServerTurn("turn-1", "inProgress")})
	if err != nil {
		t.Fatal(err)
	}
	var start appserver.TurnStartResponse
	if err = json.Unmarshal(raw, &start); err != nil {
		t.Fatal(err)
	}
	if start.Turn.ID != "turn-1" || start.Turn.Status != "inProgress" {
		t.Fatalf("bad fake turn/start shape: %#v", start)
	}

	raw, err = json.Marshal(map[string]any{"turnId": "turn-1"})
	if err != nil {
		t.Fatal(err)
	}
	var steer appserver.TurnSteerResponse
	if err = json.Unmarshal(raw, &steer); err != nil {
		t.Fatal(err)
	}
	if steer.TurnID != "turn-1" {
		t.Fatalf("bad fake turn/steer shape: %#v", steer)
	}
}

func logFakeAppServerRequest(msg fakeAppServerMessage) {
	logPath := os.Getenv(fakeAppServerLogEnv)
	if logPath == "" {
		return
	}
	raw, _ := fakeAppServerRequestLogLine(msg)
	if file, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600); err == nil {
		_, _ = file.Write(append(raw, '\n'))
		_ = file.Close()
	}
}

func fakeAppServerRequestLogLine(msg fakeAppServerMessage) ([]byte, error) {
	return json.Marshal(struct {
		Method string          `json:"method"`
		Params json.RawMessage `json:"params"`
	}{Method: msg.Method, Params: msg.Params})
}

type fakeAppServerRequest struct {
	Method string         `json:"method"`
	Params map[string]any `json:"params"`
}

func TestFakeAppServerRequestLogLine(t *testing.T) {
	raw, err := fakeAppServerRequestLogLine(fakeAppServerMessage{
		Method: "thread/start",
		Params: json.RawMessage(`{"cwd":"/tmp/project"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	var req fakeAppServerRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		t.Fatal(err)
	}
	if req.Method != "thread/start" || req.Params["cwd"] != "/tmp/project" {
		t.Fatalf("unexpected fake app-server log line: %s", raw)
	}
}

func readFakeAppServerRequests(t *testing.T, path string) []fakeAppServerRequest {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var out []fakeAppServerRequest
	for _, line := range strings.Split(firstTrimmedNonEmpty(string(raw)), "\n") {
		if line == "" {
			continue
		}
		var req fakeAppServerRequest
		if err = json.Unmarshal([]byte(line), &req); err != nil {
			t.Fatalf("bad fake app-server log line %q: %v", line, err)
		}
		out = append(out, req)
	}
	return out
}

func readFakeAppServerRequestsIfExists(t *testing.T, path string) []fakeAppServerRequest {
	t.Helper()
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return readFakeAppServerRequests(t, path)
}

func findFakeAppServerRequest(requests []fakeAppServerRequest, method string) (fakeAppServerRequest, bool) {
	for _, req := range requests {
		if req.Method == method {
			return req, true
		}
	}
	return fakeAppServerRequest{}, false
}

func countFakeAppServerRequests(requests []fakeAppServerRequest, method string) int {
	count := 0
	for _, req := range requests {
		if req.Method == method {
			count++
		}
	}
	return count
}

func fakeAppServerInputText(params map[string]any) string {
	input, _ := params["input"].([]any)
	if len(input) == 0 {
		return ""
	}
	part, _ := input[0].(map[string]any)
	text, _ := part["text"].(string)
	return text
}
