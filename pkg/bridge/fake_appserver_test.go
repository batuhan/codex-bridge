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
)

const fakeAppServerEnv = "CODEX_BRIDGE_FAKE_APPSERVER"
const fakeAppServerLogEnv = "CODEX_BRIDGE_FAKE_APPSERVER_LOG"
const fakeAppServerDelayEnv = "CODEX_BRIDGE_FAKE_APPSERVER_DELAY_MS"

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
		appendFakeAppServerRequest(msg.Method, msg.Params)
		resp := fakeAppServerMessage{ID: msg.ID}
		if rpcErr := fakeAppServerError(msg.Method, msg.Params); rpcErr != nil {
			resp.Error = rpcErr
		} else {
			resp.Result = fakeAppServerResult(msg.Method, msg.Params)
		}
		delayFakeAppServerResponse()
		raw, _ := json.Marshal(resp)
		_, _ = writer.Write(append(raw, '\n'))
		_ = writer.Flush()
	}
}

func delayFakeAppServerResponse() {
	delay, err := strconv.Atoi(os.Getenv(fakeAppServerDelayEnv))
	if err != nil || delay <= 0 {
		return
	}
	time.Sleep(time.Duration(delay) * time.Millisecond)
}

func appendFakeAppServerRequest(method string, params json.RawMessage) {
	logPath := os.Getenv(fakeAppServerLogEnv)
	if logPath == "" {
		return
	}
	raw, _ := json.Marshal(struct {
		Method string          `json:"method"`
		Params json.RawMessage `json:"params"`
	}{Method: method, Params: params})
	file, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return
	}
	_, _ = file.Write(append(raw, '\n'))
	_ = file.Close()
}

func fakeAppServerError(method string, params json.RawMessage) any {
	payload := map[string]any{}
	_ = json.Unmarshal(params, &payload)
	if method == "thread/read" && firstFakeAppServerString(payload, "threadId") == "missing-thread" {
		return map[string]any{"code": -32004, "message": "thread not loaded: missing-thread"}
	}
	return nil
}

func fakeAppServerResult(method string, params json.RawMessage) any {
	payload := map[string]any{}
	_ = json.Unmarshal(params, &payload)
	threadID := firstFakeAppServerString(payload, "threadId")
	if threadID == "" {
		threadID = "thread-1"
	}
	cwd := firstFakeAppServerString(payload, "cwd")
	switch method {
	case "model/list":
		return map[string]any{
			"data": []map[string]any{
				{"id": "gpt-5", "model": "gpt-5"},
				{"id": "gpt-5.1", "model": "gpt-5.1"},
				{"id": "claude-sonnet-4.5", "model": "anthropic/claude-sonnet-4.5"},
			},
		}
	case "thread/read", "thread/resume", "thread/start", "thread/rollback":
		if cwd == "" {
			cwd = os.TempDir()
		}
		thread := map[string]any{
			"thread": map[string]any{
				"id":            threadID,
				"sessionId":     threadID,
				"cwd":           cwd,
				"modelProvider": "openai",
			},
			"cwd":             cwd,
			"model":           "gpt-5",
			"modelProvider":   "openai",
			"reasoningEffort": "high",
		}
		if method == "thread/read" && payload["includeTurns"] == true {
			thread["thread"].(map[string]any)["turns"] = []map[string]any{
				{"id": "turn-1", "status": "completed"},
				{"id": "turn-2", "status": "completed"},
			}
		}
		return thread
	case "turn/start":
		return map[string]any{
			"turn": map[string]any{
				"id":     "turn-1",
				"status": "inProgress",
			},
		}
	case "turn/steer", "turn/interrupt":
		return map[string]any{}
	default:
		return map[string]any{}
	}
}

func firstFakeAppServerString(payload map[string]any, keys ...string) string {
	for _, key := range keys {
		if value, _ := payload[key].(string); strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

type fakeAppServerRequest struct {
	Method string         `json:"method"`
	Params map[string]any `json:"params"`
}

func readFakeAppServerRequests(t *testing.T, path string) []fakeAppServerRequest {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var out []fakeAppServerRequest
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
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
