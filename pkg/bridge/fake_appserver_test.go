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
			case "model/list":
				resp.Result = map[string]any{
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
				if threadID == "archived-thread" {
					thread["thread"].(map[string]any)["archived"] = true
				}
				if msg.Method == "thread/read" && payload["includeTurns"] == true {
					thread["thread"].(map[string]any)["turns"] = []map[string]any{
						{"id": "turn-1", "status": "completed"},
						{"id": "turn-2", "status": "completed"},
					}
				}
				resp.Result = thread
			case "turn/start":
				resp.Result = map[string]any{
					"turn": map[string]any{
						"id":     "turn-1",
						"status": "inProgress",
					},
				}
			case "turn/steer", "turn/interrupt":
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
