package bridge

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	agui "github.com/beeper/ai-bridge/pkg/ag-ui"
	aistream "github.com/beeper/ai-bridge/pkg/ai-stream"
	aimatrix "github.com/beeper/ai-bridge/pkg/ai-stream/matrix"
	"github.com/beeper/codex-bridge/pkg/appserver"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/event/cmdschema"
)

func TestParseApprovalCommandResponse(t *testing.T) {
	response, ok := parseApprovalCommandResponse("approval-1 approve")
	if !ok || !response.Approved || response.Always || response.ID != "approval-1" {
		t.Fatalf("unexpected approve response: %#v ok=%v", response, ok)
	}

	response, ok = parseApprovalCommandResponse("approval-1 always")
	if !ok || !response.Approved || !response.Always {
		t.Fatalf("unexpected always response: %#v ok=%v", response, ok)
	}

	response, ok = parseApprovalCommandResponse("approval-1 deny")
	if !ok || response.Approved || response.Reason != "denied" {
		t.Fatalf("unexpected deny response: %#v ok=%v", response, ok)
	}

	if _, ok = parseApprovalCommandResponse("approval-1 maybe"); ok {
		t.Fatal("unexpected success for invalid choice")
	}
}

func TestResolveRawApprovalChoice(t *testing.T) {
	choice, ok := resolveRawApprovalChoice("")
	if !ok || choice.Key != "approve" {
		t.Fatalf("blank approval choice should default to approve: %#v ok=%v", choice, ok)
	}
	choice, ok = resolveRawApprovalChoice("always")
	if !ok || choice.Key != "always_approve" {
		t.Fatalf("unexpected always approval choice: %#v ok=%v", choice, ok)
	}
	if _, ok = resolveRawApprovalChoice("maybe"); ok {
		t.Fatal("invalid approval choice should not resolve")
	}
}

func TestApprovalCommandStateIncludesOptionalDetails(t *testing.T) {
	got := approvalCommandState(aistream.ToolApprovalResponse{
		ID:       "approval-1",
		Approved: true,
		Always:   true,
		Reason:   "trusted",
	})
	want := map[string]any{
		"id":       "approval-1",
		"approved": true,
		"choice":   "always",
		"always":   true,
		"reason":   "trusted",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected approval command state:\n got: %#v\nwant: %#v", got, want)
	}
}

func TestSplitCommandArg(t *testing.T) {
	head, tail, ok := splitCommandArg("input-1   /tmp/project ")
	if !ok || head != "input-1" || tail != "/tmp/project" {
		t.Fatalf("unexpected split command arg: head=%q tail=%q ok=%v", head, tail, ok)
	}

	head, tail, ok = splitCommandArg("input-1")
	if ok || head != "input-1" || tail != "" {
		t.Fatalf("single command arg should have no tail: head=%q tail=%q ok=%v", head, tail, ok)
	}
}

func TestResolveApprovalCommandDefaultsToApprove(t *testing.T) {
	run := newActiveRun(&Client{}, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1")
	run.pending = map[string]*pendingServerRequest{
		"approval-1": {
			ID:         "approval-1",
			Method:     "item/commandExecution/requestApproval",
			ToolCallID: "cmd-1",
			ToolName:   "command_execution",
			Input:      map[string]any{"command": "git status"},
			Response:   make(chan any, 1),
		},
	}
	responseCh := run.pending["approval-1"].Response

	response, ok := parseApprovalCommandResponse("approval-1")
	if !ok || !run.resolvePendingResponse(response) {
		t.Fatal("approval command without explicit choice did not resolve")
	}

	select {
	case got := <-responseCh:
		response, ok := got.(aistream.ToolApprovalResponse)
		if !ok || response.ID != "approval-1" || !response.Approved || response.Always {
			t.Fatalf("unexpected approval response: %#v", got)
		}
	default:
		t.Fatal("resolved approval was not delivered to Codex waiter")
	}
}

func TestDynamicToolCallServerRequestStreamsUnsupportedToolCall(t *testing.T) {
	run := newActiveRun(&Client{}, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1")
	response, err := run.handleServerRequest(context.Background(), appserver.Message{
		ID:     "rpc-1",
		Method: "item/tool/call",
		Params: json.RawMessage(`{"threadId":"thread-1","turnId":"turn-1","callId":"call-1","namespace":"browser","tool":"open","arguments":{"url":"https://example.com"}}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	got, ok := response.(map[string]any)
	if !ok || got["success"] != false {
		t.Fatalf("unexpected dynamic tool response: %#v", response)
	}
	contentItems, ok := got["contentItems"].([]map[string]any)
	if !ok || len(contentItems) != 1 || contentItems[0]["type"] != "inputText" || contentItems[0]["text"] != unsupportedDynamicToolCallText {
		t.Fatalf("unexpected dynamic tool content items: %#v", got["contentItems"])
	}
	if !hasToolCallStartName(run.run.Events, "call-1", "browser: open") {
		t.Fatalf("dynamic tool request did not start visible AG-UI tool call: %#v", run.run.Events)
	}
	if !hasToolStartMetadataContaining(run.run.Events, "call-1", `"request":"item/tool/call"`) ||
		!hasToolStartMetadataContaining(run.run.Events, "call-1", `"callId":"call-1"`) ||
		!hasToolStartMetadataContaining(run.run.Events, "call-1", `"name":"browser: open"`) {
		t.Fatalf("dynamic tool request did not sync tool start metadata: %#v", run.run.Events)
	}
	if !hasToolArgsContaining(run.run.Events, "call-1", `"url":"https://example.com"`) || !hasToolArgsContaining(run.run.Events, "call-1", `"namespace":"browser"`) {
		t.Fatalf("dynamic tool request did not sync full input args: %#v", run.run.Events)
	}
	if !hasToolResultStateContaining(run.run.Events, "call-1", unsupportedDynamicToolCallText, agui.ToolResultStateError) {
		t.Fatalf("dynamic tool request did not stream unsupported error result: %#v", run.run.Events)
	}
	if !toolEventsInOrder(run.run.Events, "call-1", agui.EventToolCallStart, agui.EventToolCallArgs, agui.EventToolCallResult, agui.EventToolCallEnd) {
		t.Fatalf("dynamic tool request events out of order: %#v", run.run.Events)
	}
	if countToolResults(run.run.Events, "call-1") != 1 {
		t.Fatalf("dynamic tool request should have one visible result: %#v", run.run.Events)
	}
}

func TestParseCodexCommand(t *testing.T) {
	tests := []struct {
		name    string
		content *event.MessageEventContent
		want    codexCommand
		ok      bool
	}{
		{
			name:    "slash approvals",
			content: &event.MessageEventContent{Body: "/approvals"},
			want:    codexCommand{name: "approvals"},
			ok:      true,
		},
		{
			name:    "slash approve",
			content: &event.MessageEventContent{Body: "/approve approval-1 always"},
			want:    codexCommand{name: "approve", arg: "approval-1 always"},
			ok:      true,
		},
		{
			name:    "matrix command without slash",
			content: &event.MessageEventContent{MsgType: matrixCommandMsgType, Body: "approve approval-1 deny"},
			want:    codexCommand{name: "approve", arg: "approval-1 deny"},
			ok:      true,
		},
		{
			name: "structured approve",
			content: &event.MessageEventContent{MSC4391BotCommand: &event.MSC4391BotCommandInput{
				Command:   "approve",
				Arguments: json.RawMessage(`{"id":"approval-1","choice":"always"}`),
			}},
			want: codexCommand{name: "approve", arg: "approval-1 always"},
			ok:   true,
		},
		{
			name: "structured approve default",
			content: &event.MessageEventContent{MSC4391BotCommand: &event.MSC4391BotCommandInput{
				Command:   "approve",
				Arguments: json.RawMessage(`{"id":"approval-1"}`),
			}},
			want: codexCommand{name: "approve", arg: "approval-1"},
			ok:   true,
		},
		{
			name: "structured answer",
			content: &event.MessageEventContent{MSC4391BotCommand: &event.MSC4391BotCommandInput{
				Command:   "answer",
				Arguments: json.RawMessage(`{"id":"input-1","answer":"hello world"}`),
			}},
			want: codexCommand{name: "answer", arg: "input-1 hello world"},
			ok:   true,
		},
		{
			name: "structured stop",
			content: &event.MessageEventContent{MSC4391BotCommand: &event.MSC4391BotCommandInput{
				Command: "stop",
			}},
			want: codexCommand{name: "stop"},
			ok:   true,
		},
		{
			name:    "slash stop",
			content: &event.MessageEventContent{Body: "/stop"},
			want:    codexCommand{name: "stop"},
			ok:      true,
		},
		{
			name:    "abort is not a command",
			content: &event.MessageEventContent{Body: "/abort"},
			ok:      false,
		},
		{
			name:    "interrupt is not a command",
			content: &event.MessageEventContent{Body: "/interrupt"},
			ok:      false,
		},
		{
			name:    "normal message",
			content: &event.MessageEventContent{Body: "approve approval-1 deny"},
			ok:      false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := parseCodexCommand(tc.content)
			if ok != tc.ok || got != tc.want {
				t.Fatalf("unexpected command: %#v ok=%v", got, ok)
			}
		})
	}
}

func TestCodexCommandFromRawBeeperCommand(t *testing.T) {
	raw := map[string]any{
		"msgtype": string(matrixCommandMsgType),
		"command": "approve",
		"arguments": map[string]any{
			"id":     "approval-1",
			"choice": "always",
		},
	}
	command, ok := codexCommandFromRawContent(raw)
	if !ok || command != (codexCommand{name: "approve", arg: "approval-1 always"}) {
		t.Fatalf("unexpected raw command: ok=%v command=%#v", ok, command)
	}
}

func TestCodexCommandFromRawBeeperCommandStringArguments(t *testing.T) {
	raw := map[string]any{
		"msgtype":   string(matrixCommandMsgType),
		"command":   "answer",
		"arguments": "input-1 hello world",
	}
	command, ok := codexCommandFromRawContent(raw)
	if !ok || command != (codexCommand{name: "answer", arg: "input-1 hello world"}) {
		t.Fatalf("unexpected raw command: ok=%v command=%#v", ok, command)
	}
}

func TestNestedRawCodexCommand(t *testing.T) {
	raw := map[string]any{"org.matrix.msc4391.command": map[string]any{"command": "approve"}}
	if got := nestedRawCodexCommand(raw); got["command"] != "approve" {
		t.Fatalf("unexpected nested raw command: %#v", got)
	}
	if got := nestedRawCodexCommand(map[string]any{}); len(got) != 0 {
		t.Fatalf("missing nested raw command should stay empty, got %#v", got)
	}
}

func TestRawCommandArgumentsUsesFirstNonNilAlias(t *testing.T) {
	raw := map[string]any{
		"arguments": nil,
		"args":      "input-1 from args",
		"parameters": map[string]any{
			"id": "input-1",
		},
	}
	if got := rawCommandArguments(raw); got != "input-1 from args" {
		t.Fatalf("unexpected raw command arguments: %#v", got)
	}
}

func TestCodexCommandFromRawArgValue(t *testing.T) {
	raw := map[string]any{"id": "approval-1", "choice": "deny"}
	if got := codexCommandFromRawArgValue("approve", "approval-1 always", raw); got != (codexCommand{name: "approve", arg: "approval-1 always"}) {
		t.Fatalf("unexpected string raw arg command: %#v", got)
	}
	if got := codexCommandFromRawArgValue("approve", nil, raw); got != (codexCommand{name: "approve", arg: "approval-1 deny"}) {
		t.Fatalf("unexpected fallback raw arg command: %#v", got)
	}
}

func TestCodexRequestArgCommand(t *testing.T) {
	got := codexRequestArgCommand("approve", map[string]any{"id": "approval-1", "choice": "deny"}, "approval_id", "choice")
	if got != (codexCommand{name: "approve", arg: "approval-1 deny"}) {
		t.Fatalf("unexpected primary ID request command: %#v", got)
	}
	got = codexRequestArgCommand("answer", map[string]any{"request_id": "input-1", "answer": "hello"}, "request_id", "answer")
	if got != (codexCommand{name: "answer", arg: "input-1 hello"}) {
		t.Fatalf("unexpected legacy ID request command: %#v", got)
	}
}

func TestCodexCommandFromRawBeeperCommandLegacyIDAliases(t *testing.T) {
	approve, ok := codexCommandFromRawContent(map[string]any{
		"msgtype": string(matrixCommandMsgType),
		"command": "approve",
		"args": map[string]any{
			"approval_id": "approval-1",
			"choice":      "deny",
		},
	})
	if !ok || approve != (codexCommand{name: "approve", arg: "approval-1 deny"}) {
		t.Fatalf("unexpected legacy approve command: ok=%v command=%#v", ok, approve)
	}
	answer, ok := codexCommandFromRawContent(map[string]any{
		"msgtype": string(matrixCommandMsgType),
		"command": "answer",
		"args": map[string]any{
			"request_id": "input-1",
			"answer":     "hello world",
		},
	})
	if !ok || answer != (codexCommand{name: "answer", arg: "input-1 hello world"}) {
		t.Fatalf("unexpected legacy answer command: ok=%v command=%#v", ok, answer)
	}
}

func TestCodexCommandDescriptionsAreValid(t *testing.T) {
	seen := map[string]*cmdschema.EventContent{}
	for _, command := range codexCommandDescriptions() {
		if err := command.Validate(); err != nil {
			t.Fatalf("invalid command %q: %v", command.Command, err)
		}
		if seen[command.Command] != nil {
			t.Fatalf("duplicate command %q", command.Command)
		}
		seen[command.Command] = command
	}
	for _, want := range []string{"approvals", "approve", "answer", "stop"} {
		if seen[want] == nil {
			t.Fatalf("missing command description %q", want)
		}
	}
	approveKeys := make([]string, 0, len(seen["approve"].Parameters))
	for _, param := range seen["approve"].Parameters {
		approveKeys = append(approveKeys, param.Key)
	}
	if !reflect.DeepEqual(approveKeys, []string{"id", "choice"}) {
		t.Fatalf("approve command parameters changed: %#v", approveKeys)
	}
	if !seen["approve"].Parameters[1].Optional {
		t.Fatal("approve choice should stay optional so clients can send a one-tap approval")
	}
	answerKeys := make([]string, 0, len(seen["answer"].Parameters))
	for _, param := range seen["answer"].Parameters {
		answerKeys = append(answerKeys, param.Key)
	}
	if !reflect.DeepEqual(answerKeys, []string{"id", "answer"}) || seen["answer"].TailParam != "answer" {
		t.Fatalf("answer command parameters changed: keys=%#v tail=%q", answerKeys, seen["answer"].TailParam)
	}
	if len(seen["stop"].Aliases) != 0 {
		t.Fatalf("stop command should not advertise aliases: %#v", seen["stop"].Aliases)
	}
}

func TestDirectServerRequestErrors(t *testing.T) {
	code, message, ok := directServerRequestError("account/chatgptAuthTokens/refresh")
	if !ok || code == 0 || !strings.Contains(message, "local Codex login") {
		t.Fatalf("unexpected auth refresh direct error: code=%d message=%q ok=%v", code, message, ok)
	}
	if !isHandledCodexServerRequest("account/chatgptAuthTokens/refresh") {
		t.Fatal("auth refresh request should be covered as a handled server request")
	}

	code, message, ok = directServerRequestError("attestation/generate")
	if !ok || code == 0 || !strings.Contains(message, "attestation tokens") {
		t.Fatalf("unexpected attestation direct error: code=%d message=%q ok=%v", code, message, ok)
	}
}

func TestServerRequestErrorMetadata(t *testing.T) {
	got := serverRequestErrorMetadata("item/tool/call")
	want := map[string]any{"method": "item/tool/call"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected server request error metadata:\n got: %#v\nwant: %#v", got, want)
	}
}

func TestServerRequestIDNormalizesJSONRPCID(t *testing.T) {
	if got := serverRequestID(float64(42)); got != "42" {
		t.Fatalf("unexpected numeric request ID: %q", got)
	}
	if got := serverRequestID(" request-1 "); got != "request-1" {
		t.Fatalf("unexpected string request ID: %q", got)
	}
}

func TestCommandHandledResponseUsesTypedMetadata(t *testing.T) {
	client := &Client{UserLogin: testUserLogin("codex")}
	msg := testMatrixMessage("thread-1", "/approvals")
	resp := client.commandHandledResponse(msg, "approvals")
	meta, ok := resp.DB.Metadata.(*MessageMetadata)
	if !ok || meta.Role != "command" || meta.ThreadID != "thread-1" || meta.StreamStatus != "approvals" {
		t.Fatalf("unexpected command metadata: %#v", resp.DB.Metadata)
	}
	if resp.StreamOrder != time.UnixMilli(msg.Event.Timestamp).UnixNano() {
		t.Fatalf("unexpected command stream order: %d", resp.StreamOrder)
	}
}

func TestHandleMatrixMessageShortCircuitsNoSessionCommands(t *testing.T) {
	client := &Client{UserLogin: testUserLogin("codex")}
	tests := []struct {
		name    string
		content *event.MessageEventContent
	}{
		{
			name:    "slash",
			content: &event.MessageEventContent{Body: "/approvals"},
		},
		{
			name:    "beeper command",
			content: &event.MessageEventContent{MsgType: matrixCommandMsgType, Body: "approvals"},
		},
		{
			name:    "structured",
			content: &event.MessageEventContent{MSC4391BotCommand: &event.MSC4391BotCommandInput{Command: "approvals"}},
		},
		{
			name:    "approve",
			content: &event.MessageEventContent{Body: "/approve approval-1 approve"},
		},
		{
			name:    "answer",
			content: &event.MessageEventContent{MsgType: matrixCommandMsgType, Body: "answer input-1 hello"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			msg := testMatrixMessage("", "")
			msg.Content = tc.content
			resp, err := client.HandleMatrixMessage(context.Background(), msg)
			if err != nil {
				t.Fatal(err)
			}
			meta, ok := resp.DB.Metadata.(*MessageMetadata)
			if !ok || meta.Role != "command" || meta.StreamStatus != "no_session" {
				t.Fatalf("command did not short-circuit to command metadata: %#v", resp.DB.Metadata)
			}
		})
	}
}

func TestHandleMatrixMessageAcksCommandFailures(t *testing.T) {
	client := &Client{Main: &Connector{}, UserLogin: testUserLogin("codex")}
	tests := []struct {
		name   string
		body   string
		status string
	}{
		{name: "approve without active turn", body: "/approve approval-1 approve", status: "no_pending_approval"},
		{name: "answer without active turn", body: "/answer input-1 hello", status: "no_pending_input"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			msg := testMatrixMessage("thread-1", tc.body)
			resp, err := client.HandleMatrixMessage(context.Background(), msg)
			if err != nil {
				t.Fatal(err)
			}
			meta, ok := resp.DB.Metadata.(*MessageMetadata)
			if !ok || meta.Role != "command" || meta.ThreadID != "thread-1" || meta.StreamStatus != tc.status {
				t.Fatalf("command failure did not ack with metadata: %#v", resp.DB.Metadata)
			}
		})
	}

	run := &activeRun{pending: map[string]*pendingServerRequest{}}
	client.Main.active = map[string]*activeRun{"thread-1": run}
	for _, tc := range []struct {
		name   string
		body   string
		status string
	}{
		{name: "invalid approval", body: "/approve approval-1 approve", status: "invalid_approval"},
		{name: "invalid answer", body: "/answer input-1 hello", status: "invalid_answer"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			msg := testMatrixMessage("thread-1", tc.body)
			resp, err := client.HandleMatrixMessage(context.Background(), msg)
			if err != nil {
				t.Fatal(err)
			}
			meta, ok := resp.DB.Metadata.(*MessageMetadata)
			if !ok || meta.Role != "command" || meta.ThreadID != "thread-1" || meta.StreamStatus != tc.status {
				t.Fatalf("invalid command did not ack with metadata: %#v", resp.DB.Metadata)
			}
		})
	}
}

func TestHandleMatrixMessageResolvesStructuredPendingCommands(t *testing.T) {
	client := &Client{Main: &Connector{active: map[string]*activeRun{}}, UserLogin: testUserLogin("codex")}
	run := newActiveRun(client, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1")
	run.pending = map[string]*pendingServerRequest{
		"approval-1": {
			ID:         "approval-1",
			Method:     "item/commandExecution/requestApproval",
			ToolCallID: "cmd-1",
			ToolName:   "command_execution",
			Input:      map[string]any{"command": "git status"},
			Response:   make(chan any, 1),
		},
		"input-1": {
			ID:          "input-1",
			Method:      "item/tool/requestUserInput",
			ToolCallID:  "input-1",
			ToolName:    "request_user_input",
			Input:       map[string]any{"prompt": "Project directory?"},
			QuestionIDs: []string{"path"},
			Response:    make(chan any, 1),
		},
	}
	approvalCh := run.pending["approval-1"].Response
	inputCh := run.pending["input-1"].Response
	client.Main.active["thread-1"] = run

	approvalMsg := testMatrixMessage("thread-1", "")
	approvalMsg.Content = &event.MessageEventContent{MSC4391BotCommand: &event.MSC4391BotCommandInput{
		Command:   "approve",
		Arguments: json.RawMessage(`{"id":"approval-1"}`),
	}}
	resp, err := client.HandleMatrixMessage(context.Background(), approvalMsg)
	if err != nil {
		t.Fatal(err)
	}
	if meta, ok := resp.DB.Metadata.(*MessageMetadata); !ok || meta.StreamStatus != "approve" {
		t.Fatalf("structured approval was not acked as approve: %#v", resp.DB.Metadata)
	}
	select {
	case got := <-approvalCh:
		response, ok := got.(aistream.ToolApprovalResponse)
		if !ok || !response.Approved || response.ID != "approval-1" {
			t.Fatalf("structured approval did not resolve with approval response: %#v", got)
		}
	default:
		t.Fatal("structured approval did not reach pending request")
	}
	if !hasCodexRunStateDelta(run.run.Events, "command/approve", "id", "approval-1") ||
		!hasCodexRunStateDelta(run.run.Events, "command/approve", "choice", "approve") {
		t.Fatalf("structured approval command was not synced as AG-UI client state: %#v", run.run.Events)
	}

	answerMsg := testMatrixMessage("thread-1", "")
	answerMsg.Content = &event.MessageEventContent{MSC4391BotCommand: &event.MSC4391BotCommandInput{
		Command:   "answer",
		Arguments: json.RawMessage(`{"id":"input-1","answer":"/tmp/project"}`),
	}}
	resp, err = client.HandleMatrixMessage(context.Background(), answerMsg)
	if err != nil {
		t.Fatal(err)
	}
	if meta, ok := resp.DB.Metadata.(*MessageMetadata); !ok || meta.StreamStatus != "answer" {
		t.Fatalf("structured answer was not acked as answer: %#v", resp.DB.Metadata)
	}
	select {
	case got := <-inputCh:
		want := map[string]any{"path": map[string]any{"answers": []string{"/tmp/project"}}}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("structured answer did not resolve input:\n got: %#v\nwant: %#v", got, want)
		}
	default:
		t.Fatal("structured answer did not reach pending request")
	}
	if !hasCodexRunStateDelta(run.run.Events, "command/answer", "id", "input-1") {
		t.Fatalf("structured answer command was not synced as AG-UI client state: %#v", run.run.Events)
	}
	for _, event := range run.run.Events {
		if event.Type() != agui.EventStateDelta {
			continue
		}
		delta, _ := event.Get("delta").(map[string]any)
		codexRun, _ := delta["codexRun"].(map[string]any)
		if strings.Contains(anyString(codexRun["command/answer"]), "/tmp/project") {
			t.Fatalf("answer client request state should not duplicate answer text: %#v", run.run.Events)
		}
	}
}

func TestPendingRequestsText(t *testing.T) {
	run := &activeRun{pending: map[string]*pendingServerRequest{
		"approval-1": {
			ID:     "approval-1",
			Method: "item/commandExecution/requestApproval",
			Input:  map[string]any{"command": "git status", "cwd": "/tmp/project"},
		},
		"input-1": {
			ID:          "input-1",
			Method:      "item/tool/requestUserInput",
			ToolName:    "ask_user",
			Input:       map[string]any{"message": "Pick a branch"},
			QuestionIDs: []string{"branch"},
		},
	}}
	got := run.pendingRequestsText()
	for _, want := range []string{
		"Pending Codex requests:",
		"Approve command: git status",
		"/approve approval-1 approve",
		"/approve approval-1 always",
		"/approve approval-1 deny",
		"Pick a branch",
		"Reply with `/answer input-1 <answer>`.",
		"/answer input-1 <answer>",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("pending requests text missing %q:\n%s", want, got)
		}
	}
}

func TestPendingRequestIntroLines(t *testing.T) {
	got := pendingRequestIntroLines("request-1", "Approve command?")
	want := []string{"", "### request-1", "Approve command?"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected pending request intro:\n got: %#v\nwant: %#v", got, want)
	}
}

func TestPendingRequestsTextIncludesMCPElicitationURL(t *testing.T) {
	run := &activeRun{pending: map[string]*pendingServerRequest{
		"elicit-1": {
			ID:       "elicit-1",
			Method:   "mcpServer/elicitation/request",
			ToolName: "mcp:github",
			Input: map[string]any{
				"mode":       "url",
				"message":    "Authorize GitHub",
				"url":        "https://github.com/login/device",
				"serverName": "github",
			},
		},
	}}
	got := run.pendingRequestsText()
	for _, want := range []string{
		"Authorize GitHub",
		"URL: https://github.com/login/device",
		"Reply with `/answer elicit-1 <answer>`.",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("pending requests text missing %q:\n%s", want, got)
		}
	}
}

func TestCommandNoticeRunUsesBeeperStreamFinal(t *testing.T) {
	run := commandNoticeRun("notice", "message-1", "thread-1", "openai/gpt-5", time.Unix(0, 0))
	if run.ThreadID != "thread-1" || run.MessageID != "message-1" {
		t.Fatalf("unexpected notice run IDs: %#v", run)
	}
	content, extra, err := matrixFinalContent(context.Background(), nil, nil, run)
	if err != nil {
		t.Fatal(err)
	}
	assertCodexProfile(t, content)
	ai, ok := extra[aistream.BeeperAIKey].(aistream.BeeperAI)
	if !ok {
		t.Fatalf("notice missing Beeper AI final payload: %#v", extra)
	}
	if ai.Kind != aistream.AIKindFinal || ai.ThreadID != "thread-1" || ai.Message == nil {
		t.Fatalf("unexpected notice AI payload: %#v", ai)
	}
	if ai.Model != "openai/gpt-5" {
		t.Fatalf("notice AI payload lost model: %#v", ai)
	}
	if len(ai.Message.Parts) != 1 || ai.Message.Parts[0]["content"] != "notice" {
		t.Fatalf("notice final parts not projected from stream events: %#v", ai.Message.Parts)
	}
}

func TestApprovalPromptReferencesAnchorEvent(t *testing.T) {
	run := &activeRun{
		portalKey:  projectPortalKey("/tmp/project", "codex"),
		threadID:   "thread-1",
		turnID:     "turn-1",
		anchorMXID: "$anchor:example.com",
	}
	msg := run.approvalPromptMessage(aistream.ApprovalContext{
		ID:        "approval-1",
		ThreadID:  "thread-1",
		RunID:     "turn-1",
		MessageID: "assistant-1",
		ToolName:  "command_execution",
		Model:     "openai/gpt-5",
		Choices:   aistream.DefaultApprovalChoices(),
	}, time.Unix(0, 0))
	converted, err := msg.ConvertMessage(context.Background(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	part := converted.Parts[0]
	relatesTo := part.Content.RelatesTo
	if relatesTo == nil || relatesTo.Type != aimatrix.ApprovalRelationType || relatesTo.EventID != "$anchor:example.com" {
		t.Fatalf("approval prompt does not reference anchor: %#v", relatesTo)
	}
	assertCodexProfile(t, part.Content)
	meta, ok := part.DBMetadata.(*MessageMetadata)
	if !ok || meta.Role != "approval" || meta.ThreadID != "thread-1" || meta.TurnID != "turn-1" || meta.Approval == nil || meta.Approval.ID != "approval-1" || meta.Approval.Model != "openai/gpt-5" {
		t.Fatalf("approval prompt has wrong DB metadata: %#v", part.DBMetadata)
	}
}

func TestCodexApprovalResponseMapping(t *testing.T) {
	tests := []struct {
		name     string
		method   string
		response aistream.ToolApprovalResponse
		want     string
	}{
		{
			name:     "command approve",
			method:   "item/commandExecution/requestApproval",
			response: aistream.ToolApprovalResponse{ID: "a", Approved: true},
			want:     "accept",
		},
		{
			name:     "command always",
			method:   "item/commandExecution/requestApproval",
			response: aistream.ToolApprovalResponse{ID: "a", Approved: true, Always: true},
			want:     "acceptForSession",
		},
		{
			name:     "command deny",
			method:   "item/commandExecution/requestApproval",
			response: aistream.ToolApprovalResponse{ID: "a", Approved: false},
			want:     "decline",
		},
		{
			name:     "legacy command always",
			method:   "execCommandApproval",
			response: aistream.ToolApprovalResponse{ID: "a", Approved: true, Always: true},
			want:     "approved_for_session",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pending := &pendingServerRequest{Method: tc.method}
			if got := codexApprovalResponse(pending, tc.response).(map[string]any)["decision"]; got != tc.want {
				t.Fatalf("unexpected decision: got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestPermissionApprovalResponseMapping(t *testing.T) {
	pending := &pendingServerRequest{
		Method: "item/permissions/requestApproval",
		Input:  map[string]any{"permissions": map[string]any{"network": true}},
	}
	got := codexApprovalResponse(pending, aistream.ToolApprovalResponse{Approved: true, Always: true}).(map[string]any)
	want := map[string]any{"permissions": map[string]any{"network": true}, "scope": "session"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected permissions response:\n got: %#v\nwant: %#v", got, want)
	}

	got = codexApprovalResponse(pending, aistream.ToolApprovalResponse{Approved: false}).(map[string]any)
	want = map[string]any{"permissions": map[string]any{}, "scope": "turn"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected denied permissions response:\n got: %#v\nwant: %#v", got, want)
	}
}

func TestRequestedApprovalPermissions(t *testing.T) {
	got, ok := requestedApprovalPermissions(map[string]any{"permissions": map[string]any{"network": true}})
	if !ok || !reflect.DeepEqual(got, map[string]any{"network": true}) {
		t.Fatalf("unexpected requested permissions: %#v ok=%v", got, ok)
	}
	if got, ok := requestedApprovalPermissions(map[string]any{}); ok || got != nil {
		t.Fatalf("missing permissions should not match: %#v ok=%v", got, ok)
	}
}

func TestAnswerResponse(t *testing.T) {
	pending := &pendingServerRequest{Method: "item/tool/requestUserInput", QuestionIDs: []string{"path"}}
	got, err := answerResponse(pending, "/tmp/project")
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"path": map[string]any{"answers": []string{"/tmp/project"}}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected answer response:\n got: %#v\nwant: %#v", got, want)
	}

	pending.QuestionIDs = []string{"name", "cwd"}
	got, err = answerResponse(pending, "name=bridge cwd=/tmp/project")
	if err != nil {
		t.Fatal(err)
	}
	want = map[string]any{
		"name": map[string]any{"answers": []string{"bridge"}},
		"cwd":  map[string]any{"answers": []string{"/tmp/project"}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected multi-answer response:\n got: %#v\nwant: %#v", got, want)
	}

	got, err = answerResponse(pending, `name="bridge bot" cwd="/tmp/my project"`)
	if err != nil {
		t.Fatal(err)
	}
	want = map[string]any{
		"name": map[string]any{"answers": []string{"bridge bot"}},
		"cwd":  map[string]any{"answers": []string{"/tmp/my project"}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected quoted multi-answer response:\n got: %#v\nwant: %#v", got, want)
	}

	if _, err = answerResponse(pending, `name="bridge bot cwd=/tmp/project`); err == nil {
		t.Fatal("unterminated quoted value should fail")
	}
}

func TestNamedAnswerValuesDefaultsEmptyQuestionIDs(t *testing.T) {
	got, err := namedAnswerValues(nil, "plain answer")
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{defaultAnswerQuestionID: "plain answer"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected default answer values:\n got: %#v\nwant: %#v", got, want)
	}
}

func TestSingleAnswerID(t *testing.T) {
	if got := singleAnswerID([]string{"path"}); got != "path" {
		t.Fatalf("unexpected single answer ID: %q", got)
	}
	if got := singleAnswerID(nil); got != defaultAnswerQuestionID {
		t.Fatalf("unexpected default answer ID: %q", got)
	}
}

func TestHasAnswerAssignments(t *testing.T) {
	if !hasAnswerAssignments("name=bridge") {
		t.Fatal("expected assignment syntax to be detected")
	}
	if hasAnswerAssignments("plain answer") {
		t.Fatal("plain answer should not be treated as assignment syntax")
	}
}

func TestAnswerKeyEnd(t *testing.T) {
	if got := answerKeyEnd("name=bridge", 0); got != len("name") {
		t.Fatalf("unexpected key end before equals: %d", got)
	}
	if got := answerKeyEnd("name bridge", 0); got != len("name") {
		t.Fatalf("unexpected key end before space: %d", got)
	}
}

func TestInvalidAnswerKeyRange(t *testing.T) {
	if !invalidAnswerKeyRange("=value", 0, 0) {
		t.Fatal("empty key range before equals should be invalid")
	}
	if !invalidAnswerKeyRange("name", 0, len("name")) {
		t.Fatal("missing equals should be an invalid answer key range")
	}
	if invalidAnswerKeyRange("name=value", 0, len("name")) {
		t.Fatal("valid answer key range should not be invalid")
	}
}

func TestIsQuotedAnswerValue(t *testing.T) {
	if !isQuotedAnswerValue('"') || !isQuotedAnswerValue('\'') {
		t.Fatal("single and double quotes should start quoted answer values")
	}
	if isQuotedAnswerValue('a') {
		t.Fatal("plain text should not start a quoted answer value")
	}
}

func TestUnquotedAnswerValueEnd(t *testing.T) {
	if got := unquotedAnswerValueEnd("bridge", 0); got != len("bridge") {
		t.Fatalf("unexpected end for full unquoted value: %d", got)
	}
	if got := unquotedAnswerValueEnd("name=value next=answer", len("name=")); got != len("name=value") {
		t.Fatalf("unexpected end before next assignment: %d", got)
	}
}

func TestQuotedAnswerByte(t *testing.T) {
	if ch, next := quotedAnswerByte(`\"`, '\\', 1); ch != '"' || next != 2 {
		t.Fatalf("unexpected escaped quoted byte: ch=%q next=%d", ch, next)
	}
	if ch, next := quotedAnswerByte("abc", 'a', 1); ch != 'a' || next != 1 {
		t.Fatalf("unexpected plain quoted byte: ch=%q next=%d", ch, next)
	}
}

func TestInvalidQuotedAnswerSuffix(t *testing.T) {
	if invalidQuotedAnswerSuffix(`name="bridge"`, len(`name="bridge"`)) {
		t.Fatal("quoted value at end should not have an invalid suffix")
	}
	if invalidQuotedAnswerSuffix(`name="bridge" cwd=/tmp`, len(`name="bridge"`)) {
		t.Fatal("quoted value followed by space should not have an invalid suffix")
	}
	if !invalidQuotedAnswerSuffix(`name="bridge"cwd=/tmp`, len(`name="bridge"`)) {
		t.Fatal("quoted value followed by non-space should have an invalid suffix")
	}
}

func TestMCPDefaultAnswerResponse(t *testing.T) {
	got := mcpDefaultAnswerResponse("plain answer")
	want := map[string]any{defaultAnswerQuestionID: "plain answer"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected MCP default answer response:\n got: %#v\nwant: %#v", got, want)
	}
}

func TestAnswerResolutionClearsStreamInterruptWithoutChangingCodexResponse(t *testing.T) {
	run := newActiveRun(&Client{}, projectPortalKey("/tmp/project", "codex"), "thread-1", "turn-1")
	answer := map[string]any{"path": map[string]any{"answers": []string{"/tmp/project"}}}
	run.pending = map[string]*pendingServerRequest{
		"input-1": {
			ID:         "input-1",
			Method:     "item/tool/requestUserInput",
			ToolCallID: "input-1",
			ToolName:   "request_user_input",
			Input:      map[string]any{"prompt": "Project directory?"},
			Response:   make(chan any, 1),
		},
	}
	responseCh := run.pending["input-1"].Response
	run.writer.ToolApprovalRequestedWithRequest(aistream.ApprovalRequest{
		ID:         "input-1",
		ToolCallID: "input-1",
		ToolName:   "request_user_input",
		Input:      run.pending["input-1"].Input,
		Approval:   aistream.ToolApproval{ID: "input-1", NeedsApproval: true},
	})
	run.writer.InterruptWithUsage(nil)
	if len(run.run.Interrupts) != 1 {
		t.Fatalf("expected pending interrupt before answer, got %#v", run.run.Interrupts)
	}

	if !run.resolvePendingValue("input-1", answer) {
		t.Fatal("answer did not resolve pending request")
	}

	if len(run.run.Interrupts) != 0 {
		t.Fatalf("answer left stale interrupt: %#v", run.run.Interrupts)
	}
	if run.run.Status.State != "streaming" {
		t.Fatalf("answer should resume stream, got status %#v", run.run.Status)
	}
	foundToolResult := false
	for _, event := range run.run.Events {
		if event.Type() == agui.EventToolCallResult && event.Get("toolCallId") == "input-1" {
			foundToolResult = true
			break
		}
	}
	if !foundToolResult {
		t.Fatalf("answer did not publish tool result event: %#v", run.run.Events)
	}
	select {
	case got := <-responseCh:
		if !reflect.DeepEqual(got, answer) {
			t.Fatalf("Codex response changed:\n got: %#v\nwant: %#v", got, answer)
		}
	default:
		t.Fatal("resolved answer was not delivered to Codex waiter")
	}
}

func TestPendingApprovalResponseWrapsAnswerMetadata(t *testing.T) {
	answer := map[string]any{"path": map[string]any{"answers": []string{"/tmp/project"}}}
	pending := &pendingServerRequest{
		ID:     "input-1",
		Method: "item/tool/requestUserInput",
	}

	response, ok := pendingApprovalResponse(pending, answer)
	if !ok || response.ID != "input-1" || !response.Approved || response.Choice != "answer" {
		t.Fatalf("unexpected pending approval response: %#v ok=%v", response, ok)
	}
	wantMeta := map[string]any{"method": "item/tool/requestUserInput", "response": answer}
	if !reflect.DeepEqual(response.Metadata, wantMeta) {
		t.Fatalf("unexpected pending response metadata:\n got: %#v\nwant: %#v", response.Metadata, wantMeta)
	}
}

func TestWrappedPendingApprovalResponse(t *testing.T) {
	answer := map[string]any{"path": map[string]any{"answers": []string{"/tmp/project"}}}
	pending := &pendingServerRequest{ID: "input-1", Method: "item/tool/requestUserInput"}
	response, ok := wrappedPendingApprovalResponse(pending, answer)
	if !ok || response.ID != "input-1" || !response.Approved || response.Choice != "answer" {
		t.Fatalf("unexpected wrapped pending approval response: %#v ok=%v", response, ok)
	}
	if response.RespondedAt == "" {
		t.Fatal("wrapped pending approval response should include response time")
	}
	if _, ok := wrappedPendingApprovalResponse(nil, answer); ok {
		t.Fatal("nil pending request should not produce a wrapped response")
	}
}

func TestUserInputApprovalResponseBecomesEmptyAnswers(t *testing.T) {
	pending := &pendingServerRequest{Method: "item/tool/requestUserInput"}
	got := codexInputResponse(pending, aistream.ToolApprovalResponse{ID: "input-1", Approved: false}).(map[string]any)
	want := map[string]any{"answers": map[string]any{}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected denied input response:\n got: %#v\nwant: %#v", got, want)
	}
}

func TestNewApprovalRequestUsesCodexIDs(t *testing.T) {
	run := &activeRun{turnID: "turn-1"}
	pending, request, err := run.newApprovalRequest("item/commandExecution/requestApproval", "rpc-approval", []byte(`{
		"threadId": "thread-1",
		"turnId": "turn-1",
		"itemId": "item-1",
		"approvalId": "approval-1",
		"command": "git status",
		"cwd": "/tmp/project",
		"reason": "needs shell"
	}`))
	if err != nil {
		t.Fatal(err)
	}
	if pending.ID != "approval-1" || pending.ToolCallID != "item-1" || pending.ToolName != "command_execution" {
		t.Fatalf("unexpected pending request: %#v", pending)
	}
	if request.ID != "approval-1" || request.ToolCallID != "item-1" || request.Description != "needs shell" {
		t.Fatalf("unexpected approval request: %#v", request)
	}
	if request.Title != "Approve command: git status" {
		t.Fatalf("unexpected title: %q", request.Title)
	}
}

func TestCommandApprovalTitle(t *testing.T) {
	if got := commandApprovalTitle(map[string]any{"command": "git status"}); got != "Approve command: git status" {
		t.Fatalf("unexpected command approval title: %q", got)
	}
	if got := commandApprovalTitle(map[string]any{}); got != "Approve command?" {
		t.Fatalf("unexpected fallback command approval title: %q", got)
	}
}

func TestApprovalPlanSkipsEmptyLabelValues(t *testing.T) {
	if got := approvalPlan("item/commandExecution/requestApproval", map[string]any{}); got != "item/commandExecution/requestApproval" {
		t.Fatalf("unexpected empty approval plan: %q", got)
	}
	got := approvalPlan("item/commandExecution/requestApproval", map[string]any{
		"command": "git status",
		"reason":  "needs shell",
	})
	want := "Command: git status\nReason: needs shell"
	if got != want {
		t.Fatalf("unexpected approval plan:\n got: %q\nwant: %q", got, want)
	}
}

func TestJoinLinesOrFallback(t *testing.T) {
	if got := joinLinesOrFallback(nil, "fallback"); got != "fallback" {
		t.Fatalf("unexpected fallback line join: %q", got)
	}
	if got := joinLinesOrFallback([]string{"one", "two"}, "fallback"); got != "one\ntwo" {
		t.Fatalf("unexpected joined lines: %q", got)
	}
}

func TestNewApprovalRequestFallsBackToRPCRequestID(t *testing.T) {
	run := &activeRun{turnID: "turn-1"}
	pending, request, err := run.newApprovalRequest("item/commandExecution/requestApproval", "rpc-approval", []byte(`{
		"threadId": "thread-1",
		"turnId": "turn-1",
		"command": "git status"
	}`))
	if err != nil {
		t.Fatal(err)
	}
	if pending.ID != "rpc-approval" || request.ID != "rpc-approval" {
		t.Fatalf("approval did not use RPC fallback ID: pending=%#v request=%#v", pending, request)
	}
}

func TestNewInputRequestUsesRPCIDForMCPFormElicitation(t *testing.T) {
	run := &activeRun{turnID: "turn-1"}
	pending, request, err := run.newInputRequest("mcpServer/elicitation/request", "rpc-elicit-1", []byte(`{
		"threadId": "thread-1",
		"turnId": "turn-1",
		"serverName": "github",
		"mode": "form",
		"message": "GitHub login",
		"requestedSchema": {
			"type":"object",
			"properties":{
				"repo":{"type":"string","title":"Repository"},
				"branch":{"type":"string","description":"Branch name"}
			},
			"required":["repo"]
		}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	if pending.ID != "rpc-elicit-1" || request.ID != "rpc-elicit-1" || request.ToolCallID != "rpc-elicit-1" {
		t.Fatalf("MCP form elicitation did not use RPC request ID: pending=%#v request=%#v", pending, request)
	}
	if request.ToolName != "mcp:github" || request.Title != "GitHub login" {
		t.Fatalf("MCP form elicitation lost context: %#v", request)
	}
	if !reflect.DeepEqual(pending.QuestionIDs, []string{"branch", "repo"}) {
		t.Fatalf("MCP form elicitation did not expose schema fields: %#v", pending.QuestionIDs)
	}
	for _, want := range []string{"GitHub login", "branch - Branch name", "repo (required) - Repository", "/answer rpc-elicit-1 question_id=answer"} {
		if !strings.Contains(request.PlanText, want) {
			t.Fatalf("MCP form elicitation prompt missing %q: %#v", want, request)
		}
	}
}

func TestMCPElicitationFieldName(t *testing.T) {
	if got := mcpElicitationFieldName("repo (required) - Repository"); got != "repo" {
		t.Fatalf("unexpected MCP field name: %q", got)
	}
	if got := mcpElicitationFieldName(""); got != "" {
		t.Fatalf("empty MCP field descriptor should stay empty, got %q", got)
	}
}

func TestUseMCPFieldQuestionIDs(t *testing.T) {
	if !useMCPFieldQuestionIDs("mcpServer/elicitation/request", nil) {
		t.Fatal("MCP elicitation without explicit questions should use schema field IDs")
	}
	if useMCPFieldQuestionIDs("mcpServer/elicitation/request", []string{"repo"}) {
		t.Fatal("explicit question IDs should win over schema field IDs")
	}
	if useMCPFieldQuestionIDs("item/tool/requestUserInput", nil) {
		t.Fatal("non-MCP input should not use schema field IDs")
	}
}

func TestMCPElicitationProperties(t *testing.T) {
	schema := map[string]any{"properties": map[string]any{"repo": map[string]any{"title": "Repository"}}}
	if got := mcpElicitationProperties(schema); len(got) != 1 || got["repo"] == nil {
		t.Fatalf("unexpected MCP elicitation properties: %#v", got)
	}
	if got := mcpElicitationProperties(map[string]any{}); len(got) != 0 {
		t.Fatalf("missing properties should return empty map, got %#v", got)
	}
}

func TestMCPElicitationRequiredSuffix(t *testing.T) {
	if got := mcpElicitationRequiredSuffix("repo", []string{"branch", "repo"}); got != " (required)" {
		t.Fatalf("unexpected required suffix: %q", got)
	}
	if got := mcpElicitationRequiredSuffix("owner", []string{"branch", "repo"}); got != "" {
		t.Fatalf("unexpected optional suffix: %q", got)
	}
}

func TestMCPElicitationDetailSuffix(t *testing.T) {
	if got := mcpElicitationDetailSuffix(map[string]any{"title": "Repository", "description": "Repo name"}); got != " - Repository" {
		t.Fatalf("unexpected title detail suffix: %q", got)
	}
	if got := mcpElicitationDetailSuffix(map[string]any{"description": "Branch name"}); got != " - Branch name" {
		t.Fatalf("unexpected description detail suffix: %q", got)
	}
	if got := mcpElicitationDetailSuffix(map[string]any{"title": "  "}); got != "" {
		t.Fatalf("blank detail suffix should stay empty, got %q", got)
	}
}

func TestMCPElicitationFieldsLine(t *testing.T) {
	input := map[string]any{"requestedSchema": map[string]any{
		"properties": map[string]any{
			"repo":   map[string]any{"title": "Repository"},
			"branch": map[string]any{"description": "Branch name"},
		},
		"required": []any{"repo"},
	}}
	want := "Fields: branch - Branch name, repo (required) - Repository"
	if got := mcpElicitationFieldsLine(input); got != want {
		t.Fatalf("unexpected MCP fields line:\n got: %q\nwant: %q", got, want)
	}
	if got := mcpElicitationFieldsLine(map[string]any{}); got != "" {
		t.Fatalf("missing MCP fields should not produce a line, got %q", got)
	}
}

func TestInputQuestionLine(t *testing.T) {
	if got := inputQuestionLine(map[string]any{"header": "Path", "question": "Where is the repo?"}); got != "Path Where is the repo?" {
		t.Fatalf("unexpected input question line: %q", got)
	}
	if got := inputQuestionLine(map[string]any{"question": "Where is the repo?"}); got != "Where is the repo?" {
		t.Fatalf("unexpected question-only line: %q", got)
	}
	if got := inputQuestionLine(map[string]any{"header": "  "}); got != "" {
		t.Fatalf("blank question line should stay empty, got %q", got)
	}
}

func TestInputPlanText(t *testing.T) {
	reply := "Reply with `/answer input-1 <answer>`."
	if got := inputPlanText("Pick a branch", reply); got != "Pick a branch\n\n"+reply {
		t.Fatalf("unexpected described input plan: %q", got)
	}
	if got := inputPlanText("  ", reply); got != reply {
		t.Fatalf("blank description should not prefix input plan: %q", got)
	}
}

func TestCommandPartsStringPreservesStringParts(t *testing.T) {
	got := commandPartsString([]any{"git", " commit", 3, "--message=fix"})
	want := "git  commit --message=fix"
	if got != want {
		t.Fatalf("unexpected command parts string:\n got: %q\nwant: %q", got, want)
	}
}

func TestMCPFormAnswerUsesSchemaFields(t *testing.T) {
	pending := &pendingServerRequest{
		Method:      "mcpServer/elicitation/request",
		QuestionIDs: []string{"branch", "repo"},
	}
	got, err := answerResponse(pending, `repo=codex-bridge branch="metadata sync"`)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"repo": "codex-bridge", "branch": "metadata sync"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected MCP form answer:\n got: %#v\nwant: %#v", got, want)
	}

	pending.QuestionIDs = []string{"repo"}
	got, err = answerResponse(pending, "codex-bridge")
	if err != nil {
		t.Fatal(err)
	}
	want = map[string]string{"repo": "codex-bridge"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected single-field MCP form answer:\n got: %#v\nwant: %#v", got, want)
	}

	pending.QuestionIDs = nil
	got, err = answerResponse(pending, "plain answer")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, map[string]any{"answer": "plain answer"}) {
		t.Fatalf("unexpected schema-less MCP answer: %#v", got)
	}

	pending.QuestionIDs = []string{"branch", "repo"}
	if _, err = answerResponse(pending, "codex-bridge"); err == nil {
		t.Fatal("multiple MCP form fields should require assignment syntax")
	}
}

func TestMCPFormCodexInputResponseWrapsContent(t *testing.T) {
	pending := &pendingServerRequest{Method: "mcpServer/elicitation/request"}
	content := map[string]string{"repo": "codex-bridge"}
	direct := mcpElicitationResponse(mcpElicitationActionAccept, content)
	if direct["action"] != "accept" || !reflect.DeepEqual(direct["content"], content) || direct["_meta"] != nil {
		t.Fatalf("unexpected direct MCP elicitation response: %#v", direct)
	}

	got := codexInputResponse(pending, content).(map[string]any)
	if got["action"] != "accept" || !reflect.DeepEqual(got["content"], content) {
		t.Fatalf("unexpected MCP elicitation response: %#v", got)
	}
	got = codexInputResponse(pending, aistream.ToolApprovalResponse{Approved: true}).(map[string]any)
	if got["action"] != "accept" || got["content"] != nil {
		t.Fatalf("unexpected accepted MCP elicitation approval response: %#v", got)
	}
	got = codexInputResponse(pending, aistream.ToolApprovalResponse{Approved: false}).(map[string]any)
	if got["action"] != "decline" || got["content"] != nil {
		t.Fatalf("unexpected declined MCP elicitation approval response: %#v", got)
	}
}

func TestNewInputRequestIncludesMCPURLElicitationContext(t *testing.T) {
	run := &activeRun{turnID: "turn-1"}
	pending, request, err := run.newInputRequest("mcpServer/elicitation/request", "rpc-elicit-1", []byte(`{
		"threadId": "thread-1",
		"turnId": "turn-1",
		"serverName": "github",
		"mode": "url",
		"message": "Authorize GitHub",
		"url": "https://github.com/login/device",
		"elicitationId": "github-device"
	}`))
	if err != nil {
		t.Fatal(err)
	}
	if pending.ID != "github-device" || request.ID != "github-device" {
		t.Fatalf("MCP URL elicitation should prefer elicitation ID: pending=%#v request=%#v", pending, request)
	}
	for _, want := range []string{"Authorize GitHub", "URL: https://github.com/login/device", "/answer github-device <answer>"} {
		if !strings.Contains(request.PlanText, want) {
			t.Fatalf("MCP URL elicitation prompt missing %q: %#v", want, request)
		}
	}
}

func TestNewInputRequestPrefersCodexItemID(t *testing.T) {
	run := &activeRun{turnID: "turn-1"}
	pending, request, err := run.newInputRequest("item/tool/requestUserInput", "rpc-input", []byte(`{
		"threadId": "thread-1",
		"turnId": "turn-1",
		"itemId": "input-item",
		"questions": [{"id":"path","question":"Path?"}]
	}`))
	if err != nil {
		t.Fatal(err)
	}
	if pending.ID != "input-item" || request.ID != "input-item" {
		t.Fatalf("tool input request should prefer Codex item ID: pending=%#v request=%#v", pending, request)
	}
}
