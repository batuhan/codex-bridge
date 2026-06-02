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
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/event/cmdschema"
)

func TestApprovalResponseFromCommand(t *testing.T) {
	response, ok := approvalResponseFromCommand("approval-1", "approve")
	if !ok || !response.Approved || response.Always || response.ID != "approval-1" {
		t.Fatalf("unexpected approve response: %#v ok=%v", response, ok)
	}

	response, ok = approvalResponseFromCommand("approval-1", "always")
	if !ok || !response.Approved || !response.Always {
		t.Fatalf("unexpected always response: %#v ok=%v", response, ok)
	}

	response, ok = approvalResponseFromCommand("approval-1", "deny")
	if !ok || response.Approved || response.Reason != "denied" {
		t.Fatalf("unexpected deny response: %#v ok=%v", response, ok)
	}

	if _, ok = approvalResponseFromCommand("approval-1", "maybe"); ok {
		t.Fatal("unexpected success for invalid choice")
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

	if !run.resolveApprovalCommand("approval-1") {
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
			name: "structured stop alias",
			content: &event.MessageEventContent{MSC4391BotCommand: &event.MSC4391BotCommandInput{
				Command: "abort",
			}},
			want: codexCommand{name: "stop"},
			ok:   true,
		},
		{
			name:    "stop alias",
			content: &event.MessageEventContent{Body: "/stop"},
			want:    codexCommand{name: "stop"},
			ok:      true,
		},
		{
			name:    "abort alias",
			content: &event.MessageEventContent{Body: "/abort"},
			want:    codexCommand{name: "stop"},
			ok:      true,
		},
		{
			name:    "interrupt alias",
			content: &event.MessageEventContent{Body: "/interrupt"},
			want:    codexCommand{name: "stop"},
			ok:      true,
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
	if keys := commandParamKeys(seen["approve"]); !reflect.DeepEqual(keys, []string{"id", "choice"}) {
		t.Fatalf("approve command parameters changed: %#v", keys)
	}
	if !seen["approve"].Parameters[1].Optional {
		t.Fatal("approve choice should stay optional so clients can send a one-tap approval")
	}
	if keys := commandParamKeys(seen["answer"]); !reflect.DeepEqual(keys, []string{"id", "answer"}) || seen["answer"].TailParam != "answer" {
		t.Fatalf("answer command parameters changed: keys=%#v tail=%q", keys, seen["answer"].TailParam)
	}
	if !reflect.DeepEqual(seen["stop"].Aliases, []string{"abort", "interrupt"}) {
		t.Fatalf("stop command aliases changed: %#v", seen["stop"].Aliases)
	}
}

func commandParamKeys(command *cmdschema.EventContent) []string {
	keys := make([]string, 0, len(command.Parameters))
	for _, param := range command.Parameters {
		keys = append(keys, param.Key)
	}
	return keys
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
	if resp.StreamOrder != matrixEventTime(msg.Event).UnixNano() {
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
	content, extra := matrixFinalContent(run)
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
	approved := aistream.ToolApprovalResponse{ID: "a", Approved: true}
	always := aistream.ToolApprovalResponse{ID: "a", Approved: true, Always: true}
	denied := aistream.ToolApprovalResponse{ID: "a", Approved: false}

	command := &pendingServerRequest{Method: "item/commandExecution/requestApproval"}
	if got := codexApprovalResponse(command, approved).(map[string]any)["decision"]; got != "accept" {
		t.Fatalf("unexpected command approve decision: %v", got)
	}
	if got := codexApprovalResponse(command, always).(map[string]any)["decision"]; got != "acceptForSession" {
		t.Fatalf("unexpected command always decision: %v", got)
	}
	if got := codexApprovalResponse(command, denied).(map[string]any)["decision"]; got != "decline" {
		t.Fatalf("unexpected command deny decision: %v", got)
	}

	legacy := &pendingServerRequest{Method: "execCommandApproval"}
	if got := codexApprovalResponse(legacy, always).(map[string]any)["decision"]; got != "approved_for_session" {
		t.Fatalf("unexpected legacy always decision: %v", got)
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
	if !hasToolCallResult(run.run.Events, "input-1") {
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

func TestUserInputApprovalResponseBecomesEmptyAnswers(t *testing.T) {
	pending := &pendingServerRequest{Method: "item/tool/requestUserInput"}
	got := codexInputResponse(pending, aistream.ToolApprovalResponse{ID: "input-1", Approved: false}).(map[string]any)
	want := map[string]any{"answers": map[string]any{}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected denied input response:\n got: %#v\nwant: %#v", got, want)
	}
}

func hasToolCallResult(events []agui.Event, toolCallID string) bool {
	for _, event := range events {
		if event.Type() == agui.EventToolCallResult && event.Get("toolCallId") == toolCallID {
			return true
		}
	}
	return false
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

func TestNewApprovalRequestFallsBackToRPCRequestID(t *testing.T) {
	run := &activeRun{turnID: "turn-1"}
	pending, request, err := run.newApprovalRequest("execCommandApproval", "rpc-approval", []byte(`{
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
	got := codexInputResponse(pending, content).(map[string]any)
	if got["action"] != "accept" || !reflect.DeepEqual(got["content"], content) {
		t.Fatalf("unexpected MCP elicitation response: %#v", got)
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
