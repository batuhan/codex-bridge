package bridge

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	aistream "github.com/beeper/ai-bridge/pkg/ai-stream"
	aimatrix "github.com/beeper/ai-bridge/pkg/ai-stream/matrix"
	"maunium.net/go/mautrix/event"
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
	seen := map[string]bool{}
	for _, command := range codexCommandDescriptions() {
		if err := command.Validate(); err != nil {
			t.Fatalf("invalid command %q: %v", command.Command, err)
		}
		if seen[command.Command] {
			t.Fatalf("duplicate command %q", command.Command)
		}
		seen[command.Command] = true
	}
	for _, want := range []string{"approvals", "approve", "answer", "stop"} {
		if !seen[want] {
			t.Fatalf("missing command description %q", want)
		}
	}
}

func TestCommandHandledResponseUsesTypedMetadata(t *testing.T) {
	client := &Client{UserLogin: testUserLogin("codex")}
	resp := client.commandHandledResponse(testMatrixMessage("thread-1", "/approvals"), "approvals")
	meta, ok := resp.DB.Metadata.(*MessageMetadata)
	if !ok || meta.Role != "command" || meta.ThreadID != "thread-1" || meta.StreamStatus != "approvals" {
		t.Fatalf("unexpected command metadata: %#v", resp.DB.Metadata)
	}
}

func TestPendingApprovalsText(t *testing.T) {
	run := &activeRun{pending: map[string]*pendingServerRequest{
		"approval-1": {
			ID:     "approval-1",
			Method: "item/commandExecution/requestApproval",
			Input:  map[string]any{"command": "git status", "cwd": "/tmp/project"},
		},
		"input-1": {
			ID:     "input-1",
			Method: "item/tool/requestUserInput",
		},
	}}
	got := run.pendingApprovalsText()
	for _, want := range []string{
		"Pending approvals:",
		"Approve command: git status",
		"/approve approval-1 approve",
		"/approve approval-1 always",
		"/approve approval-1 deny",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("pending approvals text missing %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "input-1") {
		t.Fatalf("non-approval request leaked into approvals text:\n%s", got)
	}
}

func TestCommandNoticeRunUsesBeeperStreamFinal(t *testing.T) {
	run := commandNoticeRun("notice", "message-1", "thread-1", time.Unix(0, 0))
	if run.ThreadID != "thread-1" || run.MessageID != "message-1" {
		t.Fatalf("unexpected notice run IDs: %#v", run)
	}
	_, extra := matrixFinalContent(run)
	ai, ok := extra[aistream.BeeperAIKey].(aistream.BeeperAI)
	if !ok {
		t.Fatalf("notice missing Beeper AI final payload: %#v", extra)
	}
	if ai.Kind != aistream.AIKindFinal || ai.ThreadID != "thread-1" || ai.Message == nil {
		t.Fatalf("unexpected notice AI payload: %#v", ai)
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
	meta, ok := part.DBMetadata.(*MessageMetadata)
	if !ok || meta.Role != "approval" || meta.ThreadID != "thread-1" || meta.TurnID != "turn-1" || meta.Approval == nil || meta.Approval.ID != "approval-1" {
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
	pending, request, err := run.newApprovalRequest("item/commandExecution/requestApproval", []byte(`{
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
