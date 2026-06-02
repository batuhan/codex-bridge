package bridge

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"maunium.net/go/mautrix/event"

	agui "github.com/beeper/ai-bridge/pkg/ag-ui"
	aistream "github.com/beeper/ai-bridge/pkg/ai-stream"
	aimatrix "github.com/beeper/ai-bridge/pkg/ai-stream/matrix"
	"github.com/beeper/codex-bridge/pkg/appserver"
)

const (
	serverRequestTimeout = 30 * time.Minute
	matrixCommandMsgType = event.MessageType("com.beeper.command")
)

type pendingServerRequest struct {
	ID          string
	Method      string
	ToolCallID  string
	ToolName    string
	Input       map[string]any
	QuestionIDs []string
	Response    chan any
}

func (r *activeRun) handleServerRequest(ctx context.Context, msg appserver.Message) (any, error) {
	switch msg.Method {
	case "item/commandExecution/requestApproval", "item/fileChange/requestApproval", "item/permissions/requestApproval", "applyPatchApproval", "execCommandApproval":
		pending, request, err := r.newApprovalRequest(msg.Method, msg.Params)
		if err != nil {
			return nil, err
		}
		response, err := r.waitForServerRequest(ctx, pending, request)
		if err != nil {
			return nil, err
		}
		approval, ok := response.(aistream.ToolApprovalResponse)
		if !ok {
			return nil, fmt.Errorf("approval %s resolved with invalid response", pending.ID)
		}
		return codexApprovalResponse(pending, approval), nil
	case "item/tool/requestUserInput", "mcpServer/elicitation/request":
		pending, request, err := r.newInputRequest(msg.Method, msg.Params)
		if err != nil {
			return nil, err
		}
		response, err := r.waitForServerRequest(ctx, pending, request)
		if err != nil {
			return nil, err
		}
		return codexInputResponse(pending, response), nil
	case "item/tool/call":
		payload := rawPayload(msg.Params)
		r.mu.Lock()
		r.writer.Custom(msg.Method, payload)
		r.publishLocked()
		r.mu.Unlock()
		return map[string]any{
			"contentItems": []map[string]any{{
				"type": "inputText",
				"text": "Codex bridge does not provide dynamic client tools.",
			}},
			"success": false,
		}, nil
	case "account/chatgptAuthTokens/refresh":
		return nil, fmt.Errorf("Codex auth token refresh must be handled by the local Codex login")
	default:
		return nil, fmt.Errorf("unsupported Codex server request %s", msg.Method)
	}
}

func isHandledCodexServerRequest(method string) bool {
	switch method {
	case "item/commandExecution/requestApproval",
		"item/fileChange/requestApproval",
		"item/permissions/requestApproval",
		"applyPatchApproval",
		"execCommandApproval",
		"item/tool/requestUserInput",
		"mcpServer/elicitation/request",
		"item/tool/call",
		"account/chatgptAuthTokens/refresh",
		"attestation/generate":
		return true
	default:
		return false
	}
}

func (r *activeRun) waitForServerRequest(ctx context.Context, pending *pendingServerRequest, request aistream.ApprovalRequest) (any, error) {
	if pending == nil {
		return nil, fmt.Errorf("missing pending request")
	}
	timer := time.NewTimer(serverRequestTimeout)
	defer timer.Stop()
	r.mu.Lock()
	if r.pending == nil {
		r.pending = map[string]*pendingServerRequest{}
	}
	if r.pending[pending.ID] != nil {
		r.mu.Unlock()
		return nil, fmt.Errorf("request %s is already pending", pending.ID)
	}
	r.pending[pending.ID] = pending
	r.writer.ToolApprovalRequestedWithRequest(request)
	r.writer.InterruptWithUsage(nil)
	r.writer.Custom("codex/serverRequest", map[string]any{
		"method": msgMethod(request),
		"id":     pending.ID,
		"input":  pending.Input,
	})
	r.publishLocked()
	r.queueApprovalPromptLocked(request)
	r.client.queueCodexTyping(r.portalKey, 0)
	r.mu.Unlock()

	select {
	case value := <-pending.Response:
		return value, nil
	case <-ctx.Done():
		r.cancelPending(pending.ID)
		return nil, ctx.Err()
	case <-timer.C:
		response := aistream.TimedOutApprovalResponse(pending.ID)
		r.resolvePendingResponse(response)
		return response, nil
	}
}

func (r *activeRun) queueApprovalPromptLocked(request aistream.ApprovalRequest) {
	if r.client == nil || r.client.UserLogin == nil {
		return
	}
	choices := request.Choices
	if len(choices) == 0 {
		choices = aistream.DefaultApprovalChoices()
	}
	expiresAt := ""
	if !request.ExpiresAt.IsZero() {
		expiresAt = request.ExpiresAt.UTC().Format(time.RFC3339)
	}
	ctxMeta := aistream.ApprovalContext{
		ID:          request.ID,
		ThreadID:    r.threadID,
		RunID:       r.run.RunID,
		MessageID:   r.run.MessageID,
		Command:     "/approve " + request.ID,
		ToolCallID:  request.ToolCallID,
		ToolName:    request.ToolName,
		Title:       request.Title,
		Description: request.Description,
		PlanText:    request.PlanText,
		ExpiresAt:   expiresAt,
		Choices:     choices,
		AgentID:     "codex",
		AgentName:   "Codex",
		Metadata:    request.Metadata,
	}
	msg := r.approvalPromptMessage(ctxMeta, time.Now())
	res := r.client.UserLogin.QueueRemoteEvent(msg)
	if !res.Success {
		logCodexQueueFailure(context.Background(), res, "Failed to queue Codex approval prompt", map[string]any{
			"thread_id":   r.threadID,
			"turn_id":     r.turnID,
			"approval_id": ctxMeta.ID,
		})
	}
}

func (r *activeRun) approvalPromptMessage(ctxMeta aistream.ApprovalContext, ts time.Time) *simplevent.Message[aistream.ApprovalContext] {
	return &simplevent.Message[aistream.ApprovalContext]{
		EventMeta: remoteEventMeta(bridgev2.RemoteEventMessage, r.portalKey, codexUserID, ts),
		Data:      ctxMeta,
		ID:        networkid.MessageID(ctxMeta.ID),
		ConvertMessageFunc: func(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, data aistream.ApprovalContext) (*bridgev2.ConvertedMessage, error) {
			if data.TargetEvent == "" {
				data.TargetEvent = string(r.anchorEventID(ctx, portal))
			}
			choices := data.Choices
			if len(choices) == 0 {
				choices = aistream.DefaultApprovalChoices()
			}
			content, extra := aimatrix.ApprovalContent(data, choices)
			return &bridgev2.ConvertedMessage{Parts: []*bridgev2.ConvertedMessagePart{{
				ID:         networkid.PartID("0"),
				Type:       event.EventMessage,
				Content:    content,
				Extra:      extra,
				DBMetadata: &MessageMetadata{Role: "approval", ThreadID: r.threadID, TurnID: r.turnID, Approval: &data},
			}}}, nil
		},
	}
}

func (r *activeRun) newApprovalRequest(method string, raw json.RawMessage) (*pendingServerRequest, aistream.ApprovalRequest, error) {
	input := rawPayload(raw)
	approvalID := firstString(input, "approvalId", "itemId", "callId")
	if approvalID == "" {
		approvalID = method + ":" + r.turnID
	}
	toolCallID := firstString(input, "itemId", "callId")
	if toolCallID == "" {
		toolCallID = approvalID
	}
	toolName := approvalToolName(method)
	title := approvalTitle(method, input)
	description := firstString(input, "reason")
	plan := approvalPlan(method, input)
	pending := &pendingServerRequest{
		ID:         approvalID,
		Method:     method,
		ToolCallID: toolCallID,
		ToolName:   toolName,
		Input:      input,
		Response:   make(chan any, 1),
	}
	request := aistream.ApprovalRequest{
		ID:          approvalID,
		ToolCallID:  toolCallID,
		ToolName:    toolName,
		Title:       title,
		Description: description,
		PlanText:    plan,
		Input:       input,
		Approval:    aistream.ToolApproval{ID: approvalID, NeedsApproval: true},
		Choices:     aistream.DefaultApprovalChoices(),
		Metadata: map[string]any{
			"method": method,
			"params": input,
		},
		ExpiresAt: time.Now().Add(serverRequestTimeout),
	}
	return pending, request, nil
}

func (r *activeRun) newInputRequest(method string, raw json.RawMessage) (*pendingServerRequest, aistream.ApprovalRequest, error) {
	input := rawPayload(raw)
	requestID := firstString(input, "itemId", "elicitationId", "requestId")
	if requestID == "" {
		requestID = method + ":" + r.turnID
	}
	questions := questionIDs(input)
	title := "Input requested"
	if method == "mcpServer/elicitation/request" {
		title = "MCP input requested"
	}
	if message := firstString(input, "message"); message != "" {
		title = message
	}
	pending := &pendingServerRequest{
		ID:          requestID,
		Method:      method,
		ToolCallID:  requestID,
		ToolName:    inputToolName(method, input),
		Input:       input,
		QuestionIDs: questions,
		Response:    make(chan any, 1),
	}
	plan := "Reply with /answer " + requestID + " <answer>."
	if len(questions) > 1 {
		plan = "Reply with /answer " + requestID + " question_id=answer ..."
	}
	request := aistream.ApprovalRequest{
		ID:          requestID,
		ToolCallID:  requestID,
		ToolName:    pending.ToolName,
		Title:       title,
		Description: inputDescription(input),
		PlanText:    plan,
		Input:       input,
		Approval:    aistream.ToolApproval{ID: requestID, NeedsApproval: true},
		Choices:     aistream.DefaultApprovalChoices(),
		Metadata: map[string]any{
			"method": method,
			"params": input,
		},
		ExpiresAt: time.Now().Add(serverRequestTimeout),
	}
	return pending, request, nil
}

func (r *activeRun) resolveApproval(response aistream.ToolApprovalResponse) bool {
	return r.resolvePendingResponse(response)
}

func (r *activeRun) resolveAnswer(requestID, rawAnswer string) bool {
	r.mu.Lock()
	pending := r.pending[requestID]
	r.mu.Unlock()
	if pending == nil {
		return false
	}
	value, err := answerResponse(pending, rawAnswer)
	if err != nil {
		return false
	}
	return r.resolvePendingValue(requestID, value)
}

func (r *activeRun) resolvePendingResponse(response aistream.ToolApprovalResponse) bool {
	response.RespondedAt = responseTime(response.RespondedAt)
	return r.resolvePendingValue(response.ID, response)
}

func (r *activeRun) resolvePendingValue(id string, value any) bool {
	r.mu.Lock()
	pending := r.pending[id]
	if pending == nil {
		r.mu.Unlock()
		return false
	}
	delete(r.pending, id)
	if approval, ok := value.(aistream.ToolApprovalResponse); ok {
		r.writer.ToolApprovalResponded(pending.ToolCallID, pending.ToolName, pending.Input, approval)
		r.publishLocked()
		r.client.queueCodexTyping(r.portalKey, 30*time.Second)
	}
	r.mu.Unlock()
	pending.Response <- value
	return true
}

func (r *activeRun) cancelPending(id string) {
	r.mu.Lock()
	delete(r.pending, id)
	r.mu.Unlock()
}

func (cl *Client) handleBridgeCommand(ctx context.Context, msg *bridgev2.MatrixMessage) (*bridgev2.MatrixMessageResponse, bool, error) {
	command, ok := parseCodexCommand(msg.Content)
	if !ok {
		return nil, false, nil
	}
	meta := portalMetadata(msg.Portal.Metadata)
	if meta.ThreadID == "" {
		if command.name == "approvals" || command.name == "stop" {
			cl.queueCommandNotice(msg.Portal, "", "No Codex session is active in this room.")
			return cl.commandHandledResponse(msg, "no_session"), true, nil
		}
		return nil, true, fmt.Errorf("no Codex session is active in this room")
	}
	active := cl.Main.activeRun(meta.ThreadID)
	switch command.name {
	case "approvals":
		text := "No pending approvals."
		if active != nil {
			text = active.pendingApprovalsText()
		}
		cl.queueCommandNotice(msg.Portal, meta.ThreadID, text)
		return cl.commandHandledResponse(msg, "approvals"), true, nil
	case "approve":
		if active == nil {
			return nil, true, fmt.Errorf("no Codex turn is waiting for a response")
		}
		if !active.resolveApprovalCommand(command.arg) {
			return nil, true, fmt.Errorf("approval was not pending or the response was invalid")
		}
		return cl.commandHandledResponse(msg, "approve"), true, nil
	case "answer":
		if active == nil {
			return nil, true, fmt.Errorf("no Codex turn is waiting for a response")
		}
		if !active.resolveAnswerCommand(command.arg) {
			return nil, true, fmt.Errorf("request was not pending or the response was invalid")
		}
		return cl.commandHandledResponse(msg, "answer"), true, nil
	case "stop":
		if active == nil {
			cl.queueCommandNotice(msg.Portal, meta.ThreadID, "No active Codex turn is running.")
			return cl.commandHandledResponse(msg, "no_active_turn"), true, nil
		}
		go cl.interruptTurn(context.WithoutCancel(ctx), msg.Portal, meta.ThreadID, active.turnID)
		return cl.commandHandledResponse(msg, "stop"), true, nil
	default:
		return nil, false, nil
	}
}

func (r *activeRun) resolveApprovalCommand(args string) bool {
	approvalID, rawChoice, ok := strings.Cut(args, " ")
	if !ok {
		return false
	}
	response, ok := approvalResponseFromCommand(strings.TrimSpace(approvalID), strings.TrimSpace(rawChoice))
	if !ok {
		return false
	}
	return r.resolveApproval(response)
}

func (r *activeRun) resolveAnswerCommand(args string) bool {
	requestID, rawAnswer, ok := strings.Cut(args, " ")
	if !ok {
		return false
	}
	return r.resolveAnswer(strings.TrimSpace(requestID), strings.TrimSpace(rawAnswer))
}

type codexCommand struct {
	name string
	arg  string
}

func parseCodexCommand(content *event.MessageEventContent) (codexCommand, bool) {
	if content == nil {
		return codexCommand{}, false
	}
	if command, ok := codexCommandFromMSC4391(content.MSC4391BotCommand); ok {
		return command, true
	}
	body := strings.TrimSpace(content.Body)
	if content.MsgType == matrixCommandMsgType {
		return parseCodexCommandBody(body, true)
	}
	return parseCodexCommandBody(body, false)
}

func parseCodexCommandBody(body string, commandMessage bool) (codexCommand, bool) {
	body = strings.TrimSpace(body)
	if strings.HasPrefix(body, "/") {
		body = strings.TrimPrefix(body, "/")
	} else if strings.HasPrefix(body, "!codex ") {
		body = strings.TrimSpace(strings.TrimPrefix(body, "!codex"))
	} else if !commandMessage {
		return codexCommand{}, false
	}
	name, arg, _ := strings.Cut(body, " ")
	name = canonicalCodexCommandName(name)
	if name == "" {
		return codexCommand{}, false
	}
	return codexCommand{name: name, arg: strings.TrimSpace(arg)}, true
}

func canonicalCodexCommandName(name string) string {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "approve":
		return "approve"
	case "approval", "approvals":
		return "approvals"
	case "answer":
		return "answer"
	case "abort", "interrupt", "stop":
		return "stop"
	default:
		return ""
	}
}

func (r *activeRun) pendingApprovalsText() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	ids := make([]string, 0, len(r.pending))
	for id, pending := range r.pending {
		if isApprovalRequestMethod(pending.Method) {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	if len(ids) == 0 {
		return "No pending approvals."
	}
	out := []string{"Pending approvals:"}
	for _, id := range ids {
		pending := r.pending[id]
		out = append(out, "", "### "+id, approvalTitle(pending.Method, pending.Input))
		if plan := approvalPlan(pending.Method, pending.Input); plan != "" {
			out = append(out, "", plan)
		}
		out = append(out,
			"",
			"Respond with one of:",
			"- `/approve "+id+" approve`",
			"- `/approve "+id+" always`",
			"- `/approve "+id+" deny`",
		)
	}
	return strings.Join(out, "\n")
}

func isApprovalRequestMethod(method string) bool {
	switch method {
	case "item/commandExecution/requestApproval", "item/fileChange/requestApproval", "item/permissions/requestApproval", "applyPatchApproval", "execCommandApproval":
		return true
	default:
		return false
	}
}

func (cl *Client) commandHandledResponse(msg *bridgev2.MatrixMessage, status string) *bridgev2.MatrixMessageResponse {
	meta := portalMetadata(nil)
	if msg != nil && msg.Portal != nil {
		meta = portalMetadata(msg.Portal.Metadata)
	}
	return &bridgev2.MatrixMessageResponse{DB: &database.Message{
		ID:        networkid.MessageID("command:" + string(msg.Event.ID)),
		PartID:    partID("command"),
		Room:      msg.Portal.PortalKey,
		SenderID:  cl.GetUserID(),
		Timestamp: matrixEventTime(msg.Event),
		Metadata:  &MessageMetadata{Role: "command", ThreadID: meta.ThreadID, StreamStatus: status},
	}}
}

func (cl *Client) queueCommandNotice(portal *bridgev2.Portal, threadID, text string) {
	if cl == nil || cl.UserLogin == nil || portal == nil || strings.TrimSpace(text) == "" {
		return
	}
	now := time.Now()
	msgID := networkid.MessageID(fmt.Sprintf("command-notice:%d", now.UnixNano()))
	run := commandNoticeRun(text, string(msgID), threadID, now)
	res := cl.UserLogin.QueueRemoteEvent(&simplevent.Message[aistream.Run]{
		EventMeta: remoteEventMeta(bridgev2.RemoteEventMessage, portal.PortalKey, codexUserID, now),
		ID:        msgID,
		Data:      run,
		ConvertMessageFunc: func(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, data aistream.Run) (*bridgev2.ConvertedMessage, error) {
			content, extra, err := matrixFinalContentWithAttachment(ctx, portal, intent, data)
			if err != nil {
				return nil, err
			}
			return &bridgev2.ConvertedMessage{Parts: []*bridgev2.ConvertedMessagePart{{
				ID:         partID("command"),
				Type:       event.EventMessage,
				Content:    content,
				Extra:      extra,
				DBMetadata: &MessageMetadata{Role: "assistant", ThreadID: threadID, StreamStatus: "notice"},
			}}}, nil
		},
	})
	if !res.Success {
		logCodexQueueFailure(context.Background(), res, "Failed to queue Codex command notice", map[string]any{
			"thread_id":  threadID,
			"message_id": string(msgID),
		})
	}
}

func commandNoticeRun(text string, messageID string, threadID string, now time.Time) aistream.Run {
	run := aistream.NewRun(fmt.Sprintf("command-%d", now.UnixNano()), threadID, "codex", "codex", "Codex", now)
	run.MessageID = messageID
	run.Data["capabilities"] = codexAgentCapabilities()
	writer := aistream.NewWriter(run, func() time.Time { return now })
	writer.Start()
	writer.Text(text)
	writer.Finish(agui.FinishReasonStop)
	return *run
}

func approvalResponseFromCommand(approvalID string, rawChoice string) (aistream.ToolApprovalResponse, bool) {
	choice, ok := aistream.ResolveApprovalChoice(aistream.DefaultApprovalChoices(), rawChoice)
	if !ok {
		return aistream.ToolApprovalResponse{}, false
	}
	response := aistream.ApprovalResponseForChoice(approvalID, choice)
	response.RespondedAt = responseTime(response.RespondedAt)
	return response, true
}

func codexApprovalResponse(pending *pendingServerRequest, response aistream.ToolApprovalResponse) any {
	switch pending.Method {
	case "item/commandExecution/requestApproval":
		return map[string]any{"decision": codexCommandDecision(response)}
	case "item/fileChange/requestApproval":
		return map[string]any{"decision": codexFileDecision(response)}
	case "item/permissions/requestApproval":
		permissions := map[string]any{}
		if response.Approved {
			if requested, ok := pending.Input["permissions"].(map[string]any); ok {
				permissions = requested
			}
		}
		scope := "turn"
		if response.Always {
			scope = "session"
		}
		return map[string]any{"permissions": permissions, "scope": scope}
	case "applyPatchApproval", "execCommandApproval":
		return map[string]any{"decision": codexLegacyDecision(response)}
	default:
		return map[string]any{"decision": "denied"}
	}
}

func codexInputResponse(pending *pendingServerRequest, response any) any {
	switch pending.Method {
	case "item/tool/requestUserInput":
		if _, ok := response.(aistream.ToolApprovalResponse); ok {
			return map[string]any{"answers": map[string]any{}}
		}
		return map[string]any{"answers": response}
	case "mcpServer/elicitation/request":
		if approval, ok := response.(aistream.ToolApprovalResponse); ok {
			action := "decline"
			if approval.Approved {
				action = "accept"
			}
			return map[string]any{"action": action, "content": nil, "_meta": nil}
		}
		return map[string]any{"action": "accept", "content": response, "_meta": nil}
	default:
		return response
	}
}

func answerResponse(pending *pendingServerRequest, raw string) (any, error) {
	if pending.Method == "mcpServer/elicitation/request" {
		var decoded any
		if json.Unmarshal([]byte(raw), &decoded) == nil {
			return decoded, nil
		}
		return map[string]any{"answer": raw}, nil
	}
	ids := pending.QuestionIDs
	if len(ids) == 0 {
		ids = []string{"answer"}
	}
	answers := map[string]any{}
	fields := strings.Fields(raw)
	hasAssignments := false
	for _, field := range fields {
		if strings.Contains(field, "=") {
			hasAssignments = true
			break
		}
	}
	if !hasAssignments {
		if len(ids) > 1 {
			return nil, fmt.Errorf("multiple answers require question_id=value syntax")
		}
		answers[ids[0]] = map[string]any{"answers": []string{raw}}
		return answers, nil
	}
	for _, field := range fields {
		key, value, ok := strings.Cut(field, "=")
		if !ok || key == "" {
			return nil, fmt.Errorf("invalid answer %q", field)
		}
		answers[key] = map[string]any{"answers": []string{value}}
	}
	return answers, nil
}

func codexCommandDecision(response aistream.ToolApprovalResponse) any {
	if !response.Approved {
		if response.Reason == "aborted" || response.Choice == "cancel" {
			return "cancel"
		}
		return "decline"
	}
	if response.Always {
		return "acceptForSession"
	}
	return "accept"
}

func codexFileDecision(response aistream.ToolApprovalResponse) string {
	if !response.Approved {
		if response.Reason == "aborted" || response.Choice == "cancel" {
			return "cancel"
		}
		return "decline"
	}
	if response.Always {
		return "acceptForSession"
	}
	return "accept"
}

func codexLegacyDecision(response aistream.ToolApprovalResponse) string {
	if !response.Approved {
		if response.Reason == "aborted" || response.Choice == "cancel" {
			return "abort"
		}
		if response.Reason == "timed_out" {
			return "timed_out"
		}
		return "denied"
	}
	if response.Always {
		return "approved_for_session"
	}
	return "approved"
}

func approvalToolName(method string) string {
	switch method {
	case "item/commandExecution/requestApproval", "execCommandApproval":
		return "command_execution"
	case "item/fileChange/requestApproval", "applyPatchApproval":
		return "file_change"
	case "item/permissions/requestApproval":
		return "permissions"
	default:
		return method
	}
}

func inputToolName(method string, input map[string]any) string {
	if method == "mcpServer/elicitation/request" {
		if server := firstString(input, "serverName"); server != "" {
			return "mcp:" + server
		}
		return "mcp_elicitation"
	}
	return "request_user_input"
}

func approvalTitle(method string, input map[string]any) string {
	switch method {
	case "item/commandExecution/requestApproval", "execCommandApproval":
		if command := commandString(input); command != "" {
			return "Approve command: " + command
		}
		return "Approve command?"
	case "item/fileChange/requestApproval", "applyPatchApproval":
		return "Approve file changes?"
	case "item/permissions/requestApproval":
		return "Approve permissions?"
	default:
		return "Approve Codex request?"
	}
}

func approvalPlan(method string, input map[string]any) string {
	var lines []string
	if command := commandString(input); command != "" {
		lines = append(lines, "Command: "+command)
	}
	if cwd := firstString(input, "cwd"); cwd != "" {
		lines = append(lines, "Directory: "+cwd)
	}
	if reason := firstString(input, "reason"); reason != "" {
		lines = append(lines, "Reason: "+reason)
	}
	if grant := firstString(input, "grantRoot"); grant != "" {
		lines = append(lines, "Grant root: "+grant)
	}
	if len(lines) == 0 {
		return method
	}
	return strings.Join(lines, "\n")
}

func inputDescription(input map[string]any) string {
	if qs, ok := input["questions"].([]any); ok && len(qs) > 0 {
		var lines []string
		for _, raw := range qs {
			q, _ := raw.(map[string]any)
			header := firstString(q, "header")
			question := firstString(q, "question")
			line := strings.TrimSpace(strings.TrimSpace(header) + " " + strings.TrimSpace(question))
			if line != "" {
				lines = append(lines, line)
			}
		}
		return strings.Join(lines, "\n")
	}
	return firstString(input, "message")
}

func firstString(values map[string]any, keys ...string) string {
	for _, key := range keys {
		if value, _ := values[key].(string); strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func commandString(input map[string]any) string {
	if command := firstString(input, "command"); command != "" {
		return command
	}
	if parts, ok := input["command"].([]any); ok {
		out := make([]string, 0, len(parts))
		for _, part := range parts {
			if text, ok := part.(string); ok {
				out = append(out, text)
			}
		}
		return strings.Join(out, " ")
	}
	return ""
}

func questionIDs(input map[string]any) []string {
	questions, _ := input["questions"].([]any)
	out := make([]string, 0, len(questions))
	for _, raw := range questions {
		question, _ := raw.(map[string]any)
		if id := firstString(question, "id"); id != "" {
			out = append(out, id)
		}
	}
	return out
}

func responseTime(value string) string {
	if value != "" {
		return value
	}
	return time.Now().UTC().Format(time.RFC3339)
}

func msgMethod(request aistream.ApprovalRequest) string {
	if request.Metadata == nil {
		return ""
	}
	method, _ := request.Metadata["method"].(string)
	return method
}
