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
	serverRequestTimeout           = 30 * time.Minute
	matrixCommandMsgType           = event.MessageType("com.beeper.command")
	unsupportedDynamicToolCallText = "Codex bridge does not provide dynamic client tools."
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
		pending, request, err := r.newApprovalRequest(msg.Method, serverRequestID(msg.ID), msg.Params)
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
		pending, request, err := r.newInputRequest(msg.Method, serverRequestID(msg.ID), msg.Params)
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
		response := unsupportedDynamicToolCallResponse()
		r.mu.Lock()
		r.writeUnsupportedDynamicToolCallLocked(serverRequestID(msg.ID), payload, response)
		r.publishLocked()
		r.mu.Unlock()
		return response, nil
	default:
		return nil, fmt.Errorf("unsupported Codex server request %s", msg.Method)
	}
}

func unsupportedDynamicToolCallResponse() map[string]any {
	return map[string]any{
		"contentItems": []map[string]any{{
			"type": "inputText",
			"text": unsupportedDynamicToolCallText,
		}},
		"success": false,
	}
}

func (r *activeRun) writeUnsupportedDynamicToolCallLocked(fallbackID string, payload map[string]any, response map[string]any) {
	if r == nil || r.writer == nil {
		return
	}
	callID := firstString(payload, "callId", "itemId", "id")
	if callID == "" {
		callID = strings.TrimSpace(fallbackID)
	}
	if callID == "" {
		callID = "dynamic_tool_call"
	}
	name := dynamicToolCallName(payload)
	input := copyStateMap(payload)
	r.ensureToolStartedWithMetadata(callID, name, map[string]any{"codex": map[string]any{
		"request": "item/tool/call",
		"callId":  callID,
		"name":    name,
	}})
	if len(input) > 0 {
		r.writeToolArgsText(callID, compactJSONString(input), input)
	}
	r.writer.ToolResult(callID, unsupportedDynamicToolCallText, agui.ToolResultStateError)
	r.toolResult[callID] = true
	r.endToolCall(callID, name, input, response)
}

func dynamicToolCallName(payload map[string]any) string {
	namespace := firstString(payload, "namespace")
	tool := firstString(payload, "tool", "name")
	switch {
	case namespace != "" && tool != "":
		return namespace + ": " + tool
	case tool != "":
		return tool
	case namespace != "":
		return namespace
	default:
		return "dynamic tool"
	}
}

func serverRequestID(id any) string {
	switch value := id.(type) {
	case string:
		return strings.TrimSpace(value)
	case json.Number:
		return value.String()
	case float64:
		if value == float64(int64(value)) {
			return fmt.Sprintf("%d", int64(value))
		}
		return fmt.Sprintf("%v", value)
	case int:
		return fmt.Sprintf("%d", value)
	case int64:
		return fmt.Sprintf("%d", value)
	default:
		if id == nil {
			return ""
		}
		return strings.TrimSpace(fmt.Sprint(id))
	}
}

func directServerRequestError(method string) (int, string, bool) {
	switch method {
	case "attestation/generate":
		return -32002, "Codex bridge does not provide attestation tokens", true
	case "account/chatgptAuthTokens/refresh":
		return -32002, "Codex auth token refresh must be handled by the local Codex login", true
	default:
		return 0, "", false
	}
}

func isHandledCodexServerRequest(method string) bool {
	if _, _, ok := directServerRequestError(method); ok {
		return true
	}
	switch method {
	case "item/commandExecution/requestApproval",
		"item/fileChange/requestApproval",
		"item/permissions/requestApproval",
		"applyPatchApproval",
		"execCommandApproval",
		"item/tool/requestUserInput",
		"mcpServer/elicitation/request",
		"item/tool/call":
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
		Model:       r.run.Model,
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
			applyCodexMessageProfile(content)
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

func (r *activeRun) newApprovalRequest(method, fallbackID string, raw json.RawMessage) (*pendingServerRequest, aistream.ApprovalRequest, error) {
	input := rawPayload(raw)
	approvalID := firstString(input, "approvalId", "itemId", "callId")
	if approvalID == "" {
		approvalID = strings.TrimSpace(fallbackID)
	}
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

func (r *activeRun) newInputRequest(method, fallbackID string, raw json.RawMessage) (*pendingServerRequest, aistream.ApprovalRequest, error) {
	input := rawPayload(raw)
	requestID := firstString(input, "itemId", "elicitationId", "requestId")
	if requestID == "" {
		requestID = strings.TrimSpace(fallbackID)
	}
	if requestID == "" {
		requestID = method + ":" + r.turnID
	}
	questions := questionIDs(input)
	if method == "mcpServer/elicitation/request" && len(questions) == 0 {
		questions = mcpElicitationFieldIDs(input)
	}
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
	description := inputDescription(input)
	plan := inputPlan(requestID, questions, description)
	request := aistream.ApprovalRequest{
		ID:          requestID,
		ToolCallID:  requestID,
		ToolName:    pending.ToolName,
		Title:       title,
		Description: description,
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
	if response, ok := streamResponseForResolvedPending(pending, value); ok {
		r.writer.ToolApprovalResponded(pending.ToolCallID, pending.ToolName, pending.Input, response)
		r.publishLocked()
		if r.client != nil {
			r.client.queueCodexTyping(r.portalKey, 30*time.Second)
		}
	}
	r.mu.Unlock()
	pending.Response <- value
	return true
}

func streamResponseForResolvedPending(pending *pendingServerRequest, value any) (aistream.ToolApprovalResponse, bool) {
	if response, ok := value.(aistream.ToolApprovalResponse); ok {
		return response, true
	}
	if pending == nil || pending.ID == "" {
		return aistream.ToolApprovalResponse{}, false
	}
	return aistream.ToolApprovalResponse{
		ID:          pending.ID,
		Approved:    true,
		Choice:      "answer",
		RespondedAt: responseTime(""),
		Metadata: map[string]any{
			"method":   pending.Method,
			"response": value,
		},
	}, true
}

func (r *activeRun) cancelPending(id string) {
	r.mu.Lock()
	delete(r.pending, id)
	r.mu.Unlock()
}

func (cl *Client) handleBridgeCommand(ctx context.Context, msg *bridgev2.MatrixMessage) (*bridgev2.MatrixMessageResponse, bool, error) {
	command, ok := parseCodexCommandMessage(msg)
	if !ok {
		return nil, false, nil
	}
	meta := portalMetadata(msg.Portal.Metadata)
	if meta.ThreadID == "" {
		cl.queueCommandNotice(msg.Portal, "", "No Codex session is active in this room.")
		return cl.commandHandledResponse(msg, "no_session"), true, nil
	}
	var active *activeRun
	if cl != nil && cl.Main != nil {
		active = cl.Main.activeRun(meta.ThreadID)
	}
	switch command.name {
	case "approvals":
		text := "No pending Codex requests."
		if active != nil {
			text = active.pendingRequestsText()
		}
		cl.queueCommandNotice(msg.Portal, meta.ThreadID, text)
		return cl.commandHandledResponse(msg, "approvals"), true, nil
	case "approve":
		if active == nil {
			cl.queueCommandNotice(msg.Portal, meta.ThreadID, "No Codex turn is waiting for an approval.")
			return cl.commandHandledResponse(msg, "no_pending_approval"), true, nil
		}
		response, ok := parseApprovalCommandResponse(command.arg)
		if !ok || !active.resolveApproval(response) {
			cl.queueCommandNotice(msg.Portal, meta.ThreadID, "That approval was not pending, or the response was invalid.\n\nUse `/approvals` to list pending Codex requests.")
			return cl.commandHandledResponse(msg, "invalid_approval"), true, nil
		}
		active.writeCodexClientRequestState("command/approve", codexApprovalCommandClientState(response))
		return cl.commandHandledResponse(msg, "approve"), true, nil
	case "answer":
		if active == nil {
			cl.queueCommandNotice(msg.Portal, meta.ThreadID, "No Codex turn is waiting for input.")
			return cl.commandHandledResponse(msg, "no_pending_input"), true, nil
		}
		requestID, answer, ok := parseAnswerCommandArgs(command.arg)
		if !ok || !active.resolveAnswer(requestID, answer) {
			cl.queueCommandNotice(msg.Portal, meta.ThreadID, "That input request was not pending, or the response was invalid.\n\nUse `/approvals` to list pending Codex requests.")
			return cl.commandHandledResponse(msg, "invalid_answer"), true, nil
		}
		active.writeCodexClientRequestState("command/answer", map[string]any{
			"id": requestID,
		})
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

func parseCodexCommandMessage(msg *bridgev2.MatrixMessage) (codexCommand, bool) {
	if msg == nil {
		return codexCommand{}, false
	}
	if command, ok := parseCodexCommand(msg.Content); ok {
		return command, true
	}
	if msg.Event == nil {
		return codexCommand{}, false
	}
	return codexCommandFromRawContent(msg.Event.Content.Raw)
}

func (r *activeRun) resolveApprovalCommand(args string) bool {
	response, ok := parseApprovalCommandResponse(args)
	if !ok {
		return false
	}
	return r.resolveApproval(response)
}

func (r *activeRun) resolveAnswerCommand(args string) bool {
	requestID, answer, ok := parseAnswerCommandArgs(args)
	if !ok {
		return false
	}
	return r.resolveAnswer(requestID, answer)
}

func parseApprovalCommandResponse(args string) (aistream.ToolApprovalResponse, bool) {
	approvalID, rawChoice, _ := strings.Cut(strings.TrimSpace(args), " ")
	approvalID = strings.TrimSpace(approvalID)
	if approvalID == "" {
		return aistream.ToolApprovalResponse{}, false
	}
	rawChoice = strings.TrimSpace(rawChoice)
	if rawChoice == "" {
		rawChoice = "approve"
	}
	return approvalResponseFromCommand(approvalID, rawChoice)
}

func parseAnswerCommandArgs(args string) (requestID, answer string, ok bool) {
	requestID, answer, ok = strings.Cut(args, " ")
	if !ok {
		return "", "", false
	}
	requestID = strings.TrimSpace(requestID)
	if requestID == "" {
		return "", "", false
	}
	return requestID, strings.TrimSpace(answer), true
}

func codexApprovalCommandClientState(response aistream.ToolApprovalResponse) map[string]any {
	state := map[string]any{
		"id":       response.ID,
		"approved": response.Approved,
		"choice":   "deny",
	}
	if response.Approved {
		state["choice"] = "approve"
	}
	if response.Always {
		state["choice"] = "always"
		state["always"] = true
	}
	if response.Reason != "" {
		state["reason"] = response.Reason
	}
	return state
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

func (r *activeRun) pendingRequestsText() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	ids := make([]string, 0, len(r.pending))
	for id, pending := range r.pending {
		if pending != nil {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	if len(ids) == 0 {
		return "No pending Codex requests."
	}
	out := []string{"Pending Codex requests:"}
	for _, id := range ids {
		pending := r.pending[id]
		if isApprovalRequestMethod(pending.Method) {
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
			continue
		}
		out = append(out, "", "### "+id, pendingRequestTitle(pending), pendingRequestPlan(pending))
	}
	return strings.Join(out, "\n")
}

func pendingRequestTitle(pending *pendingServerRequest) string {
	if pending == nil {
		return "Input requested"
	}
	if message := firstString(pending.Input, "message"); message != "" {
		return message
	}
	if pending.ToolName != "" {
		return "Input requested by " + pending.ToolName
	}
	return "Input requested"
}

func pendingRequestPlan(pending *pendingServerRequest) string {
	if pending == nil || pending.ID == "" {
		return "Respond with `/answer <id> <answer>`."
	}
	return inputPlan(pending.ID, pending.QuestionIDs, inputDescription(pending.Input))
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
	timestamp := matrixEventTime(msg.Event)
	return &bridgev2.MatrixMessageResponse{DB: &database.Message{
		ID:        networkid.MessageID("command:" + string(msg.Event.ID)),
		PartID:    partID("command"),
		Room:      msg.Portal.PortalKey,
		SenderID:  cl.GetUserID(),
		Timestamp: timestamp,
		Metadata:  &MessageMetadata{Role: "command", ThreadID: meta.ThreadID, StreamStatus: status},
	}, StreamOrder: timestamp.UnixNano()}
}

func (cl *Client) queueCommandNotice(portal *bridgev2.Portal, threadID, text string) {
	if cl == nil || cl.UserLogin == nil || cl.UserLogin.Bridge == nil || portal == nil || strings.TrimSpace(text) == "" {
		return
	}
	now := time.Now()
	msgID := networkid.MessageID(fmt.Sprintf("command-notice:%d", now.UnixNano()))
	run := commandNoticeRun(text, string(msgID), threadID, activeRunInitialModel(cl, threadID), now)
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

func commandNoticeRun(text string, messageID string, threadID string, model string, now time.Time) aistream.Run {
	run := aistream.NewRun(fmt.Sprintf("command-%d", now.UnixNano()), threadID, model, "codex", "Codex", now)
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
		if len(pending.QuestionIDs) > 0 {
			return namedAnswerValues(pending.QuestionIDs, raw)
		}
		return map[string]any{"answer": raw}, nil
	}
	ids := pending.QuestionIDs
	if len(ids) == 0 {
		ids = []string{"answer"}
	}
	values, err := namedAnswerValues(ids, raw)
	if err != nil {
		return nil, err
	}
	answers := map[string]any{}
	for key, value := range values {
		answers[key] = map[string]any{"answers": []string{value}}
	}
	return answers, nil
}

func namedAnswerValues(ids []string, raw string) (map[string]string, error) {
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
		return map[string]string{ids[0]: raw}, nil
	}
	return parseAnswerAssignments(raw)
}

func parseAnswerAssignments(raw string) (map[string]string, error) {
	out := map[string]string{}
	for i := 0; i < len(raw); {
		for i < len(raw) && isAnswerSpace(raw[i]) {
			i++
		}
		if i == len(raw) {
			break
		}
		keyStart := i
		for i < len(raw) && raw[i] != '=' && !isAnswerSpace(raw[i]) {
			i++
		}
		if keyStart == i || i == len(raw) || raw[i] != '=' {
			return nil, fmt.Errorf("invalid answer assignment near %q", raw[keyStart:])
		}
		key := raw[keyStart:i]
		i++
		value, next, err := parseAnswerValue(raw, i)
		if err != nil {
			return nil, err
		}
		out[key] = value
		i = next
	}
	return out, nil
}

func parseAnswerValue(raw string, i int) (string, int, error) {
	if i >= len(raw) {
		return "", i, nil
	}
	if raw[i] != '"' && raw[i] != '\'' {
		start := i
		for i < len(raw) && !isAnswerSpace(raw[i]) {
			i++
		}
		return raw[start:i], i, nil
	}
	quote := raw[i]
	i++
	var b strings.Builder
	for i < len(raw) {
		ch := raw[i]
		i++
		if ch == quote {
			if i < len(raw) && !isAnswerSpace(raw[i]) {
				return "", i, fmt.Errorf("invalid quoted answer value near %q", raw[i:])
			}
			return b.String(), i, nil
		}
		if ch == '\\' && i < len(raw) {
			ch = raw[i]
			i++
		}
		b.WriteByte(ch)
	}
	return "", i, fmt.Errorf("unterminated quoted answer value")
}

func isAnswerSpace(ch byte) bool {
	return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r'
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
	var lines []string
	if message := firstString(input, "message"); message != "" {
		lines = append(lines, message)
	}
	if url := firstString(input, "url"); url != "" {
		lines = append(lines, "URL: "+url)
	}
	if fields := mcpElicitationFields(input); len(fields) > 0 {
		lines = append(lines, "Fields: "+strings.Join(fields, ", "))
	}
	if qs, ok := input["questions"].([]any); ok && len(qs) > 0 {
		for _, raw := range qs {
			q, _ := raw.(map[string]any)
			header := firstString(q, "header")
			question := firstString(q, "question")
			line := strings.TrimSpace(strings.TrimSpace(header) + " " + strings.TrimSpace(question))
			if line != "" {
				lines = append(lines, line)
			}
		}
	}
	return strings.Join(lines, "\n")
}

func mcpElicitationFieldIDs(input map[string]any) []string {
	fields := mcpElicitationFields(input)
	out := make([]string, 0, len(fields))
	for _, field := range fields {
		name, _, _ := strings.Cut(field, " ")
		if name != "" {
			out = append(out, name)
		}
	}
	return out
}

func mcpElicitationFields(input map[string]any) []string {
	schema, _ := input["requestedSchema"].(map[string]any)
	properties, _ := schema["properties"].(map[string]any)
	if len(properties) == 0 {
		return nil
	}
	required := map[string]bool{}
	for _, raw := range anySlice(schema["required"]) {
		if name, _ := raw.(string); strings.TrimSpace(name) != "" {
			required[strings.TrimSpace(name)] = true
		}
	}
	fields := make([]string, 0, len(properties))
	for name, raw := range properties {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		descriptor := name
		prop, _ := raw.(map[string]any)
		if required[name] {
			descriptor += " (required)"
		}
		if title := firstString(prop, "title"); title != "" {
			descriptor += " - " + title
		} else if description := firstString(prop, "description"); description != "" {
			descriptor += " - " + description
		}
		fields = append(fields, descriptor)
	}
	sort.Strings(fields)
	return fields
}

func anySlice(value any) []any {
	items, _ := value.([]any)
	return items
}

func inputPlan(requestID string, questionIDs []string, description string) string {
	var lines []string
	if description = strings.TrimSpace(description); description != "" {
		lines = append(lines, description, "")
	}
	if len(questionIDs) > 1 {
		lines = append(lines, "Reply with `/answer "+requestID+" question_id=answer ...`.")
	} else {
		lines = append(lines, "Reply with `/answer "+requestID+" <answer>`.")
	}
	return strings.Join(lines, "\n")
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
