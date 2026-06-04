package bridge

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
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
	noPendingCodexRequestsText     = "No pending Codex requests."

	serverRequestMethodUserInput       = "item/tool/requestUserInput"
	serverRequestMethodMCPElicitation  = "mcpServer/elicitation/request"
	serverRequestMethodDynamicToolCall = "item/tool/call"

	serverRequestMethodCommandApproval     = "item/commandExecution/requestApproval"
	serverRequestMethodFileChangeApproval  = "item/fileChange/requestApproval"
	serverRequestMethodPermissionsApproval = "item/permissions/requestApproval"

	serverRequestMethodLegacyCommandApproval    = "execCommandApproval"
	serverRequestMethodLegacyFileChangeApproval = "applyPatchApproval"

	approvalRequestKindCommandExecution = "command_execution"
	approvalRequestKindFileChange       = "file_change"
	approvalRequestKindPermissions      = "permissions"

	codexAnswerResponseKey  = "answers"
	defaultAnswerQuestionID = "answer"
	approvalPermissionsKey  = "permissions"

	approvalPermissionScopeTurn    = "turn"
	approvalPermissionScopeSession = "session"

	approvalDecisionKindAlways   = "always"
	approvalDecisionKindApproved = "approved"
	approvalDecisionKindCanceled = "canceled"
	approvalDecisionKindDenied   = "denied"

	codexDecisionResponseKey = "decision"

	mcpElicitationActionAccept  = "accept"
	mcpElicitationActionDecline = "decline"
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

type serverApprovalRequestData struct {
	method      string
	id          string
	toolCallID  string
	toolName    string
	title       string
	description string
	plan        string
	input       map[string]any
}

func (r *activeRun) handleServerRequest(ctx context.Context, msg appserver.Message) (any, error) {
	if isApprovalRequestMethod(msg.Method) {
		return r.handleApprovalServerRequest(ctx, msg)
	}
	if isInputRequestMethod(msg.Method) {
		return r.handleInputServerRequest(ctx, msg)
	}
	switch msg.Method {
	case serverRequestMethodDynamicToolCall:
		return r.handleDynamicToolCallServerRequest(msg), nil
	default:
		return nil, fmt.Errorf("unsupported Codex server request %s", msg.Method)
	}
}

func (r *activeRun) handleApprovalServerRequest(ctx context.Context, msg appserver.Message) (any, error) {
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
}

func (r *activeRun) handleInputServerRequest(ctx context.Context, msg appserver.Message) (any, error) {
	pending, request, err := r.newInputRequest(msg.Method, serverRequestID(msg.ID), msg.Params)
	if err != nil {
		return nil, err
	}
	response, err := r.waitForServerRequest(ctx, pending, request)
	if err != nil {
		return nil, err
	}
	return codexInputResponse(pending, response), nil
}

func (r *activeRun) handleDynamicToolCallServerRequest(msg appserver.Message) any {
	payload := rawPayload(msg.Params)
	response := map[string]any{
		"contentItems": []map[string]any{{
			"type": "inputText",
			"text": unsupportedDynamicToolCallText,
		}},
		"success": false,
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.writer != nil {
		callID := firstNonEmptyString(firstString(payload, "callId", "itemId", "id"), serverRequestID(msg.ID), "dynamic_tool_call")
		name := dynamicToolCallName(payload)
		input := copyStateMap(payload)
		r.ensureToolStartedWithMetadata(callID, name, map[string]any{"codex": map[string]any{
			"request": serverRequestMethodDynamicToolCall,
			"callId":  callID,
			"name":    name,
		}})
		if len(input) > 0 {
			r.writeToolArgsText(callID, compactJSONString(input), input)
		}
		r.writeToolResult(callID, unsupportedDynamicToolCallText, agui.ToolResultStateError)
		r.endToolCall(callID, name, input, response)
	}
	r.publishLocked()
	return response
}

func dynamicToolCallName(payload map[string]any) string {
	namespace := firstString(payload, "namespace")
	tool := firstString(payload, "tool", "name")
	if namespace != "" && tool != "" {
		return colonLabel(namespace, tool)
	}
	return firstNonEmptyString(tool, namespace, "dynamic tool")
}

func serverRequestID(id any) string {
	if id == nil {
		return ""
	}
	switch value := id.(type) {
	case string:
		return firstTrimmedNonEmpty(value)
	case json.Number:
		return value.String()
	case float64:
		if value != float64(int64(value)) {
			return fmt.Sprintf("%v", value)
		}
		return fmt.Sprintf("%d", int64(value))
	case int:
		return fmt.Sprintf("%d", value)
	case int64:
		return fmt.Sprintf("%d", value)
	default:
		return firstTrimmedNonEmpty(fmt.Sprint(id))
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
		r.discardPendingRequest(pending.ID)
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
	ctxMeta := r.approvalContext(request)
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

func (r *activeRun) approvalContext(request aistream.ApprovalRequest) aistream.ApprovalContext {
	choices := request.Choices
	if len(choices) == 0 {
		choices = aistream.DefaultApprovalChoices()
	}
	expiresAt := ""
	if !request.ExpiresAt.IsZero() {
		expiresAt = request.ExpiresAt.UTC().Format(time.RFC3339)
	}
	return aistream.ApprovalContext{
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
	approvalID := serverRequestFallbackID(method, r.turnID, fallbackID, firstString(input, "approvalId", "itemId", "callId"))
	toolCallID := firstNonEmptyString(firstString(input, "itemId", "callId"), approvalID)
	toolName := firstNonEmptyString(approvalRequestKind(method), method)
	title := approvalTitle(method, input)
	description := firstString(input, "reason")
	plan := approvalPlan(method, input)
	pending := newPendingServerRequest(method, approvalID, toolCallID, toolName, input, nil)
	request := newServerApprovalRequest(serverApprovalRequestData{
		method:      method,
		id:          approvalID,
		toolCallID:  toolCallID,
		toolName:    toolName,
		title:       title,
		description: description,
		plan:        plan,
		input:       input,
	})
	return pending, request, nil
}

func (r *activeRun) newInputRequest(method, fallbackID string, raw json.RawMessage) (*pendingServerRequest, aistream.ApprovalRequest, error) {
	input := rawPayload(raw)
	requestID := serverRequestFallbackID(method, r.turnID, fallbackID, firstString(input, "itemId", "elicitationId", "requestId"))
	questions := inputQuestionIDs(method, input)
	toolName := inputRequestToolName(method, input)
	pending := newPendingServerRequest(method, requestID, requestID, toolName, input, questions)
	description := inputDescription(input)
	plan := inputPlan(requestID, questions, description)
	request := newServerApprovalRequest(serverApprovalRequestData{
		method:      method,
		id:          requestID,
		toolCallID:  requestID,
		toolName:    toolName,
		title:       inputRequestTitle(method, input),
		description: description,
		plan:        plan,
		input:       input,
	})
	return pending, request, nil
}

func serverRequestFallbackID(method, turnID, fallbackID, payloadID string) string {
	return firstNonEmptyString(payloadID, firstTrimmedNonEmpty(fallbackID), method+":"+turnID)
}

func newPendingServerRequest(method, id, toolCallID, toolName string, input map[string]any, questionIDs []string) *pendingServerRequest {
	return &pendingServerRequest{
		ID:          id,
		Method:      method,
		ToolCallID:  toolCallID,
		ToolName:    toolName,
		Input:       input,
		QuestionIDs: questionIDs,
		Response:    make(chan any, 1),
	}
}

func inputRequestTitle(method string, input map[string]any) string {
	title := "Input requested"
	if isMCPElicitationRequestMethod(method) {
		title = "MCP input requested"
	}
	return firstNonEmptyString(firstString(input, "message"), title)
}

func inputRequestToolName(method string, input map[string]any) string {
	if !isMCPElicitationRequestMethod(method) {
		return "request_user_input"
	}
	if server := firstString(input, "serverName"); server != "" {
		return "mcp:" + server
	}
	return "mcp_elicitation"
}

func isMCPElicitationRequestMethod(method string) bool {
	return method == serverRequestMethodMCPElicitation
}

func isInputRequestMethod(method string) bool {
	return method == serverRequestMethodUserInput || isMCPElicitationRequestMethod(method)
}

func newServerApprovalRequest(data serverApprovalRequestData) aistream.ApprovalRequest {
	return aistream.ApprovalRequest{
		ID:          data.id,
		ToolCallID:  data.toolCallID,
		ToolName:    data.toolName,
		Title:       data.title,
		Description: data.description,
		PlanText:    data.plan,
		Input:       data.input,
		Approval:    aistream.ToolApproval{ID: data.id, NeedsApproval: true},
		Choices:     aistream.DefaultApprovalChoices(),
		Metadata: map[string]any{
			"method": data.method,
			"params": data.input,
		},
		ExpiresAt: time.Now().Add(serverRequestTimeout),
	}
}

func (r *activeRun) resolvePendingResponse(response aistream.ToolApprovalResponse) bool {
	response.RespondedAt = pendingResponseTime(response.RespondedAt)
	return r.resolvePendingValue(response.ID, response)
}

func (r *activeRun) resolvePendingValue(id string, value any) bool {
	r.mu.Lock()
	pending, ok := r.takePendingRequestLocked(id)
	if !ok {
		r.mu.Unlock()
		return false
	}
	if response, ok := pendingApprovalResponse(pending, value); ok {
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

func (r *activeRun) pendingRequest(id string) *pendingServerRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.pending[id]
}

func (r *activeRun) discardPendingRequest(id string) {
	r.mu.Lock()
	delete(r.pending, id)
	r.mu.Unlock()
}

func (r *activeRun) takePendingRequestLocked(id string) (*pendingServerRequest, bool) {
	pending := r.pending[id]
	if pending == nil {
		return nil, false
	}
	delete(r.pending, id)
	return pending, true
}

func pendingApprovalResponse(pending *pendingServerRequest, value any) (aistream.ToolApprovalResponse, bool) {
	if response, ok := value.(aistream.ToolApprovalResponse); ok {
		return response, true
	}
	return wrappedPendingApprovalResponse(pending, value)
}

func wrappedPendingApprovalResponse(pending *pendingServerRequest, value any) (aistream.ToolApprovalResponse, bool) {
	if pending == nil || pending.ID == "" {
		return aistream.ToolApprovalResponse{}, false
	}
	return aistream.ToolApprovalResponse{
		ID:          pending.ID,
		Approved:    true,
		Choice:      "answer",
		RespondedAt: pendingResponseTime(""),
		Metadata:    pendingResponseMetadata(pending.Method, value),
	}, true
}

func pendingResponseMetadata(method string, response any) map[string]any {
	return map[string]any{
		"method":   method,
		"response": response,
	}
}

func pendingResponseTime(value string) string {
	if value != "" {
		return value
	}
	return time.Now().UTC().Format(time.RFC3339)
}

func (cl *Client) handleBridgeCommand(ctx context.Context, msg *bridgev2.MatrixMessage) (*bridgev2.MatrixMessageResponse, bool, error) {
	if msg == nil {
		return nil, false, nil
	}
	command, ok := parseCodexCommand(msg.Content)
	if !ok && msg.Event != nil {
		command, ok = codexCommandFromRawContent(msg.Event.Content.Raw)
	}
	if !ok {
		return nil, false, nil
	}
	meta := portalMetadata(msg.Portal.Metadata)
	if meta.ThreadID == "" {
		return cl.commandNoticeHandled(msg, "", "no_session", "No Codex session is active in this room.")
	}
	active := cl.activeRunForThread(meta.ThreadID)
	switch command.name {
	case "approvals":
		text := noPendingCodexRequestsText
		if active != nil {
			text = active.pendingRequestsText()
		}
		return cl.commandNoticeHandled(msg, meta.ThreadID, "approvals", text)
	case "approve":
		if active == nil {
			return cl.commandNoticeHandled(msg, meta.ThreadID, "no_pending_approval", "No Codex turn is waiting for an approval.")
		}
		response, ok := parseApprovalCommandResponse(command.arg)
		if !ok || !active.resolvePendingResponse(response) {
			return cl.commandNoticeHandled(msg, meta.ThreadID, "invalid_approval", "That approval was not pending, or the response was invalid.\n\nUse `/approvals` to list pending Codex requests.")
		}
		active.writeCodexClientRequestState("command/approve", approvalCommandState(response))
		return cl.commandHandledResponse(msg, "approve"), true, nil
	case "answer":
		if active == nil {
			return cl.commandNoticeHandled(msg, meta.ThreadID, "no_pending_input", "No Codex turn is waiting for input.")
		}
		requestID, answer, ok := splitCommandArg(command.arg)
		if !ok || !active.resolvePendingAnswer(requestID, answer) {
			return cl.commandNoticeHandled(msg, meta.ThreadID, "invalid_answer", "That input request was not pending, or the response was invalid.\n\nUse `/approvals` to list pending Codex requests.")
		}
		active.writeCodexClientRequestState("command/answer", map[string]any{"id": requestID})
		return cl.commandHandledResponse(msg, "answer"), true, nil
	case "stop":
		if active == nil {
			return cl.commandNoticeHandled(msg, meta.ThreadID, "no_active_turn", "No active Codex turn is running.")
		}
		go cl.interruptTurn(context.WithoutCancel(ctx), msg.Portal, meta.ThreadID, active.turnID)
		return cl.commandHandledResponse(msg, "stop"), true, nil
	default:
		return nil, false, nil
	}
}

func (cl *Client) activeRunForThread(threadID string) *activeRun {
	if cl == nil || cl.Main == nil {
		return nil
	}
	return cl.Main.activeRun(threadID)
}

func (r *activeRun) resolvePendingAnswer(requestID, answer string) bool {
	pending := r.pendingRequest(requestID)
	if pending == nil {
		return false
	}
	value, err := answerResponse(pending, answer)
	return err == nil && r.resolvePendingValue(requestID, value)
}

func parseApprovalCommandResponse(args string) (aistream.ToolApprovalResponse, bool) {
	approvalID, rawChoice, _ := splitCommandArg(firstTrimmedNonEmpty(args))
	if approvalID == "" {
		return aistream.ToolApprovalResponse{}, false
	}
	choice, ok := resolveRawApprovalChoice(rawChoice)
	if !ok {
		return aistream.ToolApprovalResponse{}, false
	}
	return aistream.ApprovalResponseForChoice(approvalID, choice), true
}

func resolveRawApprovalChoice(rawChoice string) (aistream.ApprovalChoice, bool) {
	rawChoice = firstTrimmedNonEmpty(rawChoice, "approve")
	return aistream.ResolveApprovalChoice(aistream.DefaultApprovalChoices(), rawChoice)
}

func splitCommandArg(arg string) (head, tail string, ok bool) {
	head, tail, ok = strings.Cut(arg, " ")
	return firstTrimmedNonEmpty(head), firstTrimmedNonEmpty(tail), ok
}

func approvalCommandState(response aistream.ToolApprovalResponse) map[string]any {
	state := map[string]any{
		"id":       response.ID,
		"approved": response.Approved,
		"choice":   approvalCommandChoice(response),
	}
	if response.Always {
		state["always"] = true
	}
	setNonEmptyMapString(state, "reason", response.Reason)
	return state
}

func approvalCommandChoice(response aistream.ToolApprovalResponse) string {
	switch {
	case response.Always:
		return "always"
	case response.Approved:
		return "approve"
	default:
		return "deny"
	}
}

type codexCommand struct {
	name string
	arg  string
}

func codexCommandWithArg(name, arg string) codexCommand {
	return codexCommand{name: name, arg: firstTrimmedNonEmpty(arg)}
}

func parseCodexCommand(content *event.MessageEventContent) (codexCommand, bool) {
	if content == nil {
		return codexCommand{}, false
	}
	if command, ok := codexStructuredCommand(content.MSC4391BotCommand); ok {
		return command, true
	}
	body, ok := codexTextCommandBody(content)
	if !ok {
		return codexCommand{}, false
	}
	name, arg, _ := splitCommandArg(body)
	name = canonicalCodexCommandName(name)
	if name == "" {
		return codexCommand{}, false
	}
	return codexCommandWithArg(name, arg), true
}

func codexStructuredCommand(input *event.MSC4391BotCommandInput) (codexCommand, bool) {
	if input == nil {
		return codexCommand{}, false
	}
	name := canonicalCodexCommandName(input.Command)
	if name == "" {
		return codexCommand{}, false
	}
	args, ok := codexCommandArguments(input.Arguments)
	if !ok {
		return codexCommand{}, false
	}
	return codexCommandFromArgsMap(name, args), true
}

func codexTextCommandBody(content *event.MessageEventContent) (string, bool) {
	body := firstTrimmedNonEmpty(content.Body)
	if command, ok := strings.CutPrefix(body, "/"); ok {
		return command, true
	}
	if command, ok := strings.CutPrefix(body, "!codex "); ok {
		return firstTrimmedNonEmpty(command), true
	}
	if content.MsgType == matrixCommandMsgType {
		return body, true
	}
	return "", false
}

func codexCommandArguments(raw json.RawMessage) (map[string]any, bool) {
	args := map[string]any{}
	if len(raw) == 0 {
		return args, true
	}
	if err := json.Unmarshal(raw, &args); err != nil {
		return nil, false
	}
	return args, true
}

func canonicalCodexCommandName(name string) string {
	switch lowerTrimmed(name) {
	case "approve":
		return "approve"
	case "approval", "approvals":
		return "approvals"
	case "answer":
		return "answer"
	case "stop":
		return "stop"
	default:
		return ""
	}
}

func (r *activeRun) pendingRequestsText() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	entries := sortedPendingRequests(r.pending)
	if len(entries) == 0 {
		return noPendingCodexRequestsText
	}
	out := []string{"Pending Codex requests:"}
	for _, entry := range entries {
		out = append(out, pendingRequestText(entry)...)
	}
	return strings.Join(out, "\n")
}

func pendingRequestText(entry pendingEntry) []string {
	request := entry.request
	if isApprovalRequestMethod(request.Method) {
		return pendingApprovalRequestText(entry.id, request)
	}
	return pendingInputRequestText(entry.id, request)
}

func pendingApprovalRequestText(id string, request *pendingServerRequest) []string {
	out := pendingRequestIntroLines(id, approvalTitle(request.Method, request.Input))
	if plan := approvalPlan(request.Method, request.Input); plan != "" {
		out = append(out, "", plan)
	}
	return append(out, pendingApprovalResponseLines(id)...)
}

func pendingApprovalResponseLines(id string) []string {
	lines := []string{
		"",
		"Respond with one of:",
	}
	for _, choice := range pendingApprovalResponseChoices {
		lines = append(lines, approvalCommandLine(id, choice))
	}
	return lines
}

var pendingApprovalResponseChoices = []string{"approve", "always", "deny"}

func approvalCommandLine(id, choice string) string {
	return "- `/approve " + id + " " + choice + "`"
}

func pendingInputRequestText(id string, request *pendingServerRequest) []string {
	title := pendingInputRequestTitle(request)
	return append(pendingRequestIntroLines(id, title), pendingInputRequestPlan(request))
}

func pendingRequestHeading(id string) string {
	return "### " + id
}

func pendingRequestIntroLines(id, title string) []string {
	return []string{"", pendingRequestHeading(id), title}
}

func pendingInputRequestPlan(request *pendingServerRequest) string {
	if request.ID == "" {
		return "Respond with `/answer <id> <answer>`."
	}
	return inputPlan(request.ID, request.QuestionIDs, inputDescription(request.Input))
}

type pendingEntry struct {
	id      string
	request *pendingServerRequest
}

func sortedPendingRequests(pending map[string]*pendingServerRequest) []pendingEntry {
	entries := make([]pendingEntry, 0, len(pending))
	for id, req := range pending {
		entry, ok := newPendingEntry(id, req)
		if ok {
			entries = append(entries, entry)
		}
	}
	sort.Slice(entries, func(i, j int) bool { return pendingEntryBefore(entries[i], entries[j]) })
	return entries
}

func newPendingEntry(id string, request *pendingServerRequest) (pendingEntry, bool) {
	if request == nil {
		return pendingEntry{}, false
	}
	return pendingEntry{id: id, request: request}, true
}

func pendingEntryBefore(left, right pendingEntry) bool {
	return left.id < right.id
}

func pendingInputRequestTitle(request *pendingServerRequest) string {
	if message := firstString(request.Input, "message"); message != "" {
		return message
	}
	return prefixedLabel("Input requested by", request.ToolName)
}

func isApprovalRequestMethod(method string) bool {
	return approvalRequestKind(method) != ""
}

func approvalRequestKind(method string) string {
	switch method {
	case serverRequestMethodCommandApproval, serverRequestMethodLegacyCommandApproval:
		return approvalRequestKindCommandExecution
	case serverRequestMethodFileChangeApproval, serverRequestMethodLegacyFileChangeApproval:
		return approvalRequestKindFileChange
	case serverRequestMethodPermissionsApproval:
		return approvalRequestKindPermissions
	default:
		return ""
	}
}

func (cl *Client) commandNoticeHandled(msg *bridgev2.MatrixMessage, threadID, status, text string) (*bridgev2.MatrixMessageResponse, bool, error) {
	cl.queueCommandNotice(msg.Portal, threadID, text)
	return cl.commandHandledResponse(msg, status), true, nil
}

func (cl *Client) commandHandledResponse(msg *bridgev2.MatrixMessage, status string) *bridgev2.MatrixMessageResponse {
	meta := portalMetadata(nil)
	if msg != nil && msg.Portal != nil {
		meta = portalMetadata(msg.Portal.Metadata)
	}
	timestamp := matrixMessageTimestamp(msg)
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
	if cl == nil || cl.UserLogin == nil || cl.UserLogin.Bridge == nil || portal == nil || firstTrimmedNonEmpty(text) == "" {
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
			content, extra, err := matrixFinalContent(ctx, portal, intent, data)
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

func codexApprovalResponse(pending *pendingServerRequest, response aistream.ToolApprovalResponse) any {
	switch pending.Method {
	case serverRequestMethodCommandApproval, serverRequestMethodFileChangeApproval:
		return map[string]any{codexDecisionResponseKey: commandApprovalDecision(response)}
	case serverRequestMethodPermissionsApproval:
		return permissionApprovalResponse(pending, response)
	case serverRequestMethodLegacyFileChangeApproval, serverRequestMethodLegacyCommandApproval:
		return map[string]any{codexDecisionResponseKey: legacyApprovalDecision(response)}
	default:
		return map[string]any{codexDecisionResponseKey: approvalDecisionKindDenied}
	}
}

func permissionApprovalResponse(pending *pendingServerRequest, response aistream.ToolApprovalResponse) map[string]any {
	return map[string]any{
		approvalPermissionsKey: approvalPermissions(pending, response),
		"scope":                approvalPermissionScope(response),
	}
}

var commandApprovalDecisions = map[string]string{
	approvalDecisionKindAlways:   "acceptForSession",
	approvalDecisionKindApproved: "accept",
	approvalDecisionKindCanceled: "cancel",
}

var legacyApprovalDecisions = map[string]string{
	approvalDecisionKindAlways:   "approved_for_session",
	approvalDecisionKindApproved: "approved",
	approvalDecisionKindCanceled: "abort",
}

func commandApprovalDecision(response aistream.ToolApprovalResponse) string {
	return mappedApprovalDecision(approvalDecisionKind(response), commandApprovalDecisions, "decline")
}

func legacyApprovalDecision(response aistream.ToolApprovalResponse) string {
	decision := mappedApprovalDecision(approvalDecisionKind(response), legacyApprovalDecisions, "")
	if decision != "" {
		return decision
	}
	if response.Reason == "timed_out" {
		return "timed_out"
	}
	return approvalDecisionKindDenied
}

func mappedApprovalDecision(kind string, decisions map[string]string, fallback string) string {
	if decision, ok := decisions[kind]; ok {
		return decision
	}
	return fallback
}

func approvalDecisionKind(response aistream.ToolApprovalResponse) string {
	switch {
	case response.Approved && response.Always:
		return approvalDecisionKindAlways
	case response.Approved:
		return approvalDecisionKindApproved
	case approvalResponseCanceled(response):
		return approvalDecisionKindCanceled
	default:
		return approvalDecisionKindDenied
	}
}

func approvalPermissions(pending *pendingServerRequest, response aistream.ToolApprovalResponse) map[string]any {
	if !response.Approved {
		return map[string]any{}
	}
	if requested, ok := requestedApprovalPermissions(pending.Input); ok {
		return requested
	}
	return map[string]any{}
}

func requestedApprovalPermissions(input map[string]any) (map[string]any, bool) {
	permissions, ok := input[approvalPermissionsKey].(map[string]any)
	return permissions, ok
}

func approvalPermissionScope(response aistream.ToolApprovalResponse) string {
	if !response.Always {
		return approvalPermissionScopeTurn
	}
	return approvalPermissionScopeSession
}

func approvalResponseCanceled(response aistream.ToolApprovalResponse) bool {
	return response.Reason == "aborted" || response.Choice == "cancel"
}

func codexInputResponse(pending *pendingServerRequest, response any) any {
	switch pending.Method {
	case serverRequestMethodUserInput:
		return codexUserInputResponse(response)
	case serverRequestMethodMCPElicitation:
		return codexMcpElicitationResponse(response)
	default:
		return response
	}
}

func codexUserInputResponse(response any) map[string]any {
	if _, isApproval := response.(aistream.ToolApprovalResponse); isApproval {
		response = map[string]any{}
	}
	return map[string]any{codexAnswerResponseKey: response}
}

func codexMcpElicitationResponse(response any) map[string]any {
	approval, isApproval := response.(aistream.ToolApprovalResponse)
	if !isApproval {
		return map[string]any{"action": mcpElicitationActionAccept, "content": response, "_meta": nil}
	}
	action := mcpElicitationActionAccept
	if !approval.Approved {
		action = mcpElicitationActionDecline
	}
	return map[string]any{"action": action, "content": nil, "_meta": nil}
}

func answerResponse(pending *pendingServerRequest, raw string) (any, error) {
	if isMCPElicitationRequestMethod(pending.Method) {
		return mcpElicitationAnswerResponse(pending, raw)
	}
	values, err := namedAnswerValues(answerQuestionIDs(pending), raw)
	if err != nil {
		return nil, err
	}
	return codexAnswerMap(values), nil
}

func codexAnswerMap(values map[string]string) map[string]any {
	answers := map[string]any{}
	for key, value := range values {
		answers[key] = map[string]any{codexAnswerResponseKey: []string{value}}
	}
	return answers
}

func answerQuestionIDs(pending *pendingServerRequest) []string {
	if len(pending.QuestionIDs) != 0 {
		return pending.QuestionIDs
	}
	return []string{defaultAnswerQuestionID}
}

func mcpElicitationAnswerResponse(pending *pendingServerRequest, raw string) (any, error) {
	var decoded any
	if json.Unmarshal([]byte(raw), &decoded) == nil {
		return decoded, nil
	}
	if len(pending.QuestionIDs) == 0 {
		return mcpDefaultAnswerResponse(raw), nil
	}
	return namedAnswerValues(pending.QuestionIDs, raw)
}

func mcpDefaultAnswerResponse(raw string) map[string]any {
	return map[string]any{defaultAnswerQuestionID: raw}
}

func namedAnswerValues(ids []string, raw string) (map[string]string, error) {
	if hasAnswerAssignments(raw) {
		return parseAnswerAssignments(raw)
	}
	if requiresNamedAnswers(ids) {
		return nil, fmt.Errorf("multiple answers require question_id=value syntax")
	}
	return singleAnswerValue(ids, raw), nil
}

func hasAnswerAssignments(raw string) bool {
	return strings.Contains(raw, "=")
}

func singleAnswerValue(ids []string, raw string) map[string]string {
	return map[string]string{singleAnswerID(ids): raw}
}

func singleAnswerID(ids []string) string {
	if len(ids) != 0 {
		return ids[0]
	}
	return defaultAnswerQuestionID
}

func parseAnswerAssignments(raw string) (map[string]string, error) {
	out := map[string]string{}
	for i := 0; i < len(raw); {
		i = skipAnswerSpaces(raw, i)
		if i == len(raw) {
			break
		}
		key, value, next, err := parseAnswerAssignment(raw, i)
		if err != nil {
			return nil, err
		}
		out[key] = value
		i = next
	}
	return out, nil
}

func parseAnswerAssignment(raw string, i int) (key, value string, next int, err error) {
	key, next, err = parseAnswerKey(raw, i)
	if err != nil {
		return "", "", next, err
	}
	value, next, err = parseAnswerValue(raw, next)
	if err != nil {
		return "", "", next, err
	}
	return key, value, next, nil
}

func parseAnswerKey(raw string, i int) (string, int, error) {
	start := i
	i = answerKeyEnd(raw, i)
	if invalidAnswerKeyRange(raw, start, i) {
		return "", i, fmt.Errorf("invalid answer assignment near %q", raw[start:])
	}
	return raw[start:i], i + 1, nil
}

func answerKeyEnd(raw string, i int) int {
	for i < len(raw) && raw[i] != '=' && !isAnswerSpace(raw[i]) {
		i++
	}
	return i
}

func invalidAnswerKeyRange(raw string, start, end int) bool {
	return start == end || end == len(raw) || raw[end] != '='
}

func skipAnswerSpaces(raw string, i int) int {
	for i < len(raw) && isAnswerSpace(raw[i]) {
		i++
	}
	return i
}

func parseAnswerValue(raw string, i int) (string, int, error) {
	if i >= len(raw) {
		return "", i, nil
	}
	if isQuotedAnswerValue(raw[i]) {
		return parseQuotedAnswerValue(raw, i)
	}
	return parseUnquotedAnswerValue(raw, i)
}

func isQuotedAnswerValue(ch byte) bool {
	return ch == '"' || ch == '\''
}

func parseUnquotedAnswerValue(raw string, i int) (string, int, error) {
	start := i
	i = unquotedAnswerValueEnd(raw, i)
	return raw[start:i], i, nil
}

func unquotedAnswerValueEnd(raw string, i int) int {
	for i < len(raw) && !isAnswerSpace(raw[i]) {
		i++
	}
	return i
}

func parseQuotedAnswerValue(raw string, i int) (string, int, error) {
	quote := raw[i]
	i++
	var b strings.Builder
	for i < len(raw) {
		ch := raw[i]
		i++
		if ch == quote {
			if invalidQuotedAnswerSuffix(raw, i) {
				return "", i, fmt.Errorf("invalid quoted answer value near %q", raw[i:])
			}
			return b.String(), i, nil
		}
		ch, i = quotedAnswerByte(raw, ch, i)
		b.WriteByte(ch)
	}
	return "", i, fmt.Errorf("unterminated quoted answer value")
}

func quotedAnswerByte(raw string, ch byte, i int) (byte, int) {
	if ch == '\\' && i < len(raw) {
		return raw[i], i + 1
	}
	return ch, i
}

func invalidQuotedAnswerSuffix(raw string, i int) bool {
	return i < len(raw) && !isAnswerSpace(raw[i])
}

func isAnswerSpace(ch byte) bool {
	return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r'
}

func approvalTitle(method string, input map[string]any) string {
	switch approvalRequestKind(method) {
	case approvalRequestKindCommandExecution:
		return commandApprovalTitle(input)
	case approvalRequestKindFileChange:
		return "Approve file changes?"
	case approvalRequestKindPermissions:
		return "Approve permissions?"
	default:
		return "Approve Codex request?"
	}
}

func commandApprovalTitle(input map[string]any) string {
	if command := commandString(input); command != "" {
		return colonLabel("Approve command", command)
	}
	return "Approve command?"
}

func approvalPlan(method string, input map[string]any) string {
	return joinLinesOrFallback(approvalPlanLines(input), method)
}

func approvalPlanLines(input map[string]any) []string {
	var lines []string
	appendNonEmptyLabelLine(&lines, "Command", commandString(input))
	appendNonEmptyLabelLine(&lines, "Directory", firstString(input, "cwd"))
	appendNonEmptyLabelLine(&lines, "Reason", firstString(input, "reason"))
	appendNonEmptyLabelLine(&lines, "Grant root", firstString(input, "grantRoot"))
	return lines
}

func inputDescription(input map[string]any) string {
	var lines []string
	appendNonEmptyLine(&lines, firstString(input, "message"))
	appendNonEmptyLabelLine(&lines, "URL", firstString(input, "url"))
	appendNonEmptyLine(&lines, mcpElicitationFieldsLine(input))
	appendInputQuestionLines(&lines, inputQuestions(input))
	return strings.Join(lines, "\n")
}

func mcpElicitationFieldsLine(input map[string]any) string {
	fields := mcpElicitationFields(input)
	if len(fields) == 0 {
		return ""
	}
	return colonLabel("Fields", strings.Join(fields, ", "))
}

func appendInputQuestionLines(lines *[]string, questions []map[string]any) {
	for _, question := range questions {
		appendNonEmptyLine(lines, inputQuestionLine(question))
	}
}

func inputQuestionLine(question map[string]any) string {
	return trimmedSpaceJoin(firstString(question, "header"), firstString(question, "question"))
}

func appendNonEmptyLabelLine(lines *[]string, label, value string) {
	if value == "" {
		return
	}
	appendNonEmptyLine(lines, colonLabel(label, value))
}

func appendNonEmptyLine(lines *[]string, line string) {
	if line != "" {
		*lines = append(*lines, line)
	}
}

func joinLinesOrFallback(lines []string, fallback string) string {
	if len(lines) == 0 {
		return fallback
	}
	return strings.Join(lines, "\n")
}

func inputQuestionIDs(method string, input map[string]any) []string {
	questions := questionIDs(inputQuestions(input))
	if useMCPFieldQuestionIDs(method, questions) {
		return mcpElicitationFieldNames(input)
	}
	return questions
}

func useMCPFieldQuestionIDs(method string, questions []string) bool {
	return len(questions) == 0 && isMCPElicitationRequestMethod(method)
}

func questionIDs(rawQuestions []map[string]any) []string {
	questions := make([]string, 0, len(rawQuestions))
	for _, question := range rawQuestions {
		appendNonEmptyLine(&questions, firstString(question, "id"))
	}
	return questions
}

func mcpElicitationFieldNames(input map[string]any) []string {
	fields := mcpElicitationFields(input)
	names := make([]string, 0, len(fields))
	for _, field := range fields {
		appendNonEmptyLine(&names, mcpElicitationFieldName(field))
	}
	return names
}

func mcpElicitationFieldName(descriptor string) string {
	name, _, _ := strings.Cut(descriptor, " ")
	return name
}

func inputQuestions(input map[string]any) []map[string]any {
	rawQuestions, _ := input["questions"].([]any)
	questions := make([]map[string]any, 0, len(rawQuestions))
	for _, raw := range rawQuestions {
		if question, _ := raw.(map[string]any); question != nil {
			questions = append(questions, question)
		}
	}
	return questions
}

func mcpElicitationFields(input map[string]any) []string {
	schema := mcpElicitationSchema(input)
	properties := mcpElicitationProperties(schema)
	if len(properties) == 0 {
		return nil
	}
	required := stringList(schema["required"])
	fields := make([]string, 0, len(properties))
	for name, raw := range properties {
		appendNonEmptyLine(&fields, mcpElicitationFieldDescriptor(name, raw, required))
	}
	sort.Strings(fields)
	return fields
}

func mcpElicitationSchema(input map[string]any) map[string]any {
	schema, _ := input["requestedSchema"].(map[string]any)
	return schema
}

func mcpElicitationProperties(schema map[string]any) map[string]any {
	properties, _ := schema["properties"].(map[string]any)
	return properties
}

func mcpElicitationFieldDescriptor(name string, raw any, required []string) string {
	name = firstTrimmedNonEmpty(name)
	if name == "" {
		return ""
	}
	descriptor := name
	prop, _ := raw.(map[string]any)
	descriptor += mcpElicitationRequiredSuffix(name, required)
	descriptor += mcpElicitationDetailSuffix(prop)
	return descriptor
}

func mcpElicitationRequiredSuffix(name string, required []string) string {
	if slices.Contains(required, name) {
		return " (required)"
	}
	return ""
}

func mcpElicitationDetailSuffix(prop map[string]any) string {
	if detail := firstString(prop, "title", "description"); detail != "" {
		return " - " + detail
	}
	return ""
}

func inputPlan(requestID string, questionIDs []string, description string) string {
	reply := answerCommandUsage(requestID, questionIDs)
	return inputPlanText(description, reply)
}

func answerCommandUsage(requestID string, questionIDs []string) string {
	if requiresNamedAnswers(questionIDs) {
		return "Reply with `/answer " + requestID + " question_id=answer ...`."
	}
	return "Reply with `/answer " + requestID + " <answer>`."
}

func inputPlanText(description, reply string) string {
	if description := firstTrimmedNonEmpty(description); description != "" {
		return description + "\n\n" + reply
	}
	return reply
}

func requiresNamedAnswers(questionIDs []string) bool {
	return len(questionIDs) > 1
}

func commandString(input map[string]any) string {
	if command := firstString(input, "command"); command != "" {
		return command
	}
	parts, _ := input["command"].([]any)
	return commandPartsString(parts)
}

func commandPartsString(parts []any) string {
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		appendCommandPart(&out, part)
	}
	return strings.Join(out, " ")
}

func appendCommandPart(out *[]string, part any) {
	text, ok := part.(string)
	if !ok {
		return
	}
	*out = append(*out, text)
}
