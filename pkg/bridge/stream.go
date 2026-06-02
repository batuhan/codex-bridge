package bridge

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	agui "github.com/beeper/ai-bridge/pkg/ag-ui"
	aistream "github.com/beeper/ai-bridge/pkg/ai-stream"
	aibridgev2 "github.com/beeper/ai-bridge/pkg/ai-stream/bridgev2"
	aimatrix "github.com/beeper/ai-bridge/pkg/ai-stream/matrix"
)

type activeRun struct {
	mu         sync.Mutex
	client     *Client
	portalKey  networkid.PortalKey
	threadID   string
	turnID     string
	messageID  networkid.MessageID
	anchorMXID id.EventID
	roomID     id.RoomID
	publisher  bridgev2.BeeperStreamPublisher
	run        *aistream.Run
	writer     *aistream.Writer
	pending    map[string]*pendingServerRequest
	processes  map[string]string
	toolResult map[string]bool
	agentText  map[string]string
	reasoning  map[string]string
	nextSeq    int
	started    bool
}

func newActiveRun(cl *Client, portalKey networkid.PortalKey, threadID, turnID string) *activeRun {
	now := time.Now()
	run := aistream.NewRun(turnID, threadID, "codex", "codex", "Codex", now)
	run.Data["capabilities"] = codexAgentCapabilities()
	return &activeRun{
		client:     cl,
		portalKey:  portalKey,
		threadID:   threadID,
		turnID:     turnID,
		messageID:  networkid.MessageID(run.MessageID),
		run:        run,
		writer:     aistream.NewWriter(run, time.Now),
		pending:    map[string]*pendingServerRequest{},
		processes:  map[string]string{},
		toolResult: map[string]bool{},
		agentText:  map[string]string{},
		reasoning:  map[string]string{},
		nextSeq:    1,
	}
}

func codexAgentCapabilities() agui.AgentCapabilities {
	return agui.AgentCapabilities{
		Identity: &agui.IdentityCapabilities{
			Name:     "Codex",
			Type:     "coding-agent",
			Provider: "openai",
		},
		Transport: &agui.TransportCapabilities{
			Streaming: agui.Bool(true),
		},
		Tools: &agui.ToolsCapabilities{
			Supported:      agui.Bool(true),
			ClientProvided: agui.Bool(false),
		},
		State: &agui.StateCapabilities{
			Deltas:          agui.Bool(true),
			PersistentState: agui.Bool(true),
		},
		Reasoning: &agui.ReasoningCapabilities{
			Supported: agui.Bool(true),
			Streaming: agui.Bool(true),
		},
		Execution: &agui.ExecutionCapabilities{
			CodeExecution: agui.Bool(true),
			Sandboxed:     agui.Bool(false),
		},
		HumanInTheLoop: &agui.HumanInTheLoopCapabilities{
			Supported:  agui.Bool(true),
			Approvals:  agui.Bool(true),
			Interrupts: agui.Bool(true),
		},
	}
}

func (r *activeRun) start(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started {
		return nil
	}
	if r.client == nil || r.client.Main == nil || r.client.UserLogin == nil || r.client.UserLogin.Bridge == nil {
		return fmt.Errorf("missing Codex stream bridge context")
	}
	descriptor, err := r.prepareStreamLocked(ctx)
	if err != nil {
		return err
	}
	r.client.queueCodexTyping(r.portalKey, 30*time.Second)
	r.writer.Start()
	msg := aibridgev2.Anchor(r.portalKey, codexUserID, *r.run, time.Now())
	if len(msg.Data.Parts) > 0 {
		msg.Data.Parts[0].ID = partID("text")
		msg.Data.Parts[0].Content.BeeperStream = descriptor
		msg.Data.Parts[0].DBMetadata = &MessageMetadata{Role: "assistant", ThreadID: r.threadID, TurnID: r.turnID}
	}
	res := r.client.UserLogin.QueueRemoteEvent(msg)
	if !res.Success {
		if res.Error != nil {
			return res.Error
		}
		return fmt.Errorf("failed to queue Codex stream anchor")
	}
	if res.EventID != "" {
		r.anchorMXID = res.EventID
	}
	r.resolveAnchorEventIDLocked(ctx)
	if r.anchorMXID == "" {
		return fmt.Errorf("failed to resolve Codex stream anchor event ID")
	}
	if err := r.publisher.Register(ctx, r.roomID, r.anchorMXID, descriptor); err != nil {
		return fmt.Errorf("failed to register Codex stream publisher: %w", err)
	}
	r.started = true
	r.publishLocked()
	return nil
}

func (r *activeRun) prepareStreamLocked(ctx context.Context) (*event.BeeperStreamInfo, error) {
	publisher := r.client.Main.Bridge.GetBeeperStreamPublisher()
	if publisher == nil {
		return nil, fmt.Errorf("Codex bridge requires a Matrix connector with Beeper stream support")
	}
	roomID := r.roomID
	if roomID == "" {
		portal, err := r.client.Main.Bridge.GetExistingPortalByKey(ctx, r.portalKey)
		if err != nil {
			return nil, err
		}
		if portal == nil || portal.MXID == "" {
			return nil, fmt.Errorf("missing Matrix room for Codex stream")
		}
		roomID = portal.MXID
	}
	descriptor, err := publisher.NewDescriptor(ctx, roomID, "com.beeper.stream")
	if err != nil {
		return nil, fmt.Errorf("failed to create Codex stream descriptor: %w", err)
	}
	r.publisher = publisher
	r.roomID = roomID
	return descriptor, nil
}

func (r *activeRun) resolveAnchorEventIDLocked(ctx context.Context) {
	if r.anchorMXID != "" || r.client == nil || r.client.Main == nil || r.client.Main.Bridge == nil {
		return
	}
	portal, err := r.client.Main.Bridge.GetExistingPortalByKey(ctx, r.portalKey)
	if err != nil || portal == nil {
		return
	}
	r.anchorMXID = r.anchorEventID(ctx, portal)
	if r.anchorMXID == "" && portal.MXID != "" && r.client.Main.Bridge.Matrix != nil {
		r.anchorMXID = r.client.Main.Bridge.Matrix.GenerateDeterministicEventID(portal.MXID, r.portalKey, r.messageID, partID("text"))
	}
}

func (r *activeRun) handle(method string, params json.RawMessage) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.client.queueCodexTyping(r.portalKey, 30*time.Second)
	switch method {
	case "thread/status/changed", "thread/goal/updated", "thread/goal/cleared", "thread/settings/updated", "thread/tokenUsage/updated", "thread/archived", "thread/unarchived", "thread/closed", "thread/name/updated", "thread/compacted":
		r.writer.StateDelta(map[string]any{"codexThread": codexThreadState(method, r.threadID, "", params)})
		r.writer.Custom(method, rawPayload(params))
	case "turn/started":
		r.writer.StepStart(r.turnID)
		r.writer.Custom(method, rawPayload(params))
	case "hook/started":
		payload := rawPayload(params)
		id, name := hookRunIdentity(payload)
		if id != "" {
			r.writer.ToolStartWithMetadata(id, name, 0, nil, map[string]any{"codexHook": payload["run"]})
		}
		r.writer.Custom(method, payload)
	case "hook/completed":
		payload := rawPayload(params)
		id, name := hookRunIdentity(payload)
		if id != "" {
			r.writer.ToolEnd(id, name, payload["run"], map[string]any{"state": toolStateFromStatus(hookRunStatus(payload)), "status": hookRunStatus(payload)})
		}
		r.writer.Custom(method, payload)
	case "item/agentMessage/delta":
		var payload struct {
			ItemID string `json:"itemId"`
			Delta  string `json:"delta"`
		}
		_ = json.Unmarshal(params, &payload)
		if payload.Delta != "" {
			if payload.ItemID != "" {
				r.agentText[payload.ItemID] += payload.Delta
			}
			r.writer.Text(payload.Delta)
		}
	case "item/reasoning/summaryTextDelta", "item/reasoning/textDelta":
		var payload struct {
			Delta        string `json:"delta"`
			ItemID       string `json:"itemId"`
			SummaryIndex *int   `json:"summaryIndex"`
			ContentIndex *int   `json:"contentIndex"`
		}
		_ = json.Unmarshal(params, &payload)
		if payload.Delta != "" {
			index := firstIndex(payload.SummaryIndex, payload.ContentIndex)
			r.reasoning[reasoningContentKey(payload.ItemID, reasoningKind(payload.SummaryIndex), index)] += payload.Delta
			r.writer.ReasoningDelta(reasoningSectionIndex(payload.ItemID, index), payload.Delta)
		}
	case "item/reasoning/summaryPartAdded":
		var payload struct {
			ItemID       string `json:"itemId"`
			SummaryIndex int    `json:"summaryIndex"`
		}
		_ = json.Unmarshal(params, &payload)
		r.writer.ReasoningMessageStart(reasoningSectionIndex(payload.ItemID, &payload.SummaryIndex))
		r.writer.StateDelta(map[string]any{"codex": map[string]any{method: rawPayload(params)}})
		r.writer.Custom(method, rawPayload(params))
	case "item/commandExecution/outputDelta", "item/fileChange/outputDelta", "command/exec/outputDelta", "process/outputDelta":
		if itemID, delta := outputDelta(params); itemID != "" && delta != "" {
			itemID = r.toolIDForProcess(itemID)
			r.writer.ToolResult(itemID, delta, agui.ToolResultStateStreaming)
			r.toolResult[itemID] = true
		}
		r.writer.Custom(method, rawPayload(params))
	case "item/commandExecution/terminalInteraction":
		payload := rawPayload(params)
		itemID, _ := payload["itemId"].(string)
		processID, _ := payload["processId"].(string)
		r.rememberProcessTool(processID, itemID)
		stdin, _ := payload["stdin"].(string)
		if itemID != "" && stdin != "" {
			r.writer.ToolResult(itemID, stdin, agui.ToolResultStateStreaming)
			r.toolResult[itemID] = true
		}
		r.writer.Custom(method, payload)
	case "item/plan/delta":
		payload := rawPayload(params)
		if activity := planDeltaActivity(r.run.MessageID, payload); activity.Len() > 0 {
			r.writer.Add(activity)
		}
		r.writer.StateDelta(map[string]any{"codex": map[string]any{method: payload}})
		r.writer.Custom(method, payload)
	case "turn/plan/updated":
		payload := rawPayload(params)
		r.writer.Add(planSnapshotActivity(r.run.MessageID, payload))
		r.writer.StateDelta(map[string]any{"codex": map[string]any{method: payload}})
		r.writer.Custom(method, payload)
	case "turn/diff/updated":
		payload := rawPayload(params)
		if activity := diffSnapshotActivity(r.run.MessageID, payload); activity.Len() > 0 {
			r.writer.Add(activity)
		}
		r.writer.StateDelta(map[string]any{"codex": map[string]any{method: payload}})
		r.writer.Custom(method, payload)
	case "process/exited":
		payload := rawPayload(params)
		if itemID, result, state := processExitResult(payload); itemID != "" && result != "" {
			itemID = r.toolIDForProcess(itemID)
			r.writer.ToolResult(itemID, result, state)
			r.toolResult[itemID] = true
		}
		r.writer.StateDelta(map[string]any{"codex": map[string]any{method: payload}})
		r.writer.Custom(method, payload)
	case "item/mcpToolCall/progress":
		payload := rawPayload(params)
		itemID, _ := payload["itemId"].(string)
		message, _ := payload["message"].(string)
		if itemID != "" && strings.TrimSpace(message) != "" {
			r.writer.ToolResult(itemID, strings.TrimSpace(message), agui.ToolResultStateStreaming)
			r.toolResult[itemID] = true
		}
		r.writer.StateDelta(map[string]any{"codex": map[string]any{method: payload}})
		r.writer.Custom(method, payload)
	case "item/fileChange/patchUpdated":
		payload := rawPayload(params)
		itemID, _ := payload["itemId"].(string)
		if itemID != "" {
			if text := patchUpdateText(payload); text != "" {
				r.writer.ToolResult(itemID, text, agui.ToolResultStateStreaming)
				r.toolResult[itemID] = true
			}
		}
		r.writer.StateDelta(map[string]any{"codex": map[string]any{method: payload}})
		r.writer.Custom(method, payload)
	case "serverRequest/resolved":
		r.writer.StateDelta(map[string]any{"codex": map[string]any{method: rawPayload(params)}})
		r.writer.Custom(method, rawPayload(params))
	case "rawResponseItem/completed":
		payload := rawPayload(params)
		r.mapRawResponseItem(payload)
		r.writer.StateDelta(map[string]any{"codex": map[string]any{method: payload}})
		r.writer.Custom(method, payload)
	case "item/autoApprovalReview/started", "item/autoApprovalReview/completed":
		payload := rawPayload(params)
		if targetID, _ := payload["targetItemId"].(string); targetID != "" {
			state := agui.ToolResultStateStreaming
			if method == "item/autoApprovalReview/completed" {
				state = agui.ToolResultStateComplete
			}
			r.writer.ToolResult(targetID, approvalReviewText(payload), state)
			r.toolResult[targetID] = true
		}
		r.writer.StateDelta(map[string]any{"codex": map[string]any{method: payload}})
		r.writer.Custom(method, payload)
	case "model/rerouted", "model/verification", "warning", "guardianWarning", "deprecationNotice", "configWarning":
		r.writer.StateDelta(map[string]any{"codex": map[string]any{method: rawPayload(params)}})
		r.writer.Custom(method, rawPayload(params))
	case "error":
		payload := rawPayload(params)
		r.writer.Error(errorNoticeText(payload))
		r.writer.StateDelta(map[string]any{"codex": map[string]any{method: payload}})
		r.writer.Custom(method, payload)
	case "thread/realtime/transcript/delta":
		payload := rawPayload(params)
		if role, _ := payload["role"].(string); role == agui.RoleAssistant {
			if delta, _ := payload["delta"].(string); delta != "" {
				r.writer.Text(delta)
			}
		}
		r.writer.StateDelta(map[string]any{"codexRealtime": map[string]any{method: payload}})
		r.writer.Custom(method, payload)
	case "thread/realtime/started", "thread/realtime/itemAdded", "thread/realtime/transcript/done", "thread/realtime/outputAudio/delta", "thread/realtime/sdp", "thread/realtime/closed":
		r.writer.StateDelta(map[string]any{"codexRealtime": map[string]any{method: rawPayload(params)}})
		r.writer.Custom(method, rawPayload(params))
	case "thread/realtime/error":
		payload := rawPayload(params)
		r.writer.Error(threadNoticeText(method, params))
		r.writer.StateDelta(map[string]any{"codexRealtime": map[string]any{method: payload}})
		r.writer.Custom(method, payload)
	case "item/started":
		item := notificationItem(params)
		if item.Type == "commandExecution" {
			r.rememberProcessTool(firstString(item.Raw, "processId", "processHandle"), item.ID)
		}
		if item.ID != "" && item.IsToolLike() {
			r.writer.ToolStartWithMetadata(item.ID, item.Name(), 0, nil, map[string]any{"codexItem": item.Raw})
		}
		r.writer.Custom(method, rawPayload(params))
	case "item/completed":
		item := notificationItem(params)
		if item.Type == "contextCompaction" {
			r.writer.Text(codexCompactionNotice)
		} else if item.Type == "enteredReviewMode" || item.Type == "exitedReviewMode" {
			r.writer.Text(liveReviewModeText(item))
		} else if item.Type == "agentMessage" {
			r.recoverCompletedAgentMessage(item)
		} else if item.Type == "reasoning" {
			r.recoverCompletedReasoning(item)
		} else if item.Type == "plan" {
			if activity := planItemActivity(r.run.MessageID, item.Raw); activity.Len() > 0 {
				r.writer.Add(activity)
			}
		} else if item.ID != "" && item.IsToolLike() {
			state := codexItemToolState(item.Raw)
			if result := completedToolResultText(item.Raw, r.toolResult[item.ID]); result != "" {
				r.writer.ToolResult(item.ID, result, state)
				r.toolResult[item.ID] = true
			}
			r.writer.ToolEnd(item.ID, item.Name(), item.Raw, map[string]any{"state": state, "status": codexItemStatusText(item.Raw, state)})
		}
		r.writer.Custom(method, rawPayload(params))
	case "turn/completed":
		var payload struct {
			Turn struct {
				Status string `json:"status"`
				Error  *struct {
					Message string `json:"message"`
				} `json:"error"`
			} `json:"turn"`
		}
		_ = json.Unmarshal(params, &payload)
		r.writer.StepFinish(r.turnID)
		if payload.Turn.Status == "failed" {
			message := "Codex turn failed"
			if payload.Turn.Error != nil && payload.Turn.Error.Message != "" {
				message = payload.Turn.Error.Message
			}
			r.writer.Error(message)
		} else {
			r.writer.Finish(agui.FinishReasonStop)
		}
		r.writer.Custom(method, rawPayload(params))
		r.publishLocked()
		r.finalizeLocked(time.Now())
		r.client.Main.setActive(r.threadID, nil)
		return
	default:
		r.writer.Custom(method, rawPayload(params))
	}
	r.publishLocked()
}

func (r *activeRun) rememberProcessTool(processID, toolID string) {
	if r == nil || processID == "" {
		return
	}
	if r.processes == nil {
		r.processes = map[string]string{}
	}
	if toolID != "" {
		r.processes[processID] = toolID
	}
	if r.client != nil && r.client.Main != nil {
		r.client.Main.rememberProcess(processID, r)
	}
}

func (r *activeRun) toolIDForProcess(processID string) string {
	if r == nil || processID == "" {
		return processID
	}
	if toolID := r.processes[processID]; toolID != "" {
		return toolID
	}
	return processID
}

func (r *activeRun) publish() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.publishLocked()
}

func (r *activeRun) publishLocked() {
	if !r.started || r.nextSeq > len(r.run.Events) {
		return
	}
	copyRun := *r.run
	copyRun.Events = append([]agui.Event(nil), r.run.Events[r.nextSeq-1:]...)
	carriers, err := aistream.PackRunFromSeq(copyRun, r.nextSeq)
	if err != nil {
		return
	}
	for _, carrier := range carriers {
		if len(carrier.Envelopes) == 0 {
			continue
		}
		firstSeq := carrier.Envelopes[0].Seq
		if r.publisher == nil || r.roomID == "" || r.anchorMXID == "" {
			return
		}
		if err := r.publisher.Publish(context.Background(), r.roomID, r.anchorMXID, aistream.CarrierContent(copyRun, carrier.Envelopes)); err != nil {
			logFromContext(context.Background()).Err(err).
				Str("thread_id", r.threadID).
				Str("turn_id", r.turnID).
				Int("first_seq", firstSeq).
				Msg("Failed to publish Codex stream carrier")
			return
		}
		for _, env := range carrier.Envelopes {
			if env.Seq >= r.nextSeq {
				r.nextSeq = env.Seq + 1
			}
		}
	}
}

func (r *activeRun) anchorEventID(ctx context.Context, portal *bridgev2.Portal) id.EventID {
	if r.anchorMXID != "" {
		return r.anchorMXID
	}
	if portal == nil || portal.Bridge == nil || portal.Bridge.DB == nil {
		return ""
	}
	part, err := portal.Bridge.DB.Message.GetPartByID(ctx, portal.Receiver, r.messageID, partID("text"))
	if err != nil || part == nil {
		return ""
	}
	r.anchorMXID = part.MXID
	return r.anchorMXID
}

func (r *activeRun) finalize(ts time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.finalizeLocked(ts)
}

func (r *activeRun) finalizeLocked(ts time.Time) {
	r.client.queueCodexTyping(r.portalKey, 0)
	edit := aibridgev2.FinalMetadataEdit(r.portalKey, codexUserID, r.messageID, *r.run, ts)
	edit.ConvertEditFunc = func(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, existing []*database.Message, data *aistream.Run) (*bridgev2.ConvertedEdit, error) {
		if len(existing) == 0 {
			return nil, nil
		}
		content, extra, err := matrixFinalContentWithAttachment(ctx, portal, intent, *data)
		if err != nil {
			return nil, err
		}
		applyFinalMessageMetadata(existing[0], *data, r.threadID, r.turnID)
		return &bridgev2.ConvertedEdit{
			ModifiedParts: []*bridgev2.ConvertedEditPart{{
				Part:    existing[0],
				Type:    event.EventMessage,
				Content: content,
				Extra:   extra,
				TopLevelExtra: map[string]any{
					"com.beeper.dont_render_edited": true,
				},
			}},
		}, nil
	}
	res := r.client.UserLogin.QueueRemoteEvent(edit)
	if !res.Success {
		logCodexQueueFailure(context.Background(), res, "Failed to queue Codex final stream edit", map[string]any{
			"thread_id":  r.threadID,
			"turn_id":    r.turnID,
			"message_id": string(r.messageID),
		})
	}
	if r.publisher != nil && r.roomID != "" && r.anchorMXID != "" {
		r.publisher.Unregister(r.roomID, r.anchorMXID)
	}
}

func applyFinalMessageMetadata(message *database.Message, run aistream.Run, threadID, turnID string) {
	if message == nil {
		return
	}
	meta := &MessageMetadata{
		Role:         "assistant",
		ThreadID:     threadID,
		TurnID:       turnID,
		StreamStatus: finalStreamStatus(run),
	}
	if existing, ok := message.Metadata.(*MessageMetadata); ok && existing != nil {
		existing.CopyFrom(meta)
		message.Metadata = existing
		return
	}
	message.Metadata = meta
}

func finalStreamStatus(run aistream.Run) string {
	switch run.Status.State {
	case "error", "aborted", "interrupted":
		return run.Status.State
	default:
		return "complete"
	}
}

func logCodexQueueFailure(ctx context.Context, res bridgev2.EventHandlingResult, msg string, fields map[string]any) {
	log := logFromContext(ctx)
	evt := log.Error()
	if res.Error != nil {
		evt = evt.Err(res.Error)
	}
	for key, value := range fields {
		evt = evt.Interface(key, value)
	}
	evt.Bool("ignored", res.Ignored).Bool("queued", res.Queued).Msg(msg)
}

type codexItem struct {
	ID   string
	Type string
	Raw  map[string]any
}

func (i codexItem) IsToolLike() bool {
	switch i.Type {
	case "commandExecution", "fileChange", "mcpToolCall", "dynamicToolCall", "collabAgentToolCall", "webSearch", "imageView", "imageGeneration":
		return true
	default:
		return false
	}
}

func (i codexItem) Name() string {
	switch i.Type {
	case "commandExecution":
		if command, _ := i.Raw["command"].(string); command != "" {
			return "command: " + command
		}
	case "fileChange":
		return "file change"
	case "mcpToolCall":
		server, _ := i.Raw["server"].(string)
		tool, _ := i.Raw["tool"].(string)
		return strings.Trim(strings.TrimSpace(server+" "+tool), " ")
	case "dynamicToolCall":
		tool, _ := i.Raw["tool"].(string)
		if tool != "" {
			return tool
		}
	}
	if i.Type != "" {
		return i.Type
	}
	return "codex item"
}

func notificationItem(params json.RawMessage) codexItem {
	var payload struct {
		Item map[string]any `json:"item"`
	}
	_ = json.Unmarshal(params, &payload)
	idValue, _ := payload.Item["id"].(string)
	typeValue, _ := payload.Item["type"].(string)
	return codexItem{ID: idValue, Type: typeValue, Raw: payload.Item}
}

func (r *activeRun) recoverCompletedAgentMessage(item codexItem) {
	text, _ := item.Raw["text"].(string)
	if strings.TrimSpace(text) == "" || r.agentText[item.ID] != "" {
		return
	}
	r.agentText[item.ID] = text
	r.writer.Text(text)
}

func (r *activeRun) recoverCompletedReasoning(item codexItem) {
	for index, text := range stringList(item.Raw["summary"]) {
		r.recoverCompletedReasoningText(item.ID, "summary", index, text)
	}
	for index, text := range stringList(item.Raw["content"]) {
		r.recoverCompletedReasoningText(item.ID, "content", index, text)
	}
}

func (r *activeRun) recoverCompletedReasoningText(itemID string, kind string, index int, text string) {
	if strings.TrimSpace(text) == "" {
		return
	}
	key := reasoningContentKey(itemID, kind, &index)
	if r.reasoning[key] != "" {
		return
	}
	r.reasoning[key] = text
	r.writer.ReasoningDelta(reasoningSectionIndex(itemID, &index), text)
}

func (r *activeRun) mapRawResponseItem(payload map[string]any) {
	item, _ := payload["item"].(map[string]any)
	if item == nil {
		return
	}
	switch itemType, _ := item["type"].(string); itemType {
	case "message":
		if role, _ := item["role"].(string); role == agui.RoleAssistant {
			r.recoverRawAssistantMessage(item)
		}
	case "reasoning":
		r.recoverRawReasoning(item)
	case "function_call", "custom_tool_call", "tool_search_call":
		r.mapRawToolCall(item)
	case "function_call_output", "custom_tool_call_output", "tool_search_output":
		r.mapRawToolResult(item)
	case "local_shell_call", "web_search_call", "image_generation_call":
		r.mapRawToolEnd(item)
	case "context_compaction", "compaction":
		r.writer.Text(codexCompactionNotice)
	}
}

func (r *activeRun) recoverRawAssistantMessage(item map[string]any) {
	text := strings.TrimSpace(rawContentText(item["content"]))
	if text == "" {
		return
	}
	itemID, _ := item["id"].(string)
	if itemID != "" {
		if r.agentText[itemID] != "" {
			return
		}
		r.agentText[itemID] = text
		r.writer.Text(text)
		return
	}
	if !r.writer.HasTextContent() {
		r.writer.Text(text)
	}
}

func (r *activeRun) recoverRawReasoning(item map[string]any) {
	if len(r.reasoning) > 0 {
		return
	}
	index := 0
	for _, text := range rawTextItems(item["summary"]) {
		r.recoverCompletedReasoningText("raw-response", "summary", index, text)
		index++
	}
	for _, text := range rawTextItems(item["content"]) {
		r.recoverCompletedReasoningText("raw-response", "content", index, text)
		index++
	}
}

func (r *activeRun) mapRawToolCall(item map[string]any) {
	callID := firstString(item, "call_id", "id")
	if callID == "" {
		return
	}
	name := rawToolName(item)
	r.writer.ToolStartWithMetadata(callID, name, 0, nil, map[string]any{"codexRawResponseItem": item})
	args := rawToolInput(item)
	if argsText := rawToolInputText(args); argsText != "" {
		r.writer.ToolArgs(callID, argsText, args)
	}
	r.writer.ToolInputComplete(callID, name, args)
}

func (r *activeRun) mapRawToolResult(item map[string]any) {
	callID := firstString(item, "call_id", "id")
	if callID == "" {
		return
	}
	r.writer.ToolResult(callID, rawToolOutputText(item["output"]), agui.ToolResultStateComplete)
}

func (r *activeRun) mapRawToolEnd(item map[string]any) {
	callID := firstString(item, "call_id", "id")
	if callID == "" {
		return
	}
	name := rawToolName(item)
	state := agui.ToolResultStateComplete
	if toolStateFromStatus(firstString(item, "status")) == agui.ToolResultStateError {
		state = agui.ToolResultStateError
	}
	r.writer.ToolStartWithMetadata(callID, name, 0, nil, map[string]any{"codexRawResponseItem": item})
	r.writer.ToolEnd(callID, name, rawToolInput(item), map[string]any{"state": state, "status": firstString(item, "status")})
}

func rawPayload(params json.RawMessage) map[string]any {
	var out map[string]any
	if err := json.Unmarshal(params, &out); err != nil {
		return map[string]any{"raw": string(params)}
	}
	return out
}

func outputDelta(params json.RawMessage) (string, string) {
	payload := rawPayload(params)
	itemID := ""
	for _, key := range []string{"itemId", "callId", "processId", "processHandle", "id"} {
		if value, _ := payload[key].(string); value != "" {
			itemID = value
			break
		}
	}
	if value, _ := payload["deltaBase64"].(string); value != "" {
		decoded, err := base64.StdEncoding.DecodeString(value)
		if err == nil {
			return itemID, string(decoded)
		}
		return itemID, value
	}
	for _, key := range []string{"delta", "output", "text"} {
		if value, _ := payload[key].(string); value != "" {
			return itemID, value
		}
	}
	return itemID, ""
}

func processExitResult(payload map[string]any) (string, string, string) {
	itemID := firstString(payload, "processHandle", "processId", "itemId", "id")
	var parts []string
	if stdout, _ := payload["stdout"].(string); strings.TrimSpace(stdout) != "" {
		parts = append(parts, strings.TrimSpace(stdout))
	}
	if stderr, _ := payload["stderr"].(string); strings.TrimSpace(stderr) != "" {
		parts = append(parts, strings.TrimSpace(stderr))
	}
	if truthy(payload["stdoutCapReached"]) {
		parts = append(parts, "[stdout truncated]")
	}
	if truthy(payload["stderrCapReached"]) {
		parts = append(parts, "[stderr truncated]")
	}
	exitCode, _ := numberFromAny(payload["exitCode"])
	state := agui.ToolResultStateComplete
	if exitCode != 0 {
		state = agui.ToolResultStateError
		if len(parts) == 0 {
			parts = append(parts, fmt.Sprintf("Process exited with code %d.", exitCode))
		}
	}
	return itemID, strings.TrimSpace(strings.Join(parts, "\n")), state
}

func truthy(value any) bool {
	v, _ := value.(bool)
	return v
}

func reasoningIndex(itemID string) int {
	if itemID == "" {
		return 0
	}
	sum := 0
	for _, ch := range itemID {
		sum += int(ch)
	}
	return sum % 32
}

func reasoningSectionIndex(itemID string, summaryIndex *int) int {
	index := reasoningIndex(itemID)
	if summaryIndex != nil {
		index = (index + (*summaryIndex+1)*32) % 1024
	}
	return index
}

func firstIndex(indexes ...*int) *int {
	for _, index := range indexes {
		if index != nil {
			return index
		}
	}
	return nil
}

func reasoningKind(summaryIndex *int) string {
	if summaryIndex != nil {
		return "summary"
	}
	return "content"
}

func reasoningContentKey(itemID string, kind string, index *int) string {
	if index == nil {
		return itemID + ":" + kind + ":"
	}
	return fmt.Sprintf("%s:%s:%d", itemID, kind, *index)
}

func stringList(value any) []string {
	switch list := value.(type) {
	case []string:
		return list
	case []any:
		out := make([]string, 0, len(list))
		for _, item := range list {
			if text, _ := item.(string); text != "" {
				out = append(out, text)
			}
		}
		return out
	default:
		return nil
	}
}

func rawContentText(value any) string {
	return strings.Join(rawTextItems(value), "")
}

func rawTextItems(value any) []string {
	items, _ := value.([]any)
	out := make([]string, 0, len(items))
	for _, rawItem := range items {
		item, _ := rawItem.(map[string]any)
		if item == nil {
			continue
		}
		if text, _ := item["text"].(string); strings.TrimSpace(text) != "" {
			out = append(out, text)
		}
	}
	return out
}

func rawToolName(item map[string]any) string {
	if name := firstString(item, "name", "execution"); name != "" {
		return name
	}
	if itemType, _ := item["type"].(string); itemType != "" {
		return itemType
	}
	return "codex tool"
}

func rawToolInput(item map[string]any) any {
	if value, ok := item["arguments"]; ok {
		return value
	}
	if value, ok := item["input"]; ok {
		return value
	}
	if value, ok := item["action"]; ok {
		return value
	}
	if value, ok := item["tools"]; ok {
		return value
	}
	return item
}

func rawToolInputText(value any) string {
	if text, ok := value.(string); ok {
		return text
	}
	return compactJSONString(value)
}

func rawToolOutputText(value any) string {
	if text, ok := value.(string); ok {
		return text
	}
	if text := rawContentText(value); text != "" {
		return text
	}
	return compactJSONString(value)
}

func compactJSONString(value any) string {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprint(value)
	}
	return string(data)
}

func approvalReviewText(payload map[string]any) string {
	if review, _ := payload["review"].(map[string]any); review != nil {
		if status, _ := review["status"].(string); status != "" {
			if rationale, _ := review["rationale"].(string); rationale != "" {
				return "approval review " + status + ": " + rationale
			}
			return "approval review " + status
		}
	}
	return "approval review"
}

func patchUpdateText(payload map[string]any) string {
	changes, _ := payload["changes"].([]any)
	if len(changes) == 0 {
		return ""
	}
	var parts []string
	for _, rawChange := range changes {
		change, _ := rawChange.(map[string]any)
		if change == nil {
			continue
		}
		diff, _ := change["diff"].(string)
		diff = strings.TrimSpace(diff)
		if diff == "" {
			continue
		}
		path, _ := change["path"].(string)
		kind, _ := change["kind"].(string)
		header := strings.TrimSpace(strings.TrimSpace(kind + " " + path))
		if header != "" {
			parts = append(parts, header+"\n"+diff)
		} else {
			parts = append(parts, diff)
		}
	}
	return strings.Join(parts, "\n\n")
}

func planDeltaActivity(messageID string, payload map[string]any) agui.Event {
	delta, _ := payload["delta"].(string)
	if strings.TrimSpace(delta) == "" {
		return agui.Event{}
	}
	content := map[string]any{
		"status": "streaming",
		"delta":  delta,
	}
	if itemID, _ := payload["itemId"].(string); itemID != "" {
		content["itemId"] = itemID
	}
	return activitySnapshot(messageID, "codex_plan", content, false)
}

func planSnapshotActivity(messageID string, payload map[string]any) agui.Event {
	content := map[string]any{"status": "updated"}
	if explanation, ok := payload["explanation"].(string); ok {
		content["explanation"] = explanation
	}
	if plan, ok := payload["plan"].([]any); ok {
		content["plan"] = plan
	}
	return activitySnapshot(messageID, "codex_plan", content, true)
}

func planItemActivity(messageID string, item map[string]any) agui.Event {
	text, _ := item["text"].(string)
	if strings.TrimSpace(text) == "" {
		return agui.Event{}
	}
	return planSnapshotActivity(messageID, map[string]any{
		"explanation": text,
		"plan":        []any{item},
	})
}

func diffSnapshotActivity(messageID string, payload map[string]any) agui.Event {
	diff, _ := payload["diff"].(string)
	if strings.TrimSpace(diff) == "" {
		return agui.Event{}
	}
	return activitySnapshot(messageID, "codex_diff", map[string]any{
		"status": "updated",
		"diff":   diff,
	}, true)
}

func activitySnapshot(messageID, activityType string, content map[string]any, replace bool) agui.Event {
	return agui.NewEvent(map[string]any{
		"type":         agui.EventActivitySnapshot,
		"messageId":    messageID,
		"activityType": activityType,
		"content":      content,
		"replace":      replace,
	})
}

func liveReviewModeText(item codexItem) string {
	text := codexEnteredReviewNotice
	if item.Type == "exitedReviewMode" {
		text = codexExitedReviewNotice
	}
	if review, _ := item.Raw["review"].(string); strings.TrimSpace(review) != "" {
		text += "\n\n" + strings.TrimSpace(review)
	}
	return text
}

func hookRunIdentity(payload map[string]any) (string, string) {
	run, _ := payload["run"].(map[string]any)
	if run == nil {
		return "", "hook"
	}
	id, _ := run["id"].(string)
	eventName, _ := run["eventName"].(string)
	if eventName == "" {
		eventName = "hook"
	}
	return id, "hook: " + eventName
}

func hookRunStatus(payload map[string]any) string {
	run, _ := payload["run"].(map[string]any)
	if run == nil {
		return ""
	}
	status, _ := run["status"].(string)
	return status
}

func toolStateFromStatus(status string) string {
	switch strings.ToLower(status) {
	case "failed", "error", "errored", "declined", "cancelled", "canceled", "timedout", "timed_out":
		return agui.ToolResultStateError
	case "inprogress", "in_progress", "running", "pending":
		return agui.ToolResultStateStreaming
	default:
		return agui.ToolResultStateComplete
	}
}

func codexItemToolState(data map[string]any) string {
	if success, ok := data["success"].(bool); ok && !success {
		return agui.ToolResultStateError
	}
	if exitCode, ok := numberFromAny(data["exitCode"]); ok && exitCode != 0 {
		return agui.ToolResultStateError
	}
	if status, _ := data["status"].(string); status != "" {
		return toolStateFromStatus(status)
	}
	if hasNonEmptyValue(data["error"]) {
		return agui.ToolResultStateError
	}
	return agui.ToolResultStateComplete
}

func codexItemStatusText(data map[string]any, state string) string {
	if status, _ := data["status"].(string); status != "" {
		return status
	}
	if state == agui.ToolResultStateError {
		return "failed"
	}
	if state == agui.ToolResultStateStreaming {
		return "inProgress"
	}
	return "success"
}

func numberFromAny(value any) (int, bool) {
	switch v := value.(type) {
	case int:
		return v, true
	case int64:
		return int(v), true
	case float64:
		return int(v), true
	case json.Number:
		i, err := v.Int64()
		return int(i), err == nil
	default:
		return 0, false
	}
}

func hasNonEmptyValue(value any) bool {
	switch v := value.(type) {
	case nil:
		return false
	case string:
		return strings.TrimSpace(v) != ""
	case []any:
		return len(v) > 0
	case map[string]any:
		return len(v) > 0
	default:
		return true
	}
}

func matrixFinalContent(run aistream.Run) (*event.MessageEventContent, map[string]any) {
	return aimatrix.FinalContent(run)
}

func matrixFinalContentWithAttachment(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, run aistream.Run) (*event.MessageEventContent, map[string]any, error) {
	projection := aimatrix.ProjectFinal(run, nil)
	if projection.NeedsAttachment {
		partsRef, err := uploadFinalPartsRef(ctx, portal, intent, run, projection.Message)
		if err != nil {
			return nil, nil, err
		}
		projection = aimatrix.ProjectFinal(run, partsRef)
	}
	return projection.Content, projection.Extra, nil
}

func uploadFinalPartsRef(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, run aistream.Run, message aistream.UIMessage) (*aistream.FinalPartsRef, error) {
	if portal == nil || portal.Portal == nil || portal.MXID == "" {
		return nil, fmt.Errorf("missing portal for AI final parts upload")
	}
	if intent == nil {
		return nil, fmt.Errorf("missing Matrix API for AI final parts upload")
	}
	payload, err := json.Marshal(run.FinalPartsPayload(message))
	if err != nil {
		return nil, fmt.Errorf("failed to encode AI final parts: %w", err)
	}
	hash := sha256.Sum256(payload)
	url, file, err := intent.UploadMedia(ctx, portal.MXID, payload, fmt.Sprintf("ai-final-parts-%s.json", run.RunID), aistream.FinalPartsMediaType)
	if err != nil {
		return nil, fmt.Errorf("failed to upload AI final parts: %w", err)
	}
	ref := &aistream.FinalPartsRef{
		Schema:     aistream.FinalPartsRefSchema,
		MediaType:  aistream.FinalPartsMediaType,
		ByteSize:   len(payload),
		SHA256:     base64.RawURLEncoding.EncodeToString(hash[:]),
		PartsCount: len(message.Parts),
	}
	if file != nil {
		ref.File = file
	} else {
		ref.URL = string(url)
	}
	return ref, nil
}
