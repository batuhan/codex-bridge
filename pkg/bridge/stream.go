package bridge

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	agui "github.com/beeper/ai-bridge/pkg/ag-ui"
	aistream "github.com/beeper/ai-bridge/pkg/ai-stream"
	aibridgev2 "github.com/beeper/ai-bridge/pkg/ai-stream/bridgev2"
	aimatrix "github.com/beeper/ai-bridge/pkg/ai-stream/matrix"
	"github.com/beeper/ai-bridge/pkg/aidb"
	"github.com/beeper/ai-bridge/pkg/aiid"
)

const streamAnchorEventIDTimeout = 30 * time.Second
const interruptedStreamMessage = "Codex stream was interrupted before completion"
const streamCarrierBudgetBytes = aistream.FinalMessageBudgetBytes
const streamEventPreviewBytes = 8192
const streamPayloadTruncatedText = "[stream payload truncated; full content is available in final message parts]"
const fileChangeToolName = "file change"
const approvalReviewToolName = "approval review"
const webSearchToolName = "web search"
const imageViewToolName = "image view"
const imageGenerationToolName = "image generation"
const mcpToolName = "mcp tool"
const codexItemName = "codex item"
const codexToolName = "codex tool"
const codexPlanActivityType = "codex_plan"
const codexDiffActivityType = "codex_diff"
const activityStatusStreaming = "streaming"
const activityStatusUpdated = "updated"
const hookToolName = "hook"
const commandToolName = "command"
const processToolName = "process"
const collabToolName = "collab"
const reasoningKindContent = "content"
const reasoningKindSummary = "summary"

var activeStreamIdleTimeout = 5 * time.Minute
var activeStreamUnregisterDelay = 30 * time.Second
var activeStreamRecoveryTimeout = 30 * time.Second

type activeRun struct {
	mu                sync.Mutex
	client            *Client
	portalKey         networkid.PortalKey
	threadID          string
	turnID            string
	messageID         networkid.MessageID
	anchorMXID        id.EventID
	roomID            id.RoomID
	publisher         bridgev2.BeeperStreamPublisher
	run               *aistream.Run
	writer            *aistream.Writer
	pending           map[string]*pendingServerRequest
	processes         map[string]string
	toolAliases       map[string]string
	toolStarted       map[string]bool
	toolArgsText      map[string]string
	toolEnded         map[string]bool
	toolResult        map[string]bool
	subagents         map[string]bool
	agentText         map[string]string
	reasoning         map[string]string
	reasoningSections reasoningSectionState
	realtimeText      string
	published         int
	nextSeq           int
	started           bool
	unregistered      bool
}

func newActiveRun(cl *Client, portalKey networkid.PortalKey, threadID, turnID string) *activeRun {
	now := time.Now()
	run := aistream.NewRun(turnID, threadID, activeRunInitialModel(cl, threadID), "codex", "Codex", now)
	run.Data["capabilities"] = codexAgentCapabilities()
	writer := aistream.NewWriter(run, time.Now)
	writer.Start()
	return &activeRun{
		client:       cl,
		portalKey:    portalKey,
		threadID:     threadID,
		turnID:       turnID,
		messageID:    networkid.MessageID(run.MessageID),
		run:          run,
		writer:       writer,
		pending:      map[string]*pendingServerRequest{},
		processes:    map[string]string{},
		toolAliases:  map[string]string{},
		toolStarted:  map[string]bool{},
		toolArgsText: map[string]string{},
		toolEnded:    map[string]bool{},
		toolResult:   map[string]bool{},
		subagents:    map[string]bool{},
		agentText:    map[string]string{},
		reasoning:    map[string]string{},
		nextSeq:      1,
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
	return r.startLocked(ctx)
}

func (r *activeRun) startLocked(ctx context.Context) error {
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
	msg := r.anchorMessage(descriptor, time.Now())
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
	if err := r.startPublisherLocked(ctx, descriptor); err != nil {
		return err
	}
	r.client.queueCodexTyping(r.portalKey, 30*time.Second)
	r.persistLocked(ctx)
	return nil
}

func (r *activeRun) startAsync(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx = context.WithoutCancel(ctx)
	go func() {
		if err := r.start(ctx); err != nil {
			r.handleStartFailure(ctx, err, "notification")
		}
	}()
}

func (r *activeRun) handleStartFailure(ctx context.Context, err error, source string) {
	if r.client != nil && r.client.Main != nil {
		r.client.Main.setActive(r.threadID, nil)
		r.client.Main.queueThreadNotice(r.threadID, "Failed to start Codex stream:\n\n"+err.Error())
	}
	zerolog.Ctx(ctx).Err(err).
		Str("thread_id", r.threadID).
		Str("turn_id", r.turnID).
		Str("source", source).
		Msg("Failed to start Codex stream")
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
	descriptor, err := publisher.NewDescriptor(ctx, roomID, aiid.StreamType)
	if err != nil {
		return nil, fmt.Errorf("failed to create Codex stream descriptor: %w", err)
	}
	zerolog.Ctx(ctx).Debug().
		Str("room_id", string(roomID)).
		Str("descriptor_user_id", string(descriptor.UserID)).
		Str("descriptor_device_id", string(descriptor.DeviceID)).
		Str("stream_type", descriptor.Type).
		Str("thread_id", r.threadID).
		Str("turn_id", r.turnID).
		Msg("Created Codex stream descriptor")
	r.publisher = publisher
	r.roomID = roomID
	return descriptor, nil
}

func (r *activeRun) startPublisherLocked(ctx context.Context, descriptor *event.BeeperStreamInfo) error {
	if r.anchorMXID == "" {
		return fmt.Errorf("failed to resolve Codex stream anchor event ID")
	}
	if err := r.publisher.Register(ctx, r.roomID, r.anchorMXID, descriptor); err != nil {
		return fmt.Errorf("failed to register Codex stream publisher: %w", err)
	}
	zerolog.Ctx(ctx).Debug().
		Str("room_id", string(r.roomID)).
		Str("event_id", string(r.anchorMXID)).
		Str("descriptor_user_id", string(descriptor.UserID)).
		Str("descriptor_device_id", string(descriptor.DeviceID)).
		Str("stream_type", descriptor.Type).
		Str("thread_id", r.threadID).
		Str("turn_id", r.turnID).
		Msg("Registered Codex stream publisher")
	r.started = true
	r.publishLocked()
	return nil
}

func (r *activeRun) stopPublisherLocked(ctx context.Context) {
	if r.publisher == nil || r.roomID == "" || r.anchorMXID == "" {
		return
	}
	r.publisher.Unregister(r.roomID, r.anchorMXID)
	r.unregistered = true
	r.started = false
	if r.client != nil && r.client.Main != nil && r.client.Main.Store != nil && r.client.UserLogin != nil {
		if err := r.client.Main.Store.DeleteActiveStream(ctx, r.client.UserLogin.ID, r.turnID); err != nil {
			zerolog.Ctx(ctx).Err(err).
				Str("thread_id", r.threadID).
				Str("turn_id", r.turnID).
				Msg("Failed to delete rolled back Codex active stream")
		}
	}
}

func (r *activeRun) anchorMessage(descriptor *event.BeeperStreamInfo, ts time.Time) *simplevent.PreConvertedMessage {
	msg := aibridgev2.Anchor(r.portalKey, codexUserID, *r.run, ts)
	if len(msg.Data.Parts) > 0 {
		msg.Data.Parts[0].ID = partID("text")
		msg.Data.Parts[0].Content.BeeperStream = descriptor
		applyCodexMessageProfile(msg.Data.Parts[0].Content)
		msg.Data.Parts[0].DBMetadata = &MessageMetadata{Role: "assistant", ThreadID: r.threadID, TurnID: r.turnID, StreamStatus: "streaming"}
	}
	return msg
}

func (r *activeRun) resolveAnchorEventIDLocked(ctx context.Context) {
	if r.anchorMXID != "" || r.client == nil || r.client.Main == nil || r.client.Main.Bridge == nil {
		return
	}
	bridge := r.client.Main.Bridge
	portal, err := bridge.GetExistingPortalByKey(ctx, r.portalKey)
	if err != nil || portal == nil {
		return
	}
	if eventID := r.anchorEventID(ctx, portal); eventID != "" {
		zerolog.Ctx(ctx).Debug().
			Str("room_id", string(portal.MXID)).
			Str("event_id", string(eventID)).
			Str("thread_id", r.threadID).
			Str("turn_id", r.turnID).
			Msg("Resolved Codex stream anchor event ID")
		return
	}
	eventID, waitErr := aibridgev2.WaitForMessageEventID(ctx, bridge, portal.Receiver, r.messageID, partID("text"), streamAnchorEventIDTimeout)
	if eventID != "" {
		r.anchorMXID = eventID
		zerolog.Ctx(ctx).Debug().
			Str("room_id", string(portal.MXID)).
			Str("event_id", string(r.anchorMXID)).
			Str("thread_id", r.threadID).
			Str("turn_id", r.turnID).
			Msg("Resolved Codex stream anchor event ID from bridge DB")
		return
	}
	zerolog.Ctx(ctx).Warn().
		Err(waitErr).
		Str("room_id", string(portal.MXID)).
		Str("thread_id", r.threadID).
		Str("turn_id", r.turnID).
		Msg("Failed to resolve Codex stream anchor event ID")
}

func (r *activeRun) handle(method string, params json.RawMessage) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.client.queueCodexTyping(r.portalKey, 30*time.Second)
	switch method {
	case "thread/started", "thread/status/changed", "thread/goal/updated", "thread/goal/cleared", "thread/settings/updated", "thread/tokenUsage/updated", "thread/archived", "thread/unarchived", "thread/closed", "thread/name/updated", "thread/compacted":
		state := codexThreadState(method, r.threadID, "", params)
		r.applyModelStateLocked(state)
		r.writeActiveNoticeLocked(method, params)
		r.writeCodexThreadRoomStateLocked(state)
		r.writeCustom(method, rawPayload(params))
	case "turn/started":
		payload := rawPayload(params)
		r.writeCodexRunStateLocked(method, payload)
		r.writeCustom(method, payload)
	case "hook/started":
		payload := rawPayload(params)
		id, name := hookRunIdentity(payload)
		if id != "" {
			r.writer.ToolStartWithMetadata(id, name, 0, nil, codexToolStartMetadata(payload))
			r.writeToolArgs(id, payload)
		}
		r.writeCustom(method, payload)
	case "hook/completed":
		payload := rawPayload(params)
		id, name := hookRunIdentity(payload)
		if id != "" {
			r.ensureToolStartedWithMetadata(id, name, codexNotificationToolStartMetadata(method, payload))
			r.writeToolArgs(id, payload)
			run, _ := payload["run"].(map[string]any)
			status := firstString(run, "status")
			state := toolStateFromStatus(status)
			if state != agui.ToolResultStateStreaming {
				r.endToolCall(id, name, payload["run"], toolEndResult(state, status))
			}
		}
		r.writeCustom(method, payload)
	case "item/agentMessage/delta":
		raw := rawPayload(params)
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
		r.writeCustom(method, raw)
	case "item/reasoning/summaryTextDelta", "item/reasoning/textDelta":
		raw := rawPayload(params)
		var payload struct {
			Delta        string `json:"delta"`
			ItemID       string `json:"itemId"`
			SummaryIndex *int   `json:"summaryIndex"`
			ContentIndex *int   `json:"contentIndex"`
		}
		_ = json.Unmarshal(params, &payload)
		if payload.Delta != "" {
			key := reasoningDeltaContentKey(payload.ItemID, payload.SummaryIndex, payload.ContentIndex)
			r.reasoning[key] += payload.Delta
			r.writer.ReasoningDelta(r.reasoningSections.index(key), payload.Delta)
		}
		r.writeCustom(method, raw)
	case "item/reasoning/summaryPartAdded":
		var payload struct {
			ItemID       string `json:"itemId"`
			SummaryIndex int    `json:"summaryIndex"`
		}
		_ = json.Unmarshal(params, &payload)
		key := reasoningContentKey(payload.ItemID, reasoningKindSummary, &payload.SummaryIndex)
		r.writer.ReasoningMessageStart(r.reasoningSections.index(key))
		r.writeCustom(method, rawPayload(params))
	case "item/commandExecution/outputDelta", "item/fileChange/outputDelta", "command/exec/outputDelta", "process/outputDelta":
		payload := rawPayload(params)
		if itemID, delta := outputDeltaFromPayload(payload); itemID != "" && delta != "" {
			itemID = r.toolIDForProcess(itemID)
			r.ensureToolStartedWithMetadata(itemID, inferredToolName(method, itemID), codexNotificationToolStartMetadata(method, payload))
			r.writeToolResult(itemID, delta, agui.ToolResultStateStreaming)
		}
		r.writeCustom(method, payload)
	case "item/commandExecution/terminalInteraction":
		payload := rawPayload(params)
		itemID := firstString(payload, "itemId")
		processID := firstString(payload, "processId")
		r.rememberProcessTool(processID, itemID)
		stdin, _ := payload["stdin"].(string)
		if itemID != "" && stdin != "" {
			r.ensureToolStartedWithMetadata(itemID, inferredToolName(method, itemID), codexNotificationToolStartMetadata(method, payload))
			r.writeToolResult(itemID, stdin, agui.ToolResultStateStreaming)
		}
		r.writeCustom(method, payload)
	case "item/plan/delta":
		payload := rawPayload(params)
		delta, _ := payload["delta"].(string)
		if firstTrimmedNonEmpty(delta) != "" {
			content := map[string]any{
				"status": activityStatusStreaming,
				"delta":  delta,
			}
			setNonEmptyMapString(content, "itemId", firstString(payload, "itemId"))
			r.writer.Add(activitySnapshot(r.run.MessageID, codexPlanActivityType, content, false))
		}
		r.writeCustom(method, payload)
	case "turn/plan/updated":
		payload := rawPayload(params)
		content := map[string]any{"status": activityStatusUpdated}
		if explanation, ok := payload["explanation"].(string); ok {
			content["explanation"] = explanation
		}
		if plan, ok := payload["plan"].([]any); ok {
			content["plan"] = plan
		}
		r.writer.Add(activitySnapshot(r.run.MessageID, codexPlanActivityType, content, true))
		r.writeCustom(method, payload)
	case "turn/diff/updated":
		payload := rawPayload(params)
		diff, _ := payload["diff"].(string)
		if firstTrimmedNonEmpty(diff) != "" {
			r.writer.Add(activitySnapshot(r.run.MessageID, codexDiffActivityType, map[string]any{
				"status": activityStatusUpdated,
				"diff":   diff,
			}, true))
		}
		r.writeCustom(method, payload)
	case "process/exited":
		payload := rawPayload(params)
		if itemID, result, state := processExitResult(payload); itemID != "" && result != "" {
			itemID = r.toolIDForProcess(itemID)
			r.ensureToolStartedWithMetadata(itemID, inferredToolName(method, itemID), codexNotificationToolStartMetadata(method, payload))
			r.writeToolResult(itemID, result, state)
		}
		r.writeCustom(method, payload)
	case "item/mcpToolCall/progress":
		payload := rawPayload(params)
		itemID := firstString(payload, "itemId")
		message := firstString(payload, "message")
		if itemID != "" && message != "" {
			r.ensureToolStartedWithMetadata(itemID, inferredToolName(method, itemID), codexNotificationToolStartMetadata(method, payload))
			r.writeToolResult(itemID, message, agui.ToolResultStateStreaming)
		}
		r.writeCustom(method, payload)
	case "item/fileChange/patchUpdated":
		payload := rawPayload(params)
		itemID := firstString(payload, "itemId")
		if itemID != "" {
			if text := patchUpdateText(payload); text != "" {
				r.ensureToolStartedWithMetadata(itemID, inferredToolName(method, itemID), codexNotificationToolStartMetadata(method, payload))
				r.writeToolResult(itemID, text, agui.ToolResultStateStreaming)
			}
		}
		r.writeCustom(method, payload)
	case "serverRequest/resolved":
		payload := rawPayload(params)
		r.writeCodexRunStateLocked(method, payload)
		r.writeCustom(method, payload)
	case "rawResponseItem/completed":
		payload := rawPayload(params)
		r.mapRawResponseItem(payload)
		r.writeCustom(method, payload)
	case "item/autoApprovalReview/started", "item/autoApprovalReview/completed":
		payload := rawPayload(params)
		if targetID := firstString(payload, "targetItemId"); targetID != "" {
			state := agui.ToolResultStateStreaming
			if method == "item/autoApprovalReview/completed" {
				state = agui.ToolResultStateComplete
			}
			r.ensureToolStartedWithMetadata(targetID, approvalReviewToolName, codexNotificationToolStartMetadata(method, payload))
			r.writeToolArgs(targetID, payload)
			review, _ := payload["review"].(map[string]any)
			r.writeToolResult(targetID, approvalReviewResultText(review), state)
		}
		r.writeCustom(method, payload)
	case "model/rerouted", "model/verification":
		state := codexThreadState(method, r.threadID, "", params)
		r.applyModelStateLocked(state)
		r.writeActiveNoticeLocked(method, params)
		r.writeCodexThreadRoomStateLocked(state)
		r.writeCustom(method, rawPayload(params))
	case "warning", "guardianWarning", "deprecationNotice", "configWarning":
		payload := rawPayload(params)
		r.writeCodexRunStateLocked(method, payload)
		r.writeActiveNoticeLocked(method, params)
		r.writeCustom(method, payload)
	case "error":
		payload := rawPayload(params)
		r.writeCodexRunStateLocked(method, payload)
		r.writer.Error(errorNoticeText(payload))
		r.writeCustom(method, payload)
	case "thread/realtime/transcript/delta":
		payload := rawPayload(params)
		if role := firstString(payload, "role"); role == agui.RoleAssistant {
			if delta, _ := payload["delta"].(string); delta != "" {
				r.realtimeText += delta
				r.writer.Text(delta)
			}
		}
		r.writeCustom(method, payload)
	case "thread/realtime/transcript/done":
		payload := rawPayload(params)
		if role := firstString(payload, "role"); role == agui.RoleAssistant {
			if text, _ := payload["text"].(string); text != "" {
				if r.realtimeText == "" {
					r.writer.Text(text)
				} else if strings.HasPrefix(text, r.realtimeText) {
					if suffix := strings.TrimPrefix(text, r.realtimeText); suffix != "" {
						r.writer.Text(suffix)
					}
				}
				r.realtimeText = text
			}
		}
		r.writeCustom(method, payload)
	case "thread/realtime/started", "thread/realtime/itemAdded", "thread/realtime/outputAudio/delta", "thread/realtime/sdp", "thread/realtime/closed":
		payload := rawPayload(params)
		r.writeActiveNoticeLocked(method, params)
		r.writeCodexRealtimeStateLocked(method, payload)
		r.writeCustom(method, payload)
	case "thread/realtime/error":
		payload := rawPayload(params)
		r.writeCodexRealtimeStateLocked(method, payload)
		r.writer.Error(threadNoticeText(method, params))
		r.writeCustom(method, payload)
	case "item/started", "item/completed":
		payload := rawPayload(params)
		itemData, _ := payload["item"].(map[string]any)
		item := codexItem{
			ID:   firstString(itemData, "id"),
			Type: firstString(itemData, "type"),
			Raw:  itemData,
		}
		if item.Type == "commandExecution" {
			r.rememberProcessTool(firstString(item.Raw, "processId", "processHandle"), item.ID)
		}
		if method == "item/started" {
			r.startToolLikeItem(item)
			r.writeCustom(method, payload)
			break
		}
		if item.Type == "contextCompaction" {
			r.writer.Text(codexCompactionNotice)
		} else if item.Type == "enteredReviewMode" || item.Type == "exitedReviewMode" {
			r.writer.Text(reviewModeNoticeText(item.Type, firstString(item.Raw, "review")))
		} else if item.Type == "agentMessage" {
			r.recoverCompletedAgentMessage(item)
		} else if item.Type == "reasoning" {
			r.recoverCompletedReasoning(item)
		} else if item.Type == "plan" {
			text, _ := item.Raw["text"].(string)
			if firstTrimmedNonEmpty(text) != "" {
				r.writer.Add(planItemSnapshotActivity(r.run.MessageID, item.Raw))
			}
		} else if item.Type == "hookPrompt" {
			if text := liveHookPromptText(item.Raw); text != "" {
				r.writer.StateDelta(hookPromptStateDelta(item.ID, text))
			}
		} else if item.ID != "" && item.IsToolLike() {
			toolID := r.toolIDForItem(item.ID)
			r.startToolLikeItem(item)
			state := codexItemToolState(item.Raw)
			if result := codexToolResultText(item.Raw, !r.toolResult[toolID]); result != "" {
				r.writeToolResult(toolID, result, state)
			}
			if state != agui.ToolResultStateStreaming {
				status := codexItemStatusText(item.Raw, state)
				if result := codexToolCompletionMetadataText(item.Raw, state, status, r.toolResult[toolID]); result != "" {
					r.writeToolResult(toolID, result, state)
				}
				input, _ := codexItemToolInput(item.Raw)
				r.endToolCall(toolID, item.Name(), input, toolEndResult(state, status))
			}
		}
		r.writeCustom(method, payload)
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
		if !r.started {
			if err := r.startLocked(context.Background()); err != nil {
				r.handleStartFailure(context.Background(), err, "turn-completed")
				return
			}
		}
		message := ""
		if payload.Turn.Error != nil {
			message = payload.Turn.Error.Message
		}
		r.writeCodexRunStateLocked(method, rawPayload(params))
		finishCodexTurn(r.writer, payload.Turn.Status, message)
		r.writeCustom(method, rawPayload(params))
		r.publishLocked()
		r.finalizeLocked(time.Now())
		r.client.Main.setActive(r.threadID, nil)
		return
	default:
		r.writeCustom(method, rawPayload(params))
	}
	r.publishLocked()
}

func finishCodexTurn(writer *aistream.Writer, status, message string) {
	if writer == nil {
		return
	}
	message = firstTrimmedNonEmpty(message)
	if message != "" {
		writer.Error(message)
		return
	}
	statusKind := codexTurnStatusKind(status)
	switch statusKind {
	case "error":
		writer.Error("Codex turn failed")
	case "aborted":
		status = firstNonEmptyString(lowerTrimmed(status), "aborted")
		abortCodexTurn(writer, "Codex turn was "+status)
	case "in_progress":
		abortCodexTurn(writer, "Codex turn is still in progress.")
	default:
		writer.Finish(agui.FinishReasonStop)
	}
}

func abortCodexTurn(writer *aistream.Writer, message string) {
	if writer.Run.Text() == "" {
		writer.Text(message)
	}
	writer.Abort(message)
}

func codexTurnStatusKind(status string) string {
	normalized := strings.NewReplacer("-", "", "_", "", " ", "").Replace(lowerTrimmed(status))
	switch normalized {
	case "failed", "error", "errored", "timedout":
		return "error"
	case "aborted", "cancelled", "canceled", "interrupted":
		return "aborted"
	case "inprogress", "running", "active":
		return "in_progress"
	default:
		return "complete"
	}
}

func (r *activeRun) writeActiveNoticeLocked(method string, params json.RawMessage) {
	text := threadNoticeText(method, params)
	if text == "" {
		return
	}
	if method == "thread/status/changed" || method == "thread/realtime/error" || method == "error" {
		r.writer.Error(text)
		return
	}
	r.writer.Text(text)
}

func (r *activeRun) writeCodexRunStateLocked(method string, payload map[string]any) {
	r.writeCodexStateLocked("codexRun", method, payload)
}

func (r *activeRun) writeCodexRealtimeStateLocked(method string, payload map[string]any) {
	r.writeCodexStateLocked("codexRealtime", method, payload)
}

func (r *activeRun) writeCodexStateLocked(namespace, method string, payload map[string]any) {
	if r == nil || r.writer == nil || namespace == "" || method == "" {
		return
	}
	r.writer.StateDelta(codexStateDelta(namespace, codexNotificationState(method, payload)))
}

func (r *activeRun) writeCodexClientRequestState(method string, payload map[string]any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r == nil || r.writer == nil || method == "" {
		return
	}
	r.writer.StateDelta(codexStateDelta("codexRun", codexMethodState("lastClientRequest", method, payload)))
	r.publishLocked()
}

func (r *activeRun) writeCodexRoomState(eventType string, content map[string]any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.writeCodexRoomStateLocked(eventType, content)
	r.publishLocked()
}

func (r *activeRun) writeCodexThreadRoomState(state map[string]any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.writeCodexThreadRoomStateLocked(state)
	r.publishLocked()
}

func (r *activeRun) writeCodexThreadRoomStateLocked(state map[string]any) {
	if r == nil || r.writer == nil {
		return
	}
	if state == nil {
		state = map[string]any{}
	}
	state = r.enrichCodexThreadRoomStateLocked(state)
	r.writer.StateDelta(codexStateDelta("codexThread", state))
	for _, roomState := range codexThreadRoomStates(state) {
		r.writeCodexRoomStateLocked(roomState.EventType, roomState.Content)
	}
}

func (r *activeRun) enrichCodexThreadRoomStateLocked(state map[string]any) map[string]any {
	provider := r.modelProviderHintLocked()
	if provider == "" || firstString(state, "modelProvider", "provider") != "" || codexModelStateRef(state, provider) == "" {
		return state
	}
	enriched := copyStateMap(state)
	enriched["modelProvider"] = provider
	return enriched
}

func (r *activeRun) writeCodexRoomStateLocked(eventType string, content map[string]any) {
	if r == nil || r.writer == nil || eventType == "" {
		return
	}
	if content == nil {
		content = map[string]any{}
	}
	r.writer.StateDelta(codexStateDelta("codexRoomState", codexMethodState("lastEventType", eventType, content)))
}

func hookPromptStateDelta(itemID, text string) map[string]any {
	return codexStateDelta("codexRun", codexNotificationState("item/hookPrompt", hookPromptPayload(itemID, text)))
}

func hookPromptPayload(itemID, text string) map[string]any {
	payload := map[string]any{"text": text}
	setNonEmptyMapString(payload, "itemId", itemID)
	return payload
}

func codexStateDelta(namespace string, state map[string]any) map[string]any {
	return map[string]any{namespace: state}
}

func codexNotificationState(method string, payload map[string]any) map[string]any {
	return codexMethodState("lastNotification", method, payload)
}

func codexMethodState(markerKey, method string, payload map[string]any) map[string]any {
	return map[string]any{
		markerKey: method,
		method:    payload,
	}
}

func activeRunInitialModel(cl *Client, threadID string) string {
	if cl == nil || cl.Main == nil {
		return ""
	}
	if room, ok := cl.Main.threadRoom(threadID); ok {
		return firstNonEmptyString(room.model, modelProviderRef(room.modelProvider))
	}
	return ""
}

func (r *activeRun) applyModelStateLocked(state map[string]any) {
	if r == nil || r.run == nil {
		return
	}
	r.setModelLocked(codexModelStateRef(state, r.modelProviderHintLocked()))
}

func (r *activeRun) setModelLocked(model string) {
	model = firstTrimmedNonEmpty(model)
	if r == nil || r.run == nil || model == "" {
		return
	}
	if r.writer != nil {
		r.writer.SetModel(model)
		return
	}
	r.run.Model = model
}

func (r *activeRun) modelProviderHintLocked() string {
	if r == nil {
		return ""
	}
	if r.client != nil && r.client.Main != nil {
		if room, ok := r.client.Main.threadRoom(r.threadID); ok && room.modelProvider != "" {
			return room.modelProvider
		}
	}
	if r.run != nil {
		if provider := modelProviderFromModelRef(r.run.Model); provider != "" {
			return provider
		}
	}
	return ""
}

func (r *activeRun) rememberProcessTool(processID, toolID string) {
	if r == nil || processID == "" {
		return
	}
	if r.processes == nil {
		r.processes = map[string]string{}
	}
	if toolID != "" {
		if existing := r.processes[processID]; existing != "" {
			if existing != toolID {
				if r.toolAliases == nil {
					r.toolAliases = map[string]string{}
				}
				r.toolAliases[toolID] = existing
			}
		} else if r.toolStarted[processID] {
			r.processes[processID] = processID
			if r.toolAliases == nil {
				r.toolAliases = map[string]string{}
			}
			r.toolAliases[toolID] = processID
		} else {
			r.processes[processID] = toolID
		}
	}
	if r.client != nil && r.client.Main != nil {
		r.client.Main.rememberProcess(processID, r)
	}
}

func (r *activeRun) startToolLikeItem(item codexItem) {
	if r == nil || item.ID == "" || !item.IsToolLike() {
		return
	}
	r.queueSubagentResyncs(item.Raw)
	toolID := r.toolIDForItem(item.ID)
	if r.toolStarted[toolID] {
		r.writeToolArgs(toolID, item.Raw)
		return
	}
	r.ensureToolStartedWithMetadata(toolID, item.Name(), codexToolStartMetadata(item.Raw))
	r.writeToolArgs(toolID, item.Raw)
}

func (r *activeRun) queueSubagentResyncs(data map[string]any) {
	if r == nil || r.client == nil {
		return
	}
	if r.subagents == nil {
		r.subagents = map[string]bool{}
	}
	r.client.queueSubagentResyncs(context.Background(), r.threadID, "", subagentRefs(data), r.subagents)
}

func (cl *Client) queueSubagentResyncs(ctx context.Context, parentThreadID, cwd string, refs []subagentRef, seen map[string]bool) {
	if cl == nil || parentThreadID == "" {
		return
	}
	if seen == nil {
		seen = map[string]bool{}
	}
	for _, ref := range refs {
		if seen[ref.ThreadID] {
			continue
		}
		seen[ref.ThreadID] = true
		cl.queueSubagentResync(ctx, parentThreadID, cwd, ref)
	}
}

func (cl *Client) queueSubagentResync(ctx context.Context, parentThreadID, cwd string, ref subagentRef) {
	if ctx == nil {
		ctx = context.Background()
	}
	if cl == nil || cl.UserLogin == nil || cl.UserLogin.Bridge == nil || ref.ThreadID == "" {
		return
	}
	if cwd == "" && cl.Main != nil {
		if room, ok := cl.Main.threadRoom(parentThreadID); ok {
			cwd = room.cwd
		}
	}
	key := subagentPortalKey(ref.ThreadID, cl.UserLogin.ID)
	meta := remoteEventMeta(bridgev2.RemoteEventChatResync, key, codexUserID, time.Now())
	meta.CreatePortal = true
	meta.PreHandleFunc = func(ctx context.Context, portal *bridgev2.Portal) {
		portalMeta := portalMetadata(portal.Metadata)
		if portalMeta.applySubagent(parentThreadID, ref.ThreadID, cwd) {
			portal.Metadata = portalMeta
			if err := portal.Save(ctx); err != nil {
				zerolog.Ctx(ctx).Warn().Err(err).Stringer("portal_key", portal.PortalKey).Msg("Failed to save Codex subagent portal metadata")
			}
		}
	}
	meta.PostHandleFunc = func(ctx context.Context, portal *bridgev2.Portal) {
		if cl.Main != nil && portal != nil {
			cl.Main.rememberThreadRoom(ref.ThreadID, cl, portal.PortalKey, cwd, nil)
		}
	}
	res := cl.UserLogin.QueueRemoteEvent(&simplevent.ChatResync{
		EventMeta:       meta,
		ChatInfo:        subagentPortalInfo(cl.codexMembers(), parentThreadID, ref, cwd, nil),
		LatestMessageTS: time.Now(),
	})
	if !res.Success {
		logCodexQueueFailure(ctx, res, "Failed to queue Codex subagent portal resync", map[string]any{
			"thread_id":        ref.ThreadID,
			"parent_thread_id": parentThreadID,
		})
	}
}

func (r *activeRun) ensureToolStartedWithMetadata(toolID, name string, metadata map[string]any) {
	if r == nil || toolID == "" {
		return
	}
	if r.toolStarted == nil {
		r.toolStarted = map[string]bool{}
	}
	if r.toolStarted[toolID] {
		return
	}
	name = toolNameOrID(name, toolID)
	r.writer.ToolStartWithMetadata(toolID, name, 0, nil, metadata)
	r.toolStarted[toolID] = true
}

func (r *activeRun) writeToolArgs(toolID string, data map[string]any) {
	if r == nil || toolID == "" {
		return
	}
	input, ok := codexItemToolInput(data)
	if !ok {
		return
	}
	r.writeToolArgsText(toolID, rawToolInputText(input), input)
}

func (r *activeRun) writeToolArgsText(toolID, text string, input any) {
	if r == nil || r.writer == nil || toolID == "" || text == "" {
		return
	}
	if r.toolArgsText == nil {
		r.toolArgsText = map[string]string{}
	}
	prev := r.toolArgsText[toolID]
	delta := toolArgsDelta(prev, text)
	if delta == "" {
		return
	}
	r.writer.ToolArgs(toolID, delta, input)
	r.toolArgsText[toolID] = text
}

func toolArgsDelta(previous, current string) string {
	if previous == current {
		return ""
	}
	if previous != "" && strings.HasPrefix(current, previous) {
		return strings.TrimPrefix(current, previous)
	}
	return current
}

func (r *activeRun) endToolCall(toolID, name string, input, result any) {
	if r == nil || toolID == "" {
		return
	}
	if !r.claimToolEnd(toolID) {
		return
	}
	name = toolNameOrID(name, toolID)
	if !r.toolResult[toolID] {
		r.writer.ToolEnd(toolID, name, input, result)
		r.toolResult[toolID] = true
		return
	}
	r.writer.ToolInputComplete(toolID, name, input)
}

func toolEndResult(state, status string) map[string]any {
	return toolStatusResult(state, status)
}

func toolStatusResult(state, status string) map[string]any {
	return map[string]any{"state": state, "status": status}
}

func (r *activeRun) toolIDForProcess(processID string) string {
	if r == nil || processID == "" {
		return processID
	}
	return firstNonEmptyString(r.processes[processID], processID)
}

func (r *activeRun) toolIDForItem(itemID string) string {
	if r == nil || itemID == "" {
		return itemID
	}
	return firstNonEmptyString(r.toolAliases[itemID], itemID)
}

func (r *activeRun) publishLocked() {
	if !r.started || r.published >= len(r.run.Events) {
		return
	}
	if r.nextSeq <= 0 {
		r.nextSeq = 1
	}
	copyRun := *r.run
	copyRun.Events = slices.Clone(r.run.Events[r.published:])
	carriers, err := packStreamCarriersForPublish(copyRun, r.nextSeq)
	if err != nil {
		return
	}
	for _, carrier := range carriers {
		if len(carrier.Envelopes) > 0 {
			firstSeq := carrier.Envelopes[0].Seq
			lastSeq := carrier.Envelopes[len(carrier.Envelopes)-1].Seq
			if r.publisher == nil || r.roomID == "" || r.anchorMXID == "" {
				return
			}
			if err := r.publisher.Publish(suppressStreamCarrierRequestLogs(context.Background()), r.roomID, r.anchorMXID, aistream.CarrierContent(copyRun, carrier.Envelopes)); err != nil {
				zerolog.Ctx(context.Background()).Err(err).
					Str("thread_id", r.threadID).
					Str("turn_id", r.turnID).
					Int("first_seq", firstSeq).
					Msg("Failed to publish Codex stream carrier")
				return
			}
			r.nextSeq = lastSeq + 1
			r.published += len(carrier.Envelopes)
			r.persistLocked(context.Background())
			zerolog.Ctx(context.Background()).Debug().
				Str("room_id", string(r.roomID)).
				Str("event_id", string(r.anchorMXID)).
				Str("thread_id", r.threadID).
				Str("turn_id", r.turnID).
				Int("envelope_count", len(carrier.Envelopes)).
				Int("seq_start", firstSeq).
				Msg("Published Codex stream carrier")
		}
	}
}

func packStreamCarriersForPublish(run aistream.Run, startSeq int) ([]aistream.Carrier, error) {
	if startSeq <= 0 {
		startSeq = 1
	}
	if err := run.Validate(); err != nil {
		return nil, err
	}
	var carriers []aistream.Carrier
	var current aistream.Carrier
	seq := startSeq
	for _, event := range run.Events {
		event = compactStreamEventForPublish(run, event, seq)
		env, err := aistream.BuildEnvelope(run, seq, event)
		if err != nil {
			return nil, err
		}
		candidate := append(slices.Clone(current.Envelopes), env)
		if len(current.Envelopes) > 0 && streamCarrierContentSize(run, candidate) > streamCarrierBudgetBytes {
			carriers = append(carriers, current)
			current = aistream.Carrier{}
		}
		current.Envelopes = append(current.Envelopes, env)
		seq++
	}
	if len(current.Envelopes) > 0 {
		carriers = append(carriers, current)
	}
	return carriers, nil
}

func compactStreamEventForPublish(run aistream.Run, event agui.Event, seq int) agui.Event {
	env, err := aistream.BuildEnvelope(run, seq, event)
	if err == nil && streamEnvelopeFitsCarrier(run, env) {
		return event
	}
	limit := streamEventPreviewBytes
	for attempt := 0; attempt < 6; attempt++ {
		compact := compactStreamEventFields(event, limit)
		env, err = aistream.BuildEnvelope(run, seq, compact)
		if err == nil && streamEnvelopeFitsCarrier(run, env) {
			return compact
		}
		limit /= 2
		if limit < 512 {
			limit = 512
		}
	}
	return collapsedStreamEvent(event)
}

func streamEnvelopeFitsCarrier(run aistream.Run, env aistream.Envelope) bool {
	return streamCarrierContentSize(run, []aistream.Envelope{env}) <= streamCarrierBudgetBytes
}

func compactStreamEventFields(event agui.Event, stringLimit int) agui.Event {
	fields := event.Map()
	fields["streamTruncated"] = true
	switch event.Type() {
	case agui.EventToolCallResult:
		fields["content"] = streamPayloadTruncatedText
	case agui.EventActivitySnapshot:
		fields["content"] = truncatedStreamSummary()
	case agui.EventMessagesSnapshot:
		fields["messages"] = []any{map[string]any{
			"id":      truncatedMessageID(event),
			"role":    agui.RoleAssistant,
			"content": streamPayloadTruncatedText,
		}}
	case agui.EventStateSnapshot:
		fields["snapshot"] = truncatedStreamSummary()
	case agui.EventStateDelta:
		fields["delta"] = []any{map[string]any{
			"op":    "replace",
			"path":  "/streamTruncated",
			"value": streamPayloadTruncatedText,
		}}
	case agui.EventRaw:
		fields["event"] = truncatedStreamSummary()
	default:
		for key, value := range fields {
			if key == "type" || key == "timestamp" {
				continue
			}
			fields[key] = compactStreamValue(value, stringLimit)
		}
	}
	return agui.NewEvent(fields)
}

func truncatedStreamSummary() map[string]any {
	return map[string]any{
		"truncated": true,
		"summary":   streamPayloadTruncatedText,
	}
}

func truncatedMessageID(event agui.Event) string {
	return firstTrimmedNonEmpty(event.String("messageId"), event.String("runId"), "stream-truncated")
}

func collapsedStreamEvent(event agui.Event) agui.Event {
	fields := map[string]any{
		"type":            agui.EventCustom,
		"name":            "com.beeper.codex.stream_truncated",
		"originalType":    event.Type(),
		"streamTruncated": true,
		"message":         streamPayloadTruncatedText,
	}
	copyEventFields(fields, event, "timestamp", "messageId", "toolCallId")
	return agui.NewEvent(fields)
}

func copyEventFields(fields map[string]any, event agui.Event, keys ...string) {
	for _, key := range keys {
		if event.Has(key) {
			fields[key] = event.Get(key)
		}
	}
}

func compactStreamValue(value any, stringLimit int) any {
	childLimit := max(256, stringLimit/2)
	switch typed := value.(type) {
	case string:
		if len(typed) <= stringLimit {
			return typed
		}
		return utf8PrefixBytes(typed, stringLimit) + "\n\n" + streamPayloadTruncatedText
	case map[string]any:
		out := make(map[string]any, len(typed)+1)
		for key, child := range typed {
			out[key] = compactStreamValue(child, childLimit)
		}
		out["truncated"] = true
		return out
	case []any:
		if len(typed) == 0 {
			return typed
		}
		limit := min(len(typed), 4)
		out := make([]any, 0, limit+1)
		for _, child := range typed[:limit] {
			out = append(out, compactStreamValue(child, childLimit))
		}
		if len(typed) > limit {
			out = append(out, map[string]any{
				"truncated": true,
				"remaining": len(typed) - limit,
				"summary":   streamPayloadTruncatedText,
			})
		}
		return out
	default:
		return typed
	}
}

func streamCarrierContentSize(run aistream.Run, envelopes []aistream.Envelope) int {
	if raw, err := json.Marshal(aistream.CarrierContent(run, envelopes)); err == nil {
		return len(raw)
	}
	return streamCarrierBudgetBytes + 1
}

func utf8PrefixBytes(text string, maxBytes int) string {
	if maxBytes >= len(text) {
		return text
	}
	if maxBytes <= 0 {
		return ""
	}
	end := maxBytes
	for end > 0 && !utf8.RuneStart(text[end]) {
		end--
	}
	return text[:end]
}

func (r *activeRun) persistLocked(ctx context.Context) {
	if r == nil || r.client == nil || r.client.Main == nil || r.client.Main.Store == nil || r.client.UserLogin == nil || r.run == nil || !r.started || r.anchorMXID == "" || r.roomID == "" {
		return
	}
	if err := r.client.Main.Store.UpsertActiveStream(ctx, r.activeStreamRecord(time.Now())); err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).
			Str("thread_id", r.threadID).
			Str("turn_id", r.turnID).
			Msg("Failed to persist Codex active stream")
	}
}

func (r *activeRun) activeStreamRecord(now time.Time) aidb.ActiveStreamRecord {
	providerID, modelID := activeStreamModelIDs(r.run.Model)
	return aidb.ActiveStreamRecord{
		RunID:      r.turnID,
		LoginID:    r.client.UserLogin.ID,
		PortalKey:  r.portalKey,
		RoomID:     r.roomID,
		EventID:    r.anchorMXID,
		MessageID:  r.messageID,
		ProviderID: providerID,
		ModelID:    modelID,
		Run:        *r.run,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
}

func activeStreamModelIDs(model string) (providerID, modelID string) {
	providerID = "codex"
	modelID = model
	if before, after, ok := strings.Cut(modelID, "/"); ok {
		providerID = before
		modelID = after
	}
	return providerID, modelID
}

func (r *activeRun) deletePersistedLocked(ctx context.Context) {
	if r == nil || r.client == nil || r.client.Main == nil || r.client.Main.Store == nil || r.client.UserLogin == nil || r.turnID == "" {
		return
	}
	if err := r.client.Main.Store.DeleteActiveStream(ctx, r.client.UserLogin.ID, r.turnID); err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).
			Str("thread_id", r.threadID).
			Str("turn_id", r.turnID).
			Msg("Failed to delete Codex active stream")
	}
}

func suppressStreamCarrierRequestLogs(ctx context.Context) context.Context {
	log := zerolog.Ctx(ctx)
	level := log.GetLevel()
	if level >= zerolog.FatalLevel && level != zerolog.Disabled {
		return ctx
	}
	return log.Level(zerolog.FatalLevel).WithContext(ctx)
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
	edit := codexFinalStreamEdit(r.portalKey, r.messageID, *r.run, r.threadID, r.turnID, ts)
	res := r.client.UserLogin.QueueRemoteEvent(edit)
	if !res.Success {
		logCodexQueueFailure(context.Background(), res, "Failed to queue Codex final stream edit", map[string]any{
			"thread_id":  r.threadID,
			"turn_id":    r.turnID,
			"message_id": string(r.messageID),
		})
		return
	}
	r.schedulePublisherUnregisterLocked()
	r.deletePersistedLocked(context.Background())
}

func (r *activeRun) schedulePublisherUnregisterLocked() {
	if r.unregistered || r.publisher == nil || r.roomID == "" || r.anchorMXID == "" {
		return
	}
	publisher := r.publisher
	roomID := r.roomID
	eventID := r.anchorMXID
	threadID := r.threadID
	turnID := r.turnID
	unregister := func() {
		publisher.Unregister(roomID, eventID)
		zerolog.Ctx(context.Background()).Debug().
			Str("room_id", string(roomID)).
			Str("event_id", string(eventID)).
			Str("thread_id", threadID).
			Str("turn_id", turnID).
			Msg("Unregistered Codex stream publisher")
	}
	r.unregistered = true
	if activeStreamUnregisterDelay <= 0 {
		unregister()
		return
	}
	time.AfterFunc(activeStreamUnregisterDelay, unregister)
	zerolog.Ctx(context.Background()).Debug().
		Str("room_id", string(roomID)).
		Str("event_id", string(eventID)).
		Str("thread_id", threadID).
		Str("turn_id", turnID).
		Dur("delay", activeStreamUnregisterDelay).
		Msg("Scheduled Codex stream publisher unregister")
}

func (cl *Client) failPersistedActiveStreams(ctx context.Context) {
	if cl == nil || cl.Main == nil || cl.Main.Store == nil || cl.UserLogin == nil {
		return
	}
	records, err := cl.Main.Store.ListActiveStreams(ctx, cl.UserLogin.ID)
	if err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).Msg("Failed to load persisted Codex active streams")
		return
	}
	for _, record := range records {
		if cl.hasLiveActiveStream(record) {
			continue
		}
		cl.finishPersistedActiveStream(ctx, record)
	}
}

func (cl *Client) startActiveStreamJanitor() {
	if cl == nil || cl.Main == nil || cl.Main.Store == nil || cl.UserLogin == nil {
		return
	}
	cl.activeStreamJanitorMu.Lock()
	defer cl.activeStreamJanitorMu.Unlock()
	if cl.activeStreamJanitorStop != nil {
		return
	}
	janitorCtx, stop := context.WithCancel(context.Background())
	cl.activeStreamJanitorStop = stop
	go cl.runActiveStreamJanitor(janitorCtx)
}

func (cl *Client) stopActiveStreamJanitor() {
	if cl == nil {
		return
	}
	cl.activeStreamJanitorMu.Lock()
	stop := cl.activeStreamJanitorStop
	cl.activeStreamJanitorStop = nil
	cl.activeStreamJanitorMu.Unlock()
	if stop != nil {
		stop()
	}
}

func (cl *Client) runActiveStreamJanitor(ctx context.Context) {
	cl.failStaleActiveStreams(ctx)
	ticker := time.NewTicker(activeStreamJanitorInterval())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cl.failStaleActiveStreams(ctx)
		}
	}
}

func activeStreamJanitorInterval() time.Duration {
	interval := time.Minute
	if activeStreamIdleTimeout > 0 {
		interval = activeStreamIdleTimeout / 5
		switch {
		case interval <= 0:
			interval = activeStreamIdleTimeout
		case interval < time.Second:
			interval = time.Second
		}
	}
	return interval
}

func (cl *Client) failStaleActiveStreams(ctx context.Context) {
	if cl == nil || cl.Main == nil || cl.Main.Store == nil || cl.UserLogin == nil {
		return
	}
	cutoff := time.Now().Add(-activeStreamIdleTimeout)
	records, err := cl.Main.Store.ListStaleActiveStreams(ctx, cl.UserLogin.ID, cutoff)
	if err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).Msg("Failed to load stale Codex active streams")
		return
	}
	for _, record := range records {
		if cl.hasLiveActiveStream(record) {
			continue
		}
		cl.finishPersistedActiveStream(ctx, record)
	}
}

func (cl *Client) hasLiveActiveStream(record aidb.ActiveStreamRecord) bool {
	if cl == nil || cl.Main == nil || record.RunID == "" || record.Run.ThreadID == "" {
		return false
	}
	active := cl.Main.activeRun(record.Run.ThreadID)
	return active != nil && active.turnID == record.RunID
}

func (cl *Client) finishPersistedActiveStream(ctx context.Context, record aidb.ActiveStreamRecord) {
	if cl == nil || cl.Main == nil || cl.Main.Store == nil || cl.UserLogin == nil {
		return
	}
	if !cl.canFinalizePersistedActiveStream(ctx, record) {
		cl.deletePersistedActiveStreamRecord(ctx, record)
		return
	}
	run := record.Run
	ensurePersistedRunTerminal(&run)
	messageID := persistedStreamMessageID(record, run)
	edit := codexFinalStreamEdit(record.PortalKey, messageID, run, run.ThreadID, run.RunID, time.Now())
	res := cl.UserLogin.QueueRemoteEvent(edit)
	if !res.Success {
		logCodexQueueFailure(ctx, res, "Failed to queue persisted Codex final stream edit", map[string]any{
			"thread_id":  run.ThreadID,
			"turn_id":    run.RunID,
			"message_id": string(messageID),
		})
		return
	}
	cl.deletePersistedActiveStreamRecord(ctx, record)
}

func ensurePersistedRunTerminal(run *aistream.Run) {
	if !persistedRunTerminal(run.Status.State) {
		writer := aistream.NewWriter(run, time.Now)
		writer.Error(interruptedStreamMessage)
	}
}

func persistedRunTerminal(state string) bool {
	switch state {
	case "complete", "aborted", "error", "interrupted":
		return true
	default:
		return false
	}
}

func persistedStreamMessageID(record aidb.ActiveStreamRecord, run aistream.Run) networkid.MessageID {
	return networkid.MessageID(firstNonEmptyString(string(record.MessageID), run.MessageID))
}

func (cl *Client) canFinalizePersistedActiveStream(ctx context.Context, record aidb.ActiveStreamRecord) bool {
	if cl == nil || cl.Main == nil || cl.Main.Bridge == nil || record.PortalKey.ID == "" {
		return false
	}
	portal, err := cl.Main.Bridge.GetExistingPortalByKey(ctx, record.PortalKey)
	if err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).Stringer("portal_key", record.PortalKey).Msg("Failed to load portal for persisted Codex active stream")
		return false
	}
	if portal == nil || portal.MXID == "" {
		return false
	}
	meta := portalMetadata(portal.Metadata)
	return meta.ThreadID != "" && record.Run.ThreadID != "" && meta.ThreadID == record.Run.ThreadID
}

func (cl *Client) deletePersistedActiveStreamRecord(ctx context.Context, record aidb.ActiveStreamRecord) {
	if cl == nil || cl.Main == nil || cl.Main.Store == nil || cl.UserLogin == nil || record.RunID == "" {
		return
	}
	if err := cl.Main.Store.DeleteActiveStream(ctx, cl.UserLogin.ID, record.RunID); err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).
			Str("thread_id", record.Run.ThreadID).
			Str("turn_id", record.RunID).
			Msg("Failed to delete stale persisted Codex active stream")
	}
}

func codexFinalStreamEdit(portalKey networkid.PortalKey, messageID networkid.MessageID, run aistream.Run, threadID, turnID string, ts time.Time) *simplevent.Message[*aistream.Run] {
	content, extra, _ := matrixFinalContent(context.Background(), nil, nil, run)
	edit := aibridgev2.FinalMetadataEditWithContent(portalKey, codexUserID, messageID, run, content, extra, ts)
	edit.ConvertEditFunc = func(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, existing []*database.Message, data *aistream.Run) (*bridgev2.ConvertedEdit, error) {
		if len(existing) == 0 {
			return nil, nil
		}
		content, extra, err := matrixFinalContent(ctx, portal, intent, *data)
		if err != nil {
			return nil, err
		}
		applyFinalMessageMetadata(existing[0], *data, threadID, turnID)
		return &bridgev2.ConvertedEdit{
			ModifiedParts: []*bridgev2.ConvertedEditPart{{
				Part:          existing[0],
				Type:          event.EventMessage,
				Content:       content,
				Extra:         aibridgev2.FinalEditExtra(extra),
				TopLevelExtra: aibridgev2.FinalEditTopLevelExtra(),
			}},
		}, nil
	}
	return edit
}

func applyFinalMessageMetadata(message *database.Message, run aistream.Run, threadID, turnID string) {
	if message == nil {
		return
	}
	ensureMessageMetadata(message).CopyFrom(finalMessageMetadata(run, threadID, turnID))
}

func finalMessageMetadata(run aistream.Run, threadID, turnID string) *MessageMetadata {
	return &MessageMetadata{
		Role:         "assistant",
		ThreadID:     threadID,
		TurnID:       turnID,
		StreamStatus: finalStreamStatus(run),
	}
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
	log := zerolog.Ctx(ctx)
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

type subagentRef struct {
	ThreadID string
	Status   string
}

func (i codexItem) IsToolLike() bool {
	return isCodexToolItemType(i.Type)
}

func isCodexToolItemType(itemType string) bool {
	switch itemType {
	case "commandExecution", "fileChange", "mcpToolCall", "dynamicToolCall", "collabAgentToolCall", "webSearch", "imageView", "imageGeneration":
		return true
	default:
		return false
	}
}

func (i codexItem) Name() string {
	switch i.Type {
	case "commandExecution":
		if command := firstString(i.Raw, "command"); command != "" {
			return colonLabel(commandToolName, command)
		}
	case "fileChange":
		return fileChangeToolName
	case "mcpToolCall":
		return trimmedSpaceJoin(firstString(i.Raw, "server"), firstString(i.Raw, "tool"))
	case "dynamicToolCall":
		if name := dynamicToolName(i.Raw); name != "" {
			return name
		}
	case "collabAgentToolCall":
		if tool := firstString(i.Raw, "tool"); tool != "" {
			return colonLabel(collabToolName, tool)
		}
	case "webSearch":
		return webSearchName(i.Raw)
	case "imageView":
		return imageViewName(i.Raw)
	case "imageGeneration":
		return imageGenerationToolName
	}
	if i.Type != "" {
		return i.Type
	}
	return codexItemName
}

func dynamicToolName(data map[string]any) string {
	namespace := firstString(data, "namespace")
	tool := firstString(data, "tool")
	if tool == "" {
		return ""
	}
	if namespace == "" {
		return tool
	}
	return namespace + "/" + tool
}

func webSearchName(data map[string]any) string {
	if query := firstString(data, "query"); query != "" {
		return colonLabel(webSearchToolName, query)
	}
	return webSearchToolName
}

func imageViewName(data map[string]any) string {
	if path := firstString(data, "path"); path != "" {
		return colonLabel(imageViewToolName, filepath.Base(path))
	}
	return imageViewToolName
}

func codexItemToolInput(data map[string]any) (any, bool) {
	out := compactToolInputFromFields(data, codexItemToolInputExcludedFields()...)
	if out == nil {
		return nil, false
	}
	return out, true
}

func codexItemToolInputExcludedFields() []string {
	return []string{
		"id",
		"type",
		"status",
		"result",
		"error",
		"contentItems",
		"aggregatedOutput",
		"exitCode",
		"durationMs",
		"success",
		"savedPath",
	}
}

func codexToolStartMetadata(data map[string]any) map[string]any {
	if data == nil {
		return nil
	}
	codex := map[string]any{}
	copyNonEmptyMapFields(codex, data, codexToolStartMetadataFields()...)
	if subagents := subagentRefs(data); len(subagents) > 0 {
		codex["subagents"] = subagentMetadataItems(subagents)
	}
	if len(codex) == 0 {
		return nil
	}
	return map[string]any{"codex": codex}
}

func codexToolStartMetadataFields() []string {
	return []string{
		"id",
		"threadId",
		"turnId",
		"itemId",
		"callId",
		"call_id",
		"targetItemId",
		"approvalId",
		"requestId",
		"type",
		"status",
		"name",
		"tool",
		"namespace",
		"server",
		"eventName",
		"execution",
		"source",
		"processId",
		"processHandle",
		"mcpAppResourceUri",
		"pluginId",
		"senderThreadId",
		"receiverThreadIds",
		"model",
		"reasoningEffort",
	}
}

func subagentMetadataItems(refs []subagentRef) []map[string]any {
	items := make([]map[string]any, 0, len(refs))
	for _, ref := range refs {
		items = append(items, subagentMetadataItem(ref))
	}
	return items
}

func subagentMetadataItem(ref subagentRef) map[string]any {
	item := map[string]any{
		"threadId": ref.ThreadID,
		"portalId": string(subagentPortalKey(ref.ThreadID, "").ID),
		"readOnly": true,
	}
	setNonEmptyMapString(item, "status", ref.Status)
	return item
}

func subagentRefs(data map[string]any) []subagentRef {
	if data == nil || firstString(data, "type") != "collabAgentToolCall" {
		return nil
	}
	ids := stringList(data["receiverThreadIds"])
	if len(ids) == 0 {
		return nil
	}
	seen := map[string]bool{}
	states, _ := data["agentsStates"].(map[string]any)
	var refs []subagentRef
	for _, threadID := range ids {
		if seen[threadID] {
			continue
		}
		seen[threadID] = true
		status := ""
		if state, _ := states[threadID].(map[string]any); state != nil {
			status = firstString(state, "status", "state")
		}
		refs = append(refs, subagentRef{
			ThreadID: threadID,
			Status:   status,
		})
	}
	return refs
}

func stringList(value any) []string {
	var out []string
	switch typed := value.(type) {
	case []string:
		out = make([]string, 0, len(typed))
		for _, item := range typed {
			appendStringListItem(&out, item)
		}
	case []any:
		out = make([]string, 0, len(typed))
		for _, item := range typed {
			appendStringListItem(&out, item)
		}
	case string:
		appendStringListItem(&out, typed)
	}
	return out
}

func appendStringListItem(out *[]string, value any) {
	text, ok := value.(string)
	if !ok {
		return
	}
	appendTrimmedNonEmpty(out, text)
}

func codexNotificationToolStartMetadata(method string, data map[string]any) map[string]any {
	metadata := codexToolStartMetadata(data)
	if method := firstTrimmedNonEmpty(method); method != "" {
		if metadata == nil {
			metadata = map[string]any{}
		}
		codex := ensureCodexMetadata(metadata)
		codex["notification"] = method
	}
	return metadata
}

func ensureCodexMetadata(metadata map[string]any) map[string]any {
	codex, _ := metadata["codex"].(map[string]any)
	if codex == nil {
		codex = map[string]any{}
		metadata["codex"] = codex
	}
	return codex
}

func (r *activeRun) recoverCompletedAgentMessage(item codexItem) {
	text, _ := item.Raw["text"].(string)
	if firstTrimmedNonEmpty(text) == "" || r.agentText[item.ID] != "" {
		return
	}
	r.agentText[item.ID] = text
	r.writer.Text(text)
}

func (r *activeRun) recoverCompletedReasoning(item codexItem) {
	for index, text := range stringList(item.Raw[reasoningKindSummary]) {
		r.recoverCompletedReasoningText(item.ID, reasoningKindSummary, index, text)
	}
	for index, text := range stringList(item.Raw[reasoningKindContent]) {
		r.recoverCompletedReasoningText(item.ID, reasoningKindContent, index, text)
	}
}

func (r *activeRun) recoverCompletedReasoningText(itemID string, kind string, index int, text string) {
	if firstTrimmedNonEmpty(text) == "" {
		return
	}
	key := reasoningContentKey(itemID, kind, &index)
	if r.reasoning[key] != "" {
		return
	}
	r.reasoning[key] = text
	r.writer.ReasoningDelta(r.reasoningSections.index(key), text)
}

func (r *activeRun) mapRawResponseItem(payload map[string]any) {
	item, _ := payload["item"].(map[string]any)
	if item == nil {
		return
	}
	switch firstString(item, "type") {
	case "message":
		if firstString(item, "role") == agui.RoleAssistant {
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
	case "context_compaction", "compaction", "compaction_trigger":
		r.writer.Text(codexCompactionNotice)
	}
}

func (r *activeRun) recoverRawAssistantMessage(item map[string]any) {
	text := trimmedRawContentText(item["content"])
	if text == "" {
		return
	}
	itemID := firstString(item, "id")
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
	itemID := firstString(item, "id", "itemId")
	anonymous := itemID == ""
	if anonymous {
		itemID = "raw-response"
	}
	for index, text := range rawTextItems(item[reasoningKindSummary]) {
		if anonymous {
			index = reasoningTextIndex(r.reasoning, itemID, reasoningKindSummary)
		}
		r.recoverCompletedReasoningText(itemID, reasoningKindSummary, index, text)
	}
	for index, text := range rawTextItems(item[reasoningKindContent]) {
		if anonymous {
			index = reasoningTextIndex(r.reasoning, itemID, reasoningKindContent)
		}
		r.recoverCompletedReasoningText(itemID, reasoningKindContent, index, text)
	}
}

func reasoningTextIndex(existing map[string]string, itemID, kind string) int {
	for index := 0; ; index++ {
		if existing[reasoningContentKey(itemID, kind, &index)] == "" {
			return index
		}
	}
}

func (r *activeRun) mapRawToolCall(item map[string]any) {
	callID, name, ok := rawToolIdentity(item)
	if !ok {
		return
	}
	r.ensureToolStartedWithMetadata(callID, name, codexToolStartMetadata(item))
	args := r.writeRawToolInput(callID, item)
	r.completeRawToolInput(callID, name, args)
}

func (r *activeRun) completeRawToolInput(callID, name string, args any) {
	if !r.claimToolEnd(callID) {
		return
	}
	name = toolNameOrID(name, callID)
	r.writer.ToolInputComplete(callID, name, args)
}

func toolNameOrID(name, fallbackID string) string {
	if firstTrimmedNonEmpty(name) == "" {
		return fallbackID
	}
	return name
}

func (r *activeRun) claimToolEnd(toolID string) bool {
	if r == nil || toolID == "" {
		return false
	}
	if r.toolEnded == nil {
		r.toolEnded = map[string]bool{}
	}
	if r.toolEnded[toolID] {
		return false
	}
	r.toolEnded[toolID] = true
	return true
}

func (r *activeRun) mapRawToolResult(item map[string]any) {
	callID, name, ok := rawToolIdentity(item)
	if !ok {
		return
	}
	r.ensureToolStartedWithMetadata(callID, name, codexToolStartMetadata(item))
	r.writeRawToolResult(callID, item, agui.ToolResultStateComplete)
}

func (r *activeRun) mapRawToolEnd(item map[string]any) {
	callID, name, ok := rawToolIdentity(item)
	if !ok {
		return
	}
	status := firstString(item, "status")
	state := toolStateFromStatus(status)
	r.ensureToolStartedWithMetadata(callID, name, codexToolStartMetadata(item))
	r.writeRawToolResult(callID, item, state)
	input := r.writeRawToolInput(callID, item)
	if state == agui.ToolResultStateStreaming {
		return
	}
	r.endToolCall(callID, name, input, toolEndResult(state, status))
}

func rawToolIdentity(item map[string]any) (callID, name string, ok bool) {
	callID = rawToolCallID(item)
	if callID == "" {
		return "", "", false
	}
	return callID, rawToolName(item), true
}

func (r *activeRun) writeRawToolInput(toolID string, item map[string]any) any {
	input := rawToolInput(item)
	r.writeToolArgsText(toolID, rawToolInputText(input), input)
	return input
}

func (r *activeRun) writeRawToolResult(toolID string, item map[string]any, state string) {
	if output := rawToolResultText(item); output != "" {
		r.writeToolResult(toolID, output, state)
	}
}

func (r *activeRun) writeToolResult(toolID, text, state string) {
	r.writer.ToolResult(toolID, text, state)
	r.toolResult[toolID] = true
}

func inferredToolName(method, toolID string) string {
	switch {
	case strings.Contains(method, "fileChange"):
		return fileChangeToolName
	case strings.Contains(method, "mcpToolCall"):
		return mcpToolName
	case strings.Contains(method, "autoApprovalReview"):
		return approvalReviewToolName
	case strings.Contains(method, "process"):
		return colonLabel(processToolName, toolID)
	case strings.Contains(method, "command"):
		return colonLabel(commandToolName, toolID)
	default:
		return toolID
	}
}

func (r *activeRun) writeCustom(name string, value any) {
	if r == nil || r.writer == nil || !shouldBridgeCustomEvent(name) {
		return
	}
	r.writer.Custom(name, value)
}

func shouldBridgeCustomEvent(name string) bool {
	name = firstTrimmedNonEmpty(name)
	return strings.HasPrefix(name, "com.beeper.") && !strings.Contains(name, "/")
}

func rawPayload(params json.RawMessage) map[string]any {
	var out map[string]any
	if err := json.Unmarshal(params, &out); err != nil {
		return map[string]any{"raw": string(params)}
	}
	return out
}

func outputDeltaFromPayload(payload map[string]any) (string, string) {
	itemID := firstString(payload, "itemId", "callId", "processId", "processHandle", "id")
	if value, _ := payload["deltaBase64"].(string); value != "" {
		return itemID, decodedBase64Delta(value)
	}
	if value := firstRawString(payload, "delta", "output", "text"); value != "" {
		return itemID, value
	}
	return itemID, ""
}

func decodedBase64Delta(value string) string {
	decoded, err := base64.StdEncoding.DecodeString(value)
	if err == nil {
		return string(decoded)
	}
	return value
}

func firstRawString(values map[string]any, keys ...string) string {
	for _, key := range keys {
		if value, _ := values[key].(string); value != "" {
			return value
		}
	}
	return ""
}

func trimmedSpaceJoin(parts ...string) string {
	return firstTrimmedNonEmpty(strings.Join(parts, " "))
}

func processExitResult(payload map[string]any) (string, string, string) {
	itemID := firstString(payload, "processHandle", "processId", "itemId", "id")
	parts := processExitOutputParts(payload)
	exitCode, _ := numberFromAny(payload["exitCode"])
	state := processExitState(exitCode)
	appendProcessExitFallback(&parts, exitCode)
	return itemID, strings.Join(parts, "\n"), state
}

func processExitOutputParts(payload map[string]any) []string {
	var parts []string
	appendNonEmptyLine(&parts, firstString(payload, "stdout"))
	appendNonEmptyLine(&parts, firstString(payload, "stderr"))
	appendCapReachedMarker(&parts, payload, "stdoutCapReached", "[stdout truncated]")
	appendCapReachedMarker(&parts, payload, "stderrCapReached", "[stderr truncated]")
	return parts
}

func appendProcessExitFallback(parts *[]string, exitCode int) {
	if exitCode == 0 || len(*parts) != 0 {
		return
	}
	*parts = append(*parts, processExitFallbackMessage(exitCode))
}

func processExitFallbackMessage(exitCode int) string {
	return fmt.Sprintf("Process exited with code %d.", exitCode)
}

func processExitState(exitCode int) string {
	if exitCode != 0 {
		return agui.ToolResultStateError
	}
	return agui.ToolResultStateComplete
}

func appendCapReachedMarker(parts *[]string, payload map[string]any, key, marker string) {
	if firstBool(payload, key) {
		*parts = append(*parts, marker)
	}
}

type reasoningSectionState struct {
	indexes map[string]int
	next    int
}

func (s *reasoningSectionState) index(key string) int {
	if s == nil {
		return 0
	}
	key = firstNonEmptyString(key, "reasoning")
	if s.indexes == nil {
		s.indexes = map[string]int{}
	}
	if index, ok := s.indexes[key]; ok {
		return index
	}
	index := s.next
	s.next++
	s.indexes[key] = index
	return index
}

func reasoningContentKey(itemID string, kind string, index *int) string {
	return itemID + ":" + kind + ":" + reasoningIndexSuffix(index)
}

func reasoningIndexSuffix(index *int) string {
	if index == nil {
		return ""
	}
	return fmt.Sprint(*index)
}

func reasoningDeltaContentKey(itemID string, summaryIndex, contentIndex *int) string {
	if summaryIndex != nil {
		return reasoningContentKey(itemID, reasoningKindSummary, summaryIndex)
	}
	return reasoningContentKey(itemID, reasoningKindContent, contentIndex)
}

func rawContentText(value any) string {
	return strings.Join(rawTextItems(value), "")
}

func trimmedRawContentText(value any) string {
	return firstTrimmedNonEmpty(rawContentText(value))
}

func rawTextItems(value any) []string {
	switch typed := value.(type) {
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			appendNonEmptyLine(&out, rawContentText(item))
		}
		return out
	case map[string]any:
		return rawMapTextItems(typed)
	case string:
		if firstTrimmedNonEmpty(typed) != "" {
			return []string{typed}
		}
		return nil
	default:
		return nil
	}
}

func rawMapTextItems(item map[string]any) []string {
	if text, _ := item["text"].(string); firstTrimmedNonEmpty(text) != "" {
		return []string{text}
	}
	if text := rawNestedContentText(item); text != "" {
		return []string{text}
	}
	return nil
}

func rawNestedContentText(item map[string]any) string {
	for _, key := range []string{"content", "contentItems"} {
		if text := rawContentText(item[key]); text != "" {
			return text
		}
	}
	return ""
}

func liveHookPromptText(item map[string]any) string {
	return firstNonEmptyString(hookPromptFragmentsText(item), firstString(item, "text"))
}

func hookPromptFragmentsText(item map[string]any) string {
	fragments, _ := item["fragments"].([]any)
	out := make([]string, 0, len(fragments))
	for _, rawFragment := range fragments {
		fragment, _ := rawFragment.(map[string]any)
		appendNonEmptyLine(&out, firstString(fragment, "text"))
	}
	return strings.Join(out, "\n\n")
}

func rawToolName(item map[string]any) string {
	return firstNonEmptyString(firstString(item, "name", "execution", "type"), codexToolName)
}

func rawToolCallID(item map[string]any) string {
	if id := firstString(item, "call_id", "id"); id != "" {
		return id
	}
	itemType := firstNonEmptyString(firstString(item, "type"), "tool")
	sum := sha256.Sum256([]byte(compactJSONString(item)))
	return "raw_" + sanitizeID(itemType) + "_" + hex.EncodeToString(sum[:])[:12]
}

func rawToolInput(item map[string]any) any {
	return compactToolInputFromFields(item, rawToolInputExcludedFields()...)
}

func rawToolInputExcludedFields() []string {
	return []string{
		"status",
		"output",
		"tools",
		"result",
		"content",
		"contentItems",
		"encrypted_content",
	}
}

func compactToolInputMap(input map[string]any) any {
	switch len(input) {
	case 0:
		return nil
	case 1:
		for _, value := range input {
			return compactSingleToolInputValue(value)
		}
		return nil
	default:
		return input
	}
}

func compactSingleToolInputValue(value any) any {
	if text, ok := value.(string); ok {
		return firstTrimmedNonEmpty(text)
	}
	return value
}

func compactToolInputFromFields(data map[string]any, excluded ...string) any {
	input := map[string]any{}
	copyNonEmptyMapFieldsExcept(input, data, excluded...)
	return compactToolInputMap(input)
}

func rawToolInputText(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return typed
	default:
		return compactJSONString(value)
	}
}

func rawToolResultText(item map[string]any) string {
	return firstToolResultValueText(item, rawToolResultKeys()...)
}

func rawToolResultKeys() []string {
	return []string{"output", "tools", "result", "content", "contentItems"}
}

func compactJSONString(value any) string {
	if data, err := json.Marshal(value); err == nil {
		return string(data)
	}
	return fmt.Sprint(value)
}

func patchUpdateText(payload map[string]any) string {
	changes, _ := payload["changes"].([]any)
	var parts []string
	for _, rawChange := range changes {
		change, _ := rawChange.(map[string]any)
		if text := patchChangeText(change); text != "" {
			parts = append(parts, text)
		}
	}
	return strings.Join(parts, "\n\n")
}

func patchChangeText(change map[string]any) string {
	if change == nil {
		return ""
	}
	diff := firstString(change, "diff")
	if diff == "" {
		return ""
	}
	header := trimmedSpaceJoin(firstString(change, "kind"), firstString(change, "path"))
	if header == "" {
		return diff
	}
	return header + "\n" + diff
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

func planItemSnapshotActivity(messageID string, data map[string]any) agui.Event {
	content := map[string]any{
		"status": activityStatusUpdated,
		"plan":   []any{data},
	}
	if explanation, ok := data["text"].(string); ok {
		content["explanation"] = explanation
	}
	return activitySnapshot(messageID, codexPlanActivityType, content, true)
}

func approvalReviewResultText(review map[string]any) string {
	status := firstString(review, "status")
	if status == "" {
		return approvalReviewToolName
	}
	text := prefixedLabel(approvalReviewToolName, status)
	rationale := firstString(review, "rationale")
	if rationale == "" {
		return text
	}
	return colonLabel(text, rationale)
}

func hookRunIdentity(payload map[string]any) (string, string) {
	run, _ := payload["run"].(map[string]any)
	if run == nil {
		return "", hookToolName
	}
	return firstString(run, "id"), colonLabel(hookToolName, firstNonEmptyString(firstString(run, "eventName"), hookToolName))
}

func toolStateFromStatus(status string) string {
	switch lowerTrimmed(status) {
	case "failed", "error", "errored", "declined", "cancelled", "canceled", "timedout", "timed_out":
		return agui.ToolResultStateError
	case "inprogress", "in_progress", "running", "pending":
		return agui.ToolResultStateStreaming
	default:
		return agui.ToolResultStateComplete
	}
}

func codexItemToolState(data map[string]any) string {
	if explicitlyFailed(data) {
		return agui.ToolResultStateError
	}
	if hasNonZeroExitCode(data) {
		return agui.ToolResultStateError
	}
	if status := firstString(data, "status"); status != "" {
		return toolStateFromStatus(status)
	}
	if hasNonEmptyValue(data["error"]) {
		return agui.ToolResultStateError
	}
	return agui.ToolResultStateComplete
}

func hasNonZeroExitCode(data map[string]any) bool {
	exitCode, ok := numberFromAny(data["exitCode"])
	return ok && exitCode != 0
}

func codexItemStatusText(data map[string]any, state string) string {
	if status := firstString(data, "status"); status != "" {
		return status
	}
	switch state {
	case agui.ToolResultStateError:
		return "failed"
	case agui.ToolResultStateStreaming:
		return "inProgress"
	default:
		return "success"
	}
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
		if err != nil {
			return 0, false
		}
		return int(i), true
	default:
		return 0, false
	}
}

func hasNonEmptyValue(value any) bool {
	switch v := value.(type) {
	case nil:
		return false
	case string:
		return firstTrimmedNonEmpty(v) != ""
	case []any:
		return len(v) > 0
	case map[string]any:
		return len(v) > 0
	default:
		return true
	}
}

func copyNonEmptyMapFields(dst, src map[string]any, keys ...string) {
	for _, key := range keys {
		value, ok := src[key]
		if !ok || !hasNonEmptyValue(value) {
			continue
		}
		dst[key] = value
	}
}

func copyNonEmptyMapFieldsExcept(dst, src map[string]any, excluded ...string) {
	for key, value := range src {
		if !isIncludedNonEmptyField(key, value, excluded) {
			continue
		}
		dst[key] = value
	}
}

func isIncludedNonEmptyField(key string, value any, excluded []string) bool {
	return !slices.Contains(excluded, key) && hasNonEmptyValue(value)
}

func matrixFinalContent(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, run aistream.Run) (*event.MessageEventContent, map[string]any, error) {
	if portal == nil || portal.MXID == "" || intent == nil {
		content, extra := aimatrix.FinalContent(run)
		applyCodexMessageProfile(content)
		return content, extra, nil
	}
	projection, err := aimatrix.ProjectFinalWithAttachment(ctx, portal, intent, run)
	if err != nil {
		return nil, nil, err
	}
	applyCodexMessageProfile(projection.Content)
	return projection.Content, projection.Extra, nil
}

func applyCodexMessageProfile(content *event.MessageEventContent) {
	if content == nil {
		return
	}
	avatarURL := id.ContentURIString(defaultCodexAvatarMXC)
	content.BeeperPerMessageProfile = &event.BeeperPerMessageProfile{
		ID:          string(codexUserID),
		Displayname: "Codex",
		AvatarURL:   &avatarURL,
		HasFallback: true,
	}
}
