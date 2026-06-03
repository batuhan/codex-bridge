package bridge

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path/filepath"
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

func (r *activeRun) startAsync(ctx context.Context, source string) {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx = context.WithoutCancel(ctx)
	go func() {
		if err := r.start(ctx); err != nil {
			if r.client != nil && r.client.Main != nil {
				r.client.Main.setActive(r.threadID, nil)
				r.client.Main.queueThreadNotice(r.threadID, "Failed to start Codex stream:\n\n"+err.Error())
			}
			logFromContext(ctx).Err(err).
				Str("thread_id", r.threadID).
				Str("turn_id", r.turnID).
				Str("source", source).
				Msg("Failed to start Codex stream")
		}
	}()
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
	logFromContext(ctx).Debug().
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
	logFromContext(ctx).Debug().
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
			logFromContext(ctx).Err(err).
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
		logFromContext(ctx).Debug().
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
		logFromContext(ctx).Debug().
			Str("room_id", string(portal.MXID)).
			Str("event_id", string(r.anchorMXID)).
			Str("thread_id", r.threadID).
			Str("turn_id", r.turnID).
			Msg("Resolved Codex stream anchor event ID from bridge DB")
		return
	}
	logFromContext(ctx).Warn().
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
		r.writer.StateDelta(map[string]any{"codexThread": state})
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
			state := toolStateFromStatus(hookRunStatus(payload))
			if state != agui.ToolResultStateStreaming {
				r.endToolCall(id, name, payload["run"], map[string]any{"state": state, "status": hookRunStatus(payload)})
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
			index := firstIndex(payload.SummaryIndex, payload.ContentIndex)
			key := reasoningContentKey(payload.ItemID, reasoningKind(payload.SummaryIndex), index)
			r.reasoning[key] += payload.Delta
			r.writer.ReasoningDelta(r.reasoningSection(key), payload.Delta)
		}
		r.writeCustom(method, raw)
	case "item/reasoning/summaryPartAdded":
		var payload struct {
			ItemID       string `json:"itemId"`
			SummaryIndex int    `json:"summaryIndex"`
		}
		_ = json.Unmarshal(params, &payload)
		key := reasoningContentKey(payload.ItemID, "summary", &payload.SummaryIndex)
		r.writer.ReasoningMessageStart(r.reasoningSection(key))
		r.writeCustom(method, rawPayload(params))
	case "item/commandExecution/outputDelta", "item/fileChange/outputDelta", "command/exec/outputDelta", "process/outputDelta":
		payload := rawPayload(params)
		if itemID, delta := outputDeltaFromPayload(payload); itemID != "" && delta != "" {
			itemID = r.toolIDForProcess(itemID)
			r.ensureToolStartedWithMetadata(itemID, inferredToolName(method, itemID), codexNotificationToolStartMetadata(method, payload))
			r.writer.ToolResult(itemID, delta, agui.ToolResultStateStreaming)
			r.toolResult[itemID] = true
		}
		r.writeCustom(method, payload)
	case "item/commandExecution/terminalInteraction":
		payload := rawPayload(params)
		itemID, _ := payload["itemId"].(string)
		processID, _ := payload["processId"].(string)
		r.rememberProcessTool(processID, itemID)
		stdin, _ := payload["stdin"].(string)
		if itemID != "" && stdin != "" {
			r.ensureToolStartedWithMetadata(itemID, inferredToolName(method, itemID), codexNotificationToolStartMetadata(method, payload))
			r.writer.ToolResult(itemID, stdin, agui.ToolResultStateStreaming)
			r.toolResult[itemID] = true
		}
		r.writeCustom(method, payload)
	case "item/plan/delta":
		payload := rawPayload(params)
		if activity := planDeltaActivity(r.run.MessageID, payload); activity.Len() > 0 {
			r.writer.Add(activity)
		}
		r.writeCustom(method, payload)
	case "turn/plan/updated":
		payload := rawPayload(params)
		r.writer.Add(planSnapshotActivity(r.run.MessageID, payload))
		r.writeCustom(method, payload)
	case "turn/diff/updated":
		payload := rawPayload(params)
		if activity := diffSnapshotActivity(r.run.MessageID, payload); activity.Len() > 0 {
			r.writer.Add(activity)
		}
		r.writeCustom(method, payload)
	case "process/exited":
		payload := rawPayload(params)
		if itemID, result, state := processExitResult(payload); itemID != "" && result != "" {
			itemID = r.toolIDForProcess(itemID)
			r.ensureToolStartedWithMetadata(itemID, inferredToolName(method, itemID), codexNotificationToolStartMetadata(method, payload))
			r.writer.ToolResult(itemID, result, state)
			r.toolResult[itemID] = true
		}
		r.writeCustom(method, payload)
	case "item/mcpToolCall/progress":
		payload := rawPayload(params)
		itemID, _ := payload["itemId"].(string)
		message, _ := payload["message"].(string)
		if itemID != "" && strings.TrimSpace(message) != "" {
			r.ensureToolStartedWithMetadata(itemID, inferredToolName(method, itemID), codexNotificationToolStartMetadata(method, payload))
			r.writer.ToolResult(itemID, strings.TrimSpace(message), agui.ToolResultStateStreaming)
			r.toolResult[itemID] = true
		}
		r.writeCustom(method, payload)
	case "item/fileChange/patchUpdated":
		payload := rawPayload(params)
		itemID, _ := payload["itemId"].(string)
		if itemID != "" {
			if text := patchUpdateText(payload); text != "" {
				r.ensureToolStartedWithMetadata(itemID, inferredToolName(method, itemID), codexNotificationToolStartMetadata(method, payload))
				r.writer.ToolResult(itemID, text, agui.ToolResultStateStreaming)
				r.toolResult[itemID] = true
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
		if targetID, _ := payload["targetItemId"].(string); targetID != "" {
			state := agui.ToolResultStateStreaming
			if method == "item/autoApprovalReview/completed" {
				state = agui.ToolResultStateComplete
			}
			r.ensureToolStartedWithMetadata(targetID, "approval review", codexNotificationToolStartMetadata(method, payload))
			r.writeToolArgs(targetID, payload)
			r.writer.ToolResult(targetID, approvalReviewText(payload), state)
			r.toolResult[targetID] = true
		}
		r.writeCustom(method, payload)
	case "model/rerouted", "model/verification":
		state := codexThreadState(method, r.threadID, "", params)
		r.applyModelStateLocked(state)
		r.writeActiveNoticeLocked(method, params)
		r.writer.StateDelta(map[string]any{"codexThread": state})
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
		if role, _ := payload["role"].(string); role == agui.RoleAssistant {
			if delta, _ := payload["delta"].(string); delta != "" {
				r.realtimeText += delta
				r.writer.Text(delta)
			}
		}
		r.writeCustom(method, payload)
	case "thread/realtime/transcript/done":
		payload := rawPayload(params)
		if role, _ := payload["role"].(string); role == agui.RoleAssistant {
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
	case "item/started":
		item := notificationItem(params)
		if item.Type == "commandExecution" {
			r.rememberProcessTool(firstString(item.Raw, "processId", "processHandle"), item.ID)
		}
		r.startToolLikeItem(item)
		r.writeCustom(method, rawPayload(params))
	case "item/completed":
		item := notificationItem(params)
		if item.Type == "commandExecution" {
			r.rememberProcessTool(firstString(item.Raw, "processId", "processHandle"), item.ID)
		}
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
		} else if item.Type == "hookPrompt" {
			if text := liveHookPromptText(item.Raw); text != "" {
				r.writer.StateDelta(hookPromptStateDelta(item.ID, text))
			}
		} else if item.ID != "" && item.IsToolLike() {
			toolID := r.toolIDForItem(item.ID)
			r.startToolLikeItem(item)
			state := codexItemToolState(item.Raw)
			if result := completedToolResultText(item.Raw, r.toolResult[toolID]); result != "" {
				r.writer.ToolResult(toolID, result, state)
				r.toolResult[toolID] = true
			}
			if state != agui.ToolResultStateStreaming {
				if result := codexToolCompletionMetadataText(item.Raw, state, r.toolResult[toolID]); result != "" {
					r.writer.ToolResult(toolID, result, state)
					r.toolResult[toolID] = true
				}
				input, _ := codexItemToolInput(item.Raw)
				r.endToolCall(toolID, item.Name(), input, map[string]any{"state": state, "status": codexItemStatusText(item.Raw, state)})
			}
		}
		r.writeCustom(method, rawPayload(params))
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
				if r.client != nil && r.client.Main != nil {
					r.client.Main.setActive(r.threadID, nil)
					r.client.Main.queueThreadNotice(r.threadID, "Failed to start Codex stream:\n\n"+err.Error())
				}
				logFromContext(context.Background()).Err(err).
					Str("thread_id", r.threadID).
					Str("turn_id", r.turnID).
					Str("source", "turn-completed").
					Msg("Failed to start Codex stream")
				return
			}
		}
		message := ""
		if payload.Turn.Error != nil {
			message = payload.Turn.Error.Message
		}
		r.writeCodexRunStateLocked(method, rawPayload(params))
		r.finishTurnLocked(payload.Turn.Status, message)
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

func (r *activeRun) finishTurnLocked(status, message string) {
	finishCodexTurn(r.writer, status, message)
}

func finishCodexTurn(writer *aistream.Writer, status, message string) {
	if writer == nil {
		return
	}
	message = strings.TrimSpace(message)
	if message != "" {
		writer.Error(message)
		return
	}
	statusKind := codexTurnStatusKind(status)
	switch statusKind {
	case "error":
		writer.Error("Codex turn failed")
	case "aborted":
		status = strings.ToLower(strings.TrimSpace(status))
		if status == "" {
			status = "aborted"
		}
		if writer.Run.Text() == "" {
			writer.Text("Codex turn was " + status)
		}
		writer.Abort("Codex turn was " + status)
	case "in_progress":
		if writer.Run.Text() == "" {
			writer.Text("Codex turn is still in progress.")
		}
		writer.Abort("Codex turn is still in progress.")
	default:
		writer.Finish(agui.FinishReasonStop)
	}
}

func codexTurnStatusKind(status string) string {
	normalized := strings.NewReplacer("-", "", "_", "", " ", "").Replace(strings.ToLower(strings.TrimSpace(status)))
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
	state := map[string]any{
		"lastNotification": method,
		method:             payload,
	}
	r.writer.StateDelta(map[string]any{namespace: state})
}

func (r *activeRun) writeCodexClientRequestState(method string, payload map[string]any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.writeCodexClientRequestStateLocked(method, payload)
	r.publishLocked()
}

func (r *activeRun) writeCodexClientRequestStateLocked(method string, payload map[string]any) {
	if r == nil || r.writer == nil || method == "" {
		return
	}
	r.writer.StateDelta(map[string]any{"codexRun": map[string]any{
		"lastClientRequest": method,
		method:              payload,
	}})
}

func (r *activeRun) writeCodexRoomState(eventType string, content map[string]any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.writeCodexRoomStateLocked(eventType, content)
	r.publishLocked()
}

func (r *activeRun) writeCodexRoomStateLocked(eventType string, content map[string]any) {
	if r == nil || r.writer == nil || eventType == "" {
		return
	}
	if content == nil {
		content = map[string]any{}
	}
	r.writer.StateDelta(map[string]any{"codexRoomState": map[string]any{
		"lastEventType": eventType,
		eventType:       content,
	}})
}

func hookPromptStateDelta(itemID, text string) map[string]any {
	payload := map[string]any{"text": text}
	if itemID != "" {
		payload["itemId"] = itemID
	}
	return map[string]any{"codexRun": map[string]any{
		"lastNotification": "item/hookPrompt",
		"item/hookPrompt":  payload,
	}}
}

func activeRunInitialModel(cl *Client, threadID string) string {
	if cl == nil || cl.Main == nil {
		return ""
	}
	room, ok := cl.Main.threadRoom(threadID)
	if !ok {
		return ""
	}
	if room.model != "" {
		return room.model
	}
	if strings.Contains(room.modelProvider, "/") {
		return strings.TrimSpace(room.modelProvider)
	}
	return ""
}

func (r *activeRun) applyModelStateLocked(state map[string]any) {
	if r == nil || r.run == nil {
		return
	}
	if model := codexModelStateRef(state, r.modelProviderHintLocked()); model != "" {
		r.setModelLocked(model)
	}
}

func (r *activeRun) setModelLocked(model string) {
	model = strings.TrimSpace(model)
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
	if r != nil && r.client != nil && r.client.Main != nil {
		if room, ok := r.client.Main.threadRoom(r.threadID); ok && room.modelProvider != "" {
			return room.modelProvider
		}
	}
	if r != nil && r.run != nil {
		if before, _, ok := strings.Cut(r.run.Model, "/"); ok {
			return before
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
	toolID := r.toolIDForItem(item.ID)
	if r.toolStarted[toolID] {
		r.writeToolArgs(toolID, item.Raw)
		return
	}
	r.ensureToolStartedWithMetadata(toolID, item.Name(), codexToolStartMetadata(item.Raw))
	r.writeToolArgs(toolID, item.Raw)
}

func (r *activeRun) ensureToolStarted(toolID, name string) {
	r.ensureToolStartedWithMetadata(toolID, name, nil)
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
	if strings.TrimSpace(name) == "" {
		name = toolID
	}
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
	if prev == text {
		return
	}
	delta := text
	if prev != "" && strings.HasPrefix(text, prev) {
		delta = strings.TrimPrefix(text, prev)
	}
	if delta == "" {
		return
	}
	r.writer.ToolArgs(toolID, delta, input)
	r.toolArgsText[toolID] = text
}

func (r *activeRun) endToolCall(toolID, name string, input, result any) {
	if r == nil || toolID == "" {
		return
	}
	if r.toolEnded == nil {
		r.toolEnded = map[string]bool{}
	}
	if r.toolEnded[toolID] {
		return
	}
	if strings.TrimSpace(name) == "" {
		name = toolID
	}
	if r.toolResult[toolID] {
		r.writer.ToolInputComplete(toolID, name, input)
	} else {
		r.writer.ToolEnd(toolID, name, input, result)
		r.toolResult[toolID] = true
	}
	r.toolEnded[toolID] = true
}

func (r *activeRun) completeToolInput(toolID, name string, input any) {
	if r == nil || toolID == "" {
		return
	}
	if r.toolEnded == nil {
		r.toolEnded = map[string]bool{}
	}
	if r.toolEnded[toolID] {
		return
	}
	if strings.TrimSpace(name) == "" {
		name = toolID
	}
	r.writer.ToolInputComplete(toolID, name, input)
	r.toolEnded[toolID] = true
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

func (r *activeRun) toolIDForItem(itemID string) string {
	if r == nil || itemID == "" {
		return itemID
	}
	if toolID := r.toolAliases[itemID]; toolID != "" {
		return toolID
	}
	return itemID
}

func (r *activeRun) publish() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.publishLocked()
}

func (r *activeRun) publishLocked() {
	if !r.started || r.published >= len(r.run.Events) {
		return
	}
	if r.nextSeq <= 0 {
		r.nextSeq = 1
	}
	copyRun := *r.run
	copyRun.Events = append([]agui.Event(nil), r.run.Events[r.published:]...)
	carriers, err := packStreamCarriersForPublish(copyRun, r.nextSeq)
	if err != nil {
		return
	}
	for _, carrier := range carriers {
		if len(carrier.Envelopes) == 0 {
			continue
		}
		firstSeq := carrier.Envelopes[0].Seq
		lastSeq := carrier.Envelopes[len(carrier.Envelopes)-1].Seq
		if r.publisher == nil || r.roomID == "" || r.anchorMXID == "" {
			return
		}
		if err := r.publisher.Publish(suppressStreamCarrierRequestLogs(context.Background()), r.roomID, r.anchorMXID, aistream.CarrierContent(copyRun, carrier.Envelopes)); err != nil {
			logFromContext(context.Background()).Err(err).
				Str("thread_id", r.threadID).
				Str("turn_id", r.turnID).
				Int("first_seq", firstSeq).
				Msg("Failed to publish Codex stream carrier")
			return
		}
		r.nextSeq = lastSeq + 1
		r.published += len(carrier.Envelopes)
		r.persistLocked(context.Background())
		logFromContext(context.Background()).Debug().
			Str("room_id", string(r.roomID)).
			Str("event_id", string(r.anchorMXID)).
			Str("thread_id", r.threadID).
			Str("turn_id", r.turnID).
			Int("envelope_count", len(carrier.Envelopes)).
			Int("seq_start", firstSeq).
			Msg("Published Codex stream carrier")
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
		candidate := append(append([]aistream.Envelope(nil), current.Envelopes...), env)
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
	if err == nil && streamCarrierContentSize(run, []aistream.Envelope{env}) <= streamCarrierBudgetBytes {
		return event
	}
	limit := streamEventPreviewBytes
	for attempt := 0; attempt < 6; attempt++ {
		compact := compactStreamEventFields(event, limit)
		env, err = aistream.BuildEnvelope(run, seq, compact)
		if err == nil && streamCarrierContentSize(run, []aistream.Envelope{env}) <= streamCarrierBudgetBytes {
			return compact
		}
		limit /= 2
		if limit < 512 {
			limit = 512
		}
	}
	return collapsedStreamEvent(event)
}

func compactStreamEventFields(event agui.Event, stringLimit int) agui.Event {
	fields := event.Map()
	fields["streamTruncated"] = true
	switch event.Type() {
	case agui.EventToolCallResult:
		fields["content"] = streamPayloadTruncatedText
	case agui.EventActivitySnapshot:
		fields["content"] = map[string]any{
			"truncated": true,
			"summary":   streamPayloadTruncatedText,
		}
	case agui.EventMessagesSnapshot:
		fields["messages"] = []any{map[string]any{
			"id":      firstNonEmptyString(event.String("messageId"), event.String("runId"), "stream-truncated"),
			"role":    agui.RoleAssistant,
			"content": streamPayloadTruncatedText,
		}}
	case agui.EventStateSnapshot:
		fields["snapshot"] = map[string]any{
			"truncated": true,
			"summary":   streamPayloadTruncatedText,
		}
	case agui.EventStateDelta:
		fields["delta"] = []any{map[string]any{
			"op":    "replace",
			"path":  "/streamTruncated",
			"value": streamPayloadTruncatedText,
		}}
	case agui.EventRaw:
		fields["event"] = map[string]any{
			"truncated": true,
			"summary":   streamPayloadTruncatedText,
		}
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

func collapsedStreamEvent(event agui.Event) agui.Event {
	fields := map[string]any{
		"type":            agui.EventCustom,
		"name":            "com.beeper.codex.stream_truncated",
		"originalType":    event.Type(),
		"streamTruncated": true,
		"message":         streamPayloadTruncatedText,
	}
	if event.Has("timestamp") {
		fields["timestamp"] = event.Get("timestamp")
	}
	if event.Has("messageId") {
		fields["messageId"] = event.Get("messageId")
	}
	if event.Has("toolCallId") {
		fields["toolCallId"] = event.Get("toolCallId")
	}
	return agui.NewEvent(fields)
}

func compactStreamValue(value any, stringLimit int) any {
	switch typed := value.(type) {
	case string:
		if len(typed) <= stringLimit {
			return typed
		}
		return utf8PrefixBytes(typed, stringLimit) + "\n\n" + streamPayloadTruncatedText
	case map[string]any:
		out := make(map[string]any, len(typed)+1)
		for key, child := range typed {
			out[key] = compactStreamValue(child, max(256, stringLimit/2))
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
			out = append(out, compactStreamValue(child, max(256, stringLimit/2)))
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
	raw, err := json.Marshal(aistream.CarrierContent(run, envelopes))
	if err != nil {
		return streamCarrierBudgetBytes + 1
	}
	return len(raw)
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

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func (r *activeRun) persistLocked(ctx context.Context) {
	if r == nil || r.client == nil || r.client.Main == nil || r.client.Main.Store == nil || r.client.UserLogin == nil || r.run == nil || !r.started || r.anchorMXID == "" || r.roomID == "" {
		return
	}
	modelID := r.run.Model
	providerID := "codex"
	if before, after, ok := strings.Cut(modelID, "/"); ok {
		providerID = before
		modelID = after
	}
	now := time.Now()
	if err := r.client.Main.Store.UpsertActiveStream(ctx, aidb.ActiveStreamRecord{
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
	}); err != nil {
		logFromContext(ctx).Warn().Err(err).
			Str("thread_id", r.threadID).
			Str("turn_id", r.turnID).
			Msg("Failed to persist Codex active stream")
	}
}

func (r *activeRun) deletePersistedLocked(ctx context.Context) {
	if r == nil || r.client == nil || r.client.Main == nil || r.client.Main.Store == nil || r.client.UserLogin == nil || r.turnID == "" {
		return
	}
	if err := r.client.Main.Store.DeleteActiveStream(ctx, r.client.UserLogin.ID, r.turnID); err != nil {
		logFromContext(ctx).Warn().Err(err).
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
		logFromContext(context.Background()).Debug().
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
	logFromContext(context.Background()).Debug().
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
		logFromContext(ctx).Warn().Err(err).Msg("Failed to load persisted Codex active streams")
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
	if activeStreamIdleTimeout <= 0 {
		return time.Minute
	}
	interval := activeStreamIdleTimeout / 5
	if interval <= 0 {
		return activeStreamIdleTimeout
	}
	if interval < time.Second {
		return time.Second
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
		logFromContext(ctx).Warn().Err(err).Msg("Failed to load stale Codex active streams")
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
	run := record.Run
	switch run.Status.State {
	case "complete", "aborted", "error", "interrupted":
	default:
		writer := aistream.NewWriter(&run, time.Now)
		writer.Error(interruptedStreamMessage)
	}
	messageID := record.MessageID
	if messageID == "" {
		messageID = networkid.MessageID(run.MessageID)
	}
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
	if err := cl.Main.Store.DeleteActiveStream(ctx, cl.UserLogin.ID, record.RunID); err != nil {
		logFromContext(ctx).Warn().Err(err).
			Str("thread_id", run.ThreadID).
			Str("turn_id", run.RunID).
			Msg("Failed to delete persisted Codex active stream")
	}
}

func codexFinalStreamEdit(portalKey networkid.PortalKey, messageID networkid.MessageID, run aistream.Run, threadID, turnID string, ts time.Time) *simplevent.Message[*aistream.Run] {
	content, extra := matrixFinalContent(run)
	edit := aibridgev2.FinalMetadataEditWithContent(portalKey, codexUserID, messageID, run, content, extra, ts)
	edit.ConvertEditFunc = func(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, existing []*database.Message, data *aistream.Run) (*bridgev2.ConvertedEdit, error) {
		if len(existing) == 0 {
			return nil, nil
		}
		content, extra, err := matrixFinalContentWithAttachment(ctx, portal, intent, *data)
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
		namespace, _ := i.Raw["namespace"].(string)
		tool, _ := i.Raw["tool"].(string)
		if tool != "" {
			if namespace != "" {
				return namespace + "/" + tool
			}
			return tool
		}
	case "collabAgentToolCall":
		if tool, _ := i.Raw["tool"].(string); strings.TrimSpace(tool) != "" {
			return "collab: " + strings.TrimSpace(tool)
		}
	case "webSearch":
		if query, _ := i.Raw["query"].(string); strings.TrimSpace(query) != "" {
			return "web search: " + strings.TrimSpace(query)
		}
		return "web search"
	case "imageView":
		if path, _ := i.Raw["path"].(string); strings.TrimSpace(path) != "" {
			return "image view: " + filepath.Base(strings.TrimSpace(path))
		}
		return "image view"
	case "imageGeneration":
		return "image generation"
	}
	if i.Type != "" {
		return i.Type
	}
	return "codex item"
}

func codexItemToolInput(data map[string]any) (any, bool) {
	input := map[string]any{}
	for key, value := range data {
		if isCodexItemToolOutputField(key) || !hasNonEmptyValue(value) {
			continue
		}
		input[key] = value
	}
	if len(input) == 0 {
		return nil, false
	}
	if len(input) == 1 {
		for _, value := range input {
			if text, ok := value.(string); ok {
				return strings.TrimSpace(text), true
			}
			return value, true
		}
	}
	return input, true
}

func codexToolStartMetadata(data map[string]any) map[string]any {
	if data == nil {
		return nil
	}
	codex := map[string]any{}
	for _, key := range []string{
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
	} {
		if value, ok := data[key]; ok && hasNonEmptyValue(value) {
			codex[key] = value
		}
	}
	if len(codex) == 0 {
		return nil
	}
	return map[string]any{"codex": codex}
}

func codexNotificationToolStartMetadata(method string, data map[string]any) map[string]any {
	metadata := codexToolStartMetadata(data)
	method = strings.TrimSpace(method)
	if method == "" {
		return metadata
	}
	if metadata == nil {
		return map[string]any{"codex": map[string]any{"notification": method}}
	}
	codex, _ := metadata["codex"].(map[string]any)
	if codex == nil {
		codex = map[string]any{}
		metadata["codex"] = codex
	}
	codex["notification"] = method
	return metadata
}

func isCodexItemToolOutputField(key string) bool {
	switch key {
	case "id",
		"type",
		"status",
		"result",
		"error",
		"contentItems",
		"aggregatedOutput",
		"exitCode",
		"durationMs",
		"success",
		"savedPath":
		return true
	default:
		return false
	}
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
	r.writer.ReasoningDelta(r.reasoningSection(key), text)
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
	case "context_compaction", "compaction", "compaction_trigger":
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
	itemID := firstString(item, "id", "itemId")
	anonymous := itemID == ""
	if anonymous {
		itemID = "raw-response"
	}
	for index, text := range rawTextItems(item["summary"]) {
		if anonymous {
			index = reasoningTextIndex(r.reasoning, itemID, "summary")
		}
		r.recoverCompletedReasoningText(itemID, "summary", index, text)
	}
	for index, text := range rawTextItems(item["content"]) {
		if anonymous {
			index = reasoningTextIndex(r.reasoning, itemID, "content")
		}
		r.recoverCompletedReasoningText(itemID, "content", index, text)
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
	callID := rawToolCallID(item)
	if callID == "" {
		return
	}
	name := rawToolName(item)
	r.ensureToolStartedWithMetadata(callID, name, codexToolStartMetadata(item))
	args := rawToolInput(item)
	r.writeToolArgsText(callID, rawToolInputText(args), args)
	r.completeToolInput(callID, name, args)
}

func (r *activeRun) mapRawToolResult(item map[string]any) {
	callID := rawToolCallID(item)
	if callID == "" {
		return
	}
	r.ensureToolStartedWithMetadata(callID, rawToolName(item), codexToolStartMetadata(item))
	if output := rawToolResultText(item); output != "" {
		r.writer.ToolResult(callID, output, agui.ToolResultStateComplete)
		r.toolResult[callID] = true
	}
}

func (r *activeRun) mapRawToolEnd(item map[string]any) {
	callID := rawToolCallID(item)
	if callID == "" {
		return
	}
	name := rawToolName(item)
	state := toolStateFromStatus(firstString(item, "status"))
	r.ensureToolStartedWithMetadata(callID, name, codexToolStartMetadata(item))
	if output := rawToolResultText(item); output != "" {
		r.writer.ToolResult(callID, output, state)
		r.toolResult[callID] = true
	}
	input := rawToolInput(item)
	r.writeToolArgsText(callID, rawToolInputText(input), input)
	if state == agui.ToolResultStateStreaming {
		return
	}
	r.endToolCall(callID, name, input, map[string]any{"state": state, "status": firstString(item, "status")})
}

func inferredToolName(method, toolID string) string {
	switch {
	case strings.Contains(method, "fileChange"):
		return "file change"
	case strings.Contains(method, "mcpToolCall"):
		return "mcp tool"
	case strings.Contains(method, "autoApprovalReview"):
		return "approval review"
	case strings.Contains(method, "process"):
		return "process: " + toolID
	case strings.Contains(method, "command"):
		return "command: " + toolID
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
	name = strings.TrimSpace(name)
	if name == "" || strings.HasPrefix(name, "codex/") || strings.Contains(name, "/") {
		return false
	}
	return strings.HasPrefix(name, "com.beeper.")
}

func rawPayload(params json.RawMessage) map[string]any {
	var out map[string]any
	if err := json.Unmarshal(params, &out); err != nil {
		return map[string]any{"raw": string(params)}
	}
	return out
}

func outputDelta(params json.RawMessage) (string, string) {
	return outputDeltaFromPayload(rawPayload(params))
}

func outputDeltaFromPayload(payload map[string]any) (string, string) {
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

func (r *activeRun) reasoningSection(key string) int {
	return r.reasoningSections.index(key)
}

type reasoningSectionState struct {
	indexes map[string]int
	next    int
}

func (s *reasoningSectionState) index(key string) int {
	if s == nil {
		return 0
	}
	if key == "" {
		key = "reasoning"
	}
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
	switch typed := value.(type) {
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			if text := rawContentText(item); text != "" {
				out = append(out, text)
			}
		}
		return out
	case map[string]any:
		if text, _ := typed["text"].(string); strings.TrimSpace(text) != "" {
			return []string{text}
		}
		for _, key := range []string{"content", "contentItems"} {
			if text := rawContentText(typed[key]); text != "" {
				return []string{text}
			}
		}
		return nil
	case string:
		if strings.TrimSpace(typed) != "" {
			return []string{typed}
		}
		return nil
	default:
		return nil
	}
}

func liveHookPromptText(item map[string]any) string {
	fragments, _ := item["fragments"].([]any)
	out := make([]string, 0, len(fragments))
	for _, rawFragment := range fragments {
		fragment, _ := rawFragment.(map[string]any)
		if text, _ := fragment["text"].(string); strings.TrimSpace(text) != "" {
			out = append(out, strings.TrimSpace(text))
		}
	}
	if len(out) > 0 {
		return strings.Join(out, "\n\n")
	}
	text, _ := item["text"].(string)
	return strings.TrimSpace(text)
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

func rawToolCallID(item map[string]any) string {
	if id := firstString(item, "call_id", "id"); id != "" {
		return id
	}
	itemType := firstString(item, "type")
	if itemType == "" {
		itemType = "tool"
	}
	data, err := json.Marshal(item)
	if err != nil {
		data = []byte(fmt.Sprint(item))
	}
	sum := sha256.Sum256(data)
	return "raw_" + sanitizeID(itemType) + "_" + hex.EncodeToString(sum[:])[:12]
}

func rawToolInput(item map[string]any) any {
	input := map[string]any{}
	for key, value := range item {
		if isRawToolOutputField(key) || !hasNonEmptyValue(value) {
			continue
		}
		input[key] = value
	}
	if len(input) == 0 {
		return nil
	}
	if len(input) == 1 {
		for _, value := range input {
			if text, ok := value.(string); ok {
				return strings.TrimSpace(text)
			}
			return value
		}
	}
	return input
}

func isRawToolOutputField(key string) bool {
	switch key {
	case "status",
		"output",
		"tools",
		"result",
		"content",
		"contentItems",
		"encrypted_content":
		return true
	default:
		return false
	}
}

func rawToolInputText(value any) string {
	if value == nil {
		return ""
	}
	if text, ok := value.(string); ok {
		return text
	}
	return compactJSONString(value)
}

func rawToolOutputText(value any) string {
	return codexToolResultValueText(value)
}

func rawToolResultText(item map[string]any) string {
	for _, key := range []string{"output", "tools", "result", "content", "contentItems"} {
		if text := rawToolOutputText(item[key]); text != "" {
			return text
		}
	}
	return ""
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
	content, extra := aimatrix.FinalContent(run)
	applyCodexMessageProfile(content)
	return content, extra
}

func matrixFinalContentWithAttachment(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, run aistream.Run) (*event.MessageEventContent, map[string]any, error) {
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
