package bridge

import (
	"context"
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"

	agui "github.com/beeper/ai-bridge/pkg/ag-ui"
	aistream "github.com/beeper/ai-bridge/pkg/ai-stream"
	"github.com/beeper/ai-bridge/pkg/msgconv"
	"github.com/beeper/codex-bridge/pkg/appserver"
)

const codexCompactionNotice = "Codex compacted the thread context."
const codexEnteredReviewNotice = "Codex entered review mode."
const codexExitedReviewNotice = "Codex exited review mode."

func (cl *Client) projectBackfillMessages(ctx context.Context, portal *bridgev2.Portal, thread appserver.Thread) ([]*bridgev2.BackfillMessage, error) {
	var messages []*bridgev2.BackfillMessage
	var streamOrder int64
	for _, turn := range sortedBackfillTurns(thread) {
		ts := backfillTurnTime(thread, turn)
		for _, item := range turn.Items {
			if item.Type != "userMessage" {
				continue
			}
			if body := backfillUserBody(item); strings.TrimSpace(body) != "" {
				streamOrder = nextBackfillStreamOrder(streamOrder, ts)
				messages = append(messages, cl.backfillUserMessage(portal.PortalKey, thread.ID, turn.ID, item, body, ts, streamOrder))
				ts = ts.Add(time.Millisecond)
			}
		}
		streamOrder = nextBackfillStreamOrder(streamOrder, ts)
		msg, ok, err := cl.backfillAssistantMessage(ctx, portal, thread, turn, ts, streamOrder)
		if err != nil {
			return nil, err
		}
		if ok {
			messages = append(messages, msg)
		}
	}
	return messages, nil
}

func paginateBackfillMessages(messages []*bridgev2.BackfillMessage, params bridgev2.FetchMessagesParams) *bridgev2.FetchMessagesResponse {
	count := params.Count
	if count <= 0 || count > len(messages) {
		count = len(messages)
	}
	start, end := 0, len(messages)
	if params.Forward {
		start = forwardBackfillStart(messages, params.AnchorMessage)
		if params.AnchorMessage == nil && len(messages)-start > count {
			start = len(messages) - count
		}
		end = min(len(messages), start+count)
		return &bridgev2.FetchMessagesResponse{
			Messages: messages[start:end],
			Forward:  true,
			MarkRead: true,
		}
	}
	end = backwardBackfillEnd(messages, params.AnchorMessage, params.Cursor)
	start = max(0, end-count)
	cursor := networkid.PaginationCursor("")
	if start > 0 {
		cursor = networkid.PaginationCursor(strconv.Itoa(start))
	}
	return &bridgev2.FetchMessagesResponse{
		Messages:             messages[start:end],
		Cursor:               cursor,
		HasMore:              start > 0,
		MarkRead:             true,
		ApproxTotalCount:     len(messages),
		ApproxRemainingCount: start,
	}
}

func forwardBackfillStart(messages []*bridgev2.BackfillMessage, anchor *database.Message) int {
	if anchor == nil {
		return 0
	}
	for i, msg := range messages {
		if msg.ID == anchor.ID {
			return i + 1
		}
	}
	for i, msg := range messages {
		if msg.Timestamp.After(anchor.Timestamp) {
			return i
		}
	}
	return len(messages)
}

func backwardBackfillEnd(messages []*bridgev2.BackfillMessage, anchor *database.Message, cursor networkid.PaginationCursor) int {
	if cursor != "" {
		if end, err := strconv.Atoi(string(cursor)); err == nil {
			return max(0, min(end, len(messages)))
		}
	}
	if anchor == nil {
		return len(messages)
	}
	for i, msg := range messages {
		if msg.ID == anchor.ID {
			return i
		}
	}
	for i, msg := range messages {
		if !msg.Timestamp.Before(anchor.Timestamp) {
			return i
		}
	}
	return len(messages)
}

func sortedBackfillTurns(thread appserver.Thread) []appserver.Turn {
	turns := append([]appserver.Turn(nil), thread.Turns...)
	sort.SliceStable(turns, func(i, j int) bool {
		return backfillTurnTime(thread, turns[i]).Before(backfillTurnTime(thread, turns[j]))
	})
	return turns
}

func (cl *Client) backfillUserMessage(portalKey networkid.PortalKey, threadID, turnID string, item appserver.TurnItem, body string, ts time.Time, streamOrder int64) *bridgev2.BackfillMessage {
	msgID := backfillUserMessageID(item, turnID)
	return &bridgev2.BackfillMessage{
		ConvertedMessage: &bridgev2.ConvertedMessage{Parts: []*bridgev2.ConvertedMessagePart{{
			ID:         partID("text"),
			Type:       event.EventMessage,
			Content:    msgconv.TextContent(body),
			DBMetadata: &MessageMetadata{Role: "user", ThreadID: threadID, TurnID: turnID, StreamStatus: "done"},
		}}},
		Sender:      bridgev2.EventSender{Sender: cl.GetUserID(), IsFromMe: true, SenderLogin: cl.UserLogin.ID},
		ID:          networkid.MessageID(msgID),
		TxnID:       networkid.TransactionID(msgID),
		Timestamp:   ts,
		StreamOrder: streamOrder,
	}
}

func (cl *Client) backfillAssistantMessage(ctx context.Context, portal *bridgev2.Portal, thread appserver.Thread, turn appserver.Turn, ts time.Time, streamOrder int64) (*bridgev2.BackfillMessage, bool, error) {
	run := aistream.NewRun(turn.ID, thread.ID, threadModelRef(thread), "codex", "Codex", ts)
	run.Data["capabilities"] = codexAgentCapabilities()
	writer := aistream.NewWriter(run, func() time.Time { return ts })
	writer.Start()
	writer.StepStart(turn.ID)

	hasContent := false
	for _, item := range turn.Items {
		if item.Type == "userMessage" {
			continue
		}
		hasContent = mapBackfillItem(writer, run.MessageID, item) || hasContent
	}
	if !hasContent && turn.Error == nil {
		return nil, false, nil
	}
	writer.StepFinish(turn.ID)
	if turn.Error != nil && strings.TrimSpace(turn.Error.Message) != "" {
		writer.Error(turn.Error.Message)
	} else {
		writer.Finish(agui.FinishReasonStop)
	}

	content, extra, err := cl.backfillFinalContent(ctx, portal, *run)
	if err != nil {
		return nil, false, err
	}
	msgID := "codex:" + turn.ID + ":assistant"
	return &bridgev2.BackfillMessage{
		ConvertedMessage: &bridgev2.ConvertedMessage{Parts: []*bridgev2.ConvertedMessagePart{{
			ID:         partID("text"),
			Type:       event.EventMessage,
			Content:    content,
			Extra:      extra,
			DBMetadata: &MessageMetadata{Role: "assistant", ThreadID: thread.ID, TurnID: turn.ID, StreamStatus: "complete"},
		}}},
		Sender:      bridgev2.EventSender{Sender: codexUserID},
		ID:          networkid.MessageID(msgID),
		TxnID:       networkid.TransactionID(msgID),
		Timestamp:   ts,
		StreamOrder: streamOrder,
	}, true, nil
}

func (cl *Client) backfillFinalContent(ctx context.Context, portal *bridgev2.Portal, run aistream.Run) (*event.MessageEventContent, map[string]any, error) {
	if cl != nil && cl.Main != nil && cl.Main.Bridge != nil && cl.Main.Bridge.Bot != nil && portal != nil && portal.MXID != "" {
		return matrixFinalContentWithAttachment(ctx, portal, cl.Main.Bridge.Bot, run)
	}
	content, extra := matrixFinalContent(run)
	return content, extra, nil
}

func mapBackfillItem(writer *aistream.Writer, messageID string, item appserver.TurnItem) bool {
	switch item.Type {
	case "agentMessage":
		if strings.TrimSpace(item.Text) == "" {
			return false
		}
		writer.Text(item.Text)
	case "contextCompaction":
		writer.Text(codexCompactionNotice)
	case "reasoning":
		reasoning := backfillReasoningTexts(item)
		if len(reasoning) == 0 {
			return false
		}
		for index, text := range reasoning {
			i := index
			writer.ReasoningDelta(reasoningSectionIndex(item.ID, &i), text)
		}
	case "hookPrompt":
		if text := hookPromptText(item); text != "" {
			writer.StateDelta(map[string]any{"codex": map[string]any{"hookPrompt": backfillItemData(item), "hookPromptText": text}})
		}
	case "enteredReviewMode", "exitedReviewMode":
		writer.Text(reviewModeText(item))
	case "plan":
		data := backfillItemData(item)
		writer.Add(planSnapshotActivity(messageID, map[string]any{
			"explanation": data["text"],
			"plan":        []any{data},
		}))
		writer.StateDelta(map[string]any{"codex": map[string]any{"turn/plan/updated": data}})
	case "commandExecution", "fileChange", "mcpToolCall", "dynamicToolCall", "collabAgentToolCall", "webSearch", "imageView", "imageGeneration":
		writeBackfillToolItem(writer, item)
	default:
		if strings.TrimSpace(item.Text) == "" {
			writer.Custom("codex/item", backfillItemData(item))
			return true
		}
		writer.Text(item.Text)
	}
	writer.Custom("codex/item", backfillItemData(item))
	return true
}

func writeBackfillToolItem(writer *aistream.Writer, item appserver.TurnItem) {
	data := backfillItemData(item)
	name := backfillToolName(item, data)
	state := codexItemToolState(data)
	writer.ToolStartWithMetadata(item.ID, name, 0, nil, map[string]any{"codexItem": data})
	if input, ok := codexItemToolInput(data); ok {
		if text := rawToolInputText(input); text != "" {
			writer.ToolArgs(item.ID, text, input)
		}
	}
	if result := backfillToolResultText(data); result != "" {
		writer.ToolResult(item.ID, result, state)
	}
	writer.ToolEnd(item.ID, name, data, map[string]any{"state": state, "status": codexItemStatusText(data, state)})
}

func backfillToolName(item appserver.TurnItem, data map[string]any) string {
	name := codexItem{ID: item.ID, Type: item.Type, Raw: data}.Name()
	if strings.TrimSpace(name) != "" {
		return name
	}
	return "codex item"
}

func backfillToolResultText(data map[string]any) string {
	return codexToolResultText(data, true)
}

func completedToolResultText(data map[string]any, alreadyStreamed bool) string {
	return codexToolResultText(data, !alreadyStreamed)
}

func codexToolResultText(data map[string]any, includeStreamedFields bool) string {
	if includeStreamedFields {
		if text := patchUpdateText(data); text != "" {
			return text
		}
	}
	keys := []string{"result", "error", "contentItems", "savedPath"}
	if includeStreamedFields {
		keys = append([]string{"aggregatedOutput"}, keys...)
	}
	for _, key := range keys {
		text := codexToolResultValueText(data[key])
		if text != "" {
			return text
		}
	}
	return ""
}

func codexToolResultValueText(value any) string {
	if value == nil {
		return ""
	}
	if text, ok := value.(string); ok {
		if text = strings.TrimSpace(text); text != "" {
			return text
		}
		return ""
	}
	raw, err := json.Marshal(value)
	if err == nil && len(raw) > 0 && string(raw) != "null" {
		return string(raw)
	}
	return ""
}

func isBackfilledThreadItemType(itemType string) bool {
	switch itemType {
	case "userMessage",
		"agentMessage",
		"contextCompaction",
		"reasoning",
		"hookPrompt",
		"enteredReviewMode",
		"exitedReviewMode",
		"plan",
		"commandExecution",
		"fileChange",
		"mcpToolCall",
		"dynamicToolCall",
		"collabAgentToolCall",
		"webSearch",
		"imageView",
		"imageGeneration":
		return true
	default:
		return false
	}
}

func backfillItemData(item appserver.TurnItem) map[string]any {
	data := map[string]any{}
	for key, value := range item.Raw {
		data[key] = value
	}
	if _, ok := data["id"]; !ok && item.ID != "" {
		data["id"] = item.ID
	}
	if _, ok := data["clientId"]; !ok && item.ClientID != "" {
		data["clientId"] = item.ClientID
	}
	if _, ok := data["type"]; !ok && item.Type != "" {
		data["type"] = item.Type
	}
	if _, ok := data["phase"]; !ok && item.Phase != "" {
		data["phase"] = item.Phase
	}
	if _, ok := data["text"]; !ok && item.Text != "" {
		data["text"] = item.Text
	}
	if _, ok := data["summary"]; !ok && len(item.Summary) > 0 {
		data["summary"] = item.Summary
	}
	if _, ok := data["content"]; !ok && len(item.ReasoningContent) > 0 {
		data["content"] = item.ReasoningContent
	}
	if _, ok := data["fragments"]; !ok && len(item.Fragments) > 0 {
		data["fragments"] = item.Fragments
	}
	if _, ok := data["review"]; !ok && item.Review != "" {
		data["review"] = item.Review
	}
	if _, ok := data["command"]; !ok && item.Command != "" {
		data["command"] = item.Command
	}
	if _, ok := data["aggregatedOutput"]; !ok && item.AggregatedOutput != "" {
		data["aggregatedOutput"] = item.AggregatedOutput
	}
	if _, ok := data["arguments"]; !ok && len(item.Arguments) > 0 {
		data["arguments"] = item.Arguments
	}
	return data
}

func backfillUserMessageID(item appserver.TurnItem, turnID string) networkid.MessageID {
	if item.ClientID != "" {
		return networkid.MessageID("user:" + item.ClientID)
	}
	return networkid.MessageID("codex:" + turnID + ":" + item.ID)
}

func backfillUserBody(item appserver.TurnItem) string {
	var parts []string
	for _, part := range item.Content {
		switch part.Type {
		case "text":
			if strings.TrimSpace(part.Text) == "" {
				continue
			}
			parts = append(parts, part.Text)
		case "image":
			parts = append(parts, strings.TrimSpace("Image: "+part.URL))
		case "localImage":
			parts = append(parts, strings.TrimSpace("Local image: "+part.Path))
		case "skill":
			parts = append(parts, strings.TrimSpace("Skill: "+strings.TrimSpace(part.Name+" "+part.Path)))
		case "mention":
			parts = append(parts, strings.TrimSpace("Mention: "+strings.TrimSpace(part.Name+" "+part.Path)))
		}
	}
	return strings.Join(parts, "\n\n")
}

func backfillReasoningTexts(item appserver.TurnItem) []string {
	var texts []string
	for _, text := range item.Summary {
		if strings.TrimSpace(text) != "" {
			texts = append(texts, text)
		}
	}
	for _, text := range item.ReasoningContent {
		if strings.TrimSpace(text) != "" {
			texts = append(texts, text)
		}
	}
	if strings.TrimSpace(item.Text) != "" {
		texts = append(texts, item.Text)
	}
	return texts
}

func hookPromptText(item appserver.TurnItem) string {
	var fragments []string
	for _, fragment := range item.Fragments {
		if strings.TrimSpace(fragment.Text) != "" {
			fragments = append(fragments, strings.TrimSpace(fragment.Text))
		}
	}
	return strings.Join(fragments, "\n\n")
}

func reviewModeText(item appserver.TurnItem) string {
	text := codexEnteredReviewNotice
	if item.Type == "exitedReviewMode" {
		text = codexExitedReviewNotice
	}
	if strings.TrimSpace(item.Review) != "" {
		text += "\n\n" + strings.TrimSpace(item.Review)
	}
	return text
}

func backfillTurnTime(thread appserver.Thread, turn appserver.Turn) time.Time {
	if turn.StartedAt > 0 {
		return time.Unix(turn.StartedAt, 0)
	}
	if thread.CreatedAt > 0 {
		return time.Unix(thread.CreatedAt, 0)
	}
	return time.Now()
}

func nextBackfillStreamOrder(previous int64, ts time.Time) int64 {
	order := ts.UnixNano()
	if order <= previous {
		return previous + 1
	}
	return order
}
