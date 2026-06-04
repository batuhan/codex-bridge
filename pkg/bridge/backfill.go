package bridge

import (
	"context"
	"encoding/json"
	"slices"
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
	aimatrix "github.com/beeper/ai-bridge/pkg/ai-stream/matrix"
	"github.com/beeper/ai-bridge/pkg/msgconv"
	"github.com/beeper/codex-bridge/pkg/appserver"
)

const codexCompactionNotice = "Codex compacted the thread context."
const codexEnteredReviewNotice = "Codex entered review mode."
const codexExitedReviewNotice = "Codex exited review mode."
const backfillTextEventBudgetBytes = 48 * 1024
const backfillOversizedUserMessageNotice = "[backfilled user message truncated; full content was too large to inline]"

func reviewModeNoticeText(itemType, review string) string {
	text := codexEnteredReviewNotice
	if itemType == "exitedReviewMode" {
		text = codexExitedReviewNotice
	}
	if review := firstTrimmedNonEmpty(review); review != "" {
		return text + "\n\n" + review
	}
	return text
}

func (cl *Client) projectBackfillMessages(ctx context.Context, portal *bridgev2.Portal, thread appserver.Thread) ([]*bridgev2.BackfillMessage, error) {
	return cl.backfillMessagesFromEntries(ctx, portal, projectBackfillEntries(thread))
}

func (cl *Client) backfillMessagesFromEntries(ctx context.Context, portal *bridgev2.Portal, entries []*backfillEntry) ([]*bridgev2.BackfillMessage, error) {
	var messages []*bridgev2.BackfillMessage
	seenSubagents := map[string]bool{}
	for _, entry := range entries {
		switch entry.Kind {
		case backfillEntryUser:
			cl.queueSubagentResyncs(ctx, entry.Thread.ID, entry.Thread.Cwd, subagentRefs(backfillItemData(entry.Item)), seenSubagents)
			msg, err := cl.backfillUserMessage(ctx, portal, entry.Thread, entry.Turn.ID, entry.Item, entry.Body, entry.Timestamp, entry.StreamOrder)
			if err != nil {
				return nil, err
			}
			messages = append(messages, msg)
		case backfillEntryAssistant:
			cl.queueBackfillSubagentsForTurn(ctx, entry.Thread, entry.Turn, seenSubagents)
			msg, ok, err := cl.backfillAssistantMessage(ctx, portal, entry.Thread, entry.Turn, entry.Timestamp, entry.StreamOrder)
			if err != nil {
				return nil, err
			}
			if ok {
				messages = append(messages, msg)
			}
		}
	}
	return messages, nil
}

func dedupeBackfillMessages(messages []*bridgev2.BackfillMessage) []*bridgev2.BackfillMessage {
	if len(messages) < 2 {
		return messages
	}
	seen := make(map[networkid.MessageID]bool, len(messages))
	filtered := messages[:0]
	for _, msg := range messages {
		if msg == nil || msg.ID == "" {
			filtered = append(filtered, msg)
			continue
		}
		if seen[msg.ID] {
			continue
		}
		seen[msg.ID] = true
		filtered = append(filtered, msg)
	}
	return filtered
}

func (cl *Client) queueBackfillSubagentsForTurn(ctx context.Context, thread appserver.Thread, turn appserver.Turn, seen map[string]bool) {
	for _, item := range turn.Items {
		cl.queueSubagentResyncs(ctx, thread.ID, thread.Cwd, subagentRefs(backfillItemData(item)), seen)
	}
}

type backfillEntryKind int

const (
	backfillEntryUser backfillEntryKind = iota + 1
	backfillEntryAssistant
)

type backfillEntry struct {
	Kind        backfillEntryKind
	Thread      appserver.Thread
	Turn        appserver.Turn
	Item        appserver.TurnItem
	Body        string
	ID          networkid.MessageID
	Timestamp   time.Time
	StreamOrder int64
}

func projectBackfillEntries(thread appserver.Thread) []*backfillEntry {
	var entries []*backfillEntry
	var streamOrder int64
	for _, turn := range sortedBackfillTurns(thread) {
		ts := backfillTurnTime(thread, turn)
		for _, item := range turn.Items {
			body := backfillUserMessageBody(item)
			if body == "" {
				continue
			}
			streamOrder = nextBackfillStreamOrder(streamOrder, ts)
			entries = append(entries, &backfillEntry{
				Kind:        backfillEntryUser,
				Thread:      thread,
				Turn:        turn,
				Item:        item,
				Body:        body,
				ID:          backfillUserMessageID(turn.ID, item),
				Timestamp:   ts,
				StreamOrder: streamOrder,
			})
			ts = ts.Add(time.Millisecond)
		}
		streamOrder = nextBackfillStreamOrder(streamOrder, ts)
		if shouldBackfillAssistantEntry(thread, turn, ts) {
			entries = append(entries, &backfillEntry{
				Kind:        backfillEntryAssistant,
				Thread:      thread,
				Turn:        turn,
				ID:          backfillAssistantMessageID(turn.ID),
				Timestamp:   ts,
				StreamOrder: streamOrder,
			})
		}
	}
	return entries
}

func shouldBackfillAssistantEntry(thread appserver.Thread, turn appserver.Turn, ts time.Time) bool {
	run := newBackfillRun(thread, turn, ts)
	writer := aistream.NewWriter(run, func() time.Time { return ts })
	writer.Start()
	var reasoningSections reasoningSectionState
	hasContent := mapBackfillAssistantItems(writer, run.MessageID, turn.Items, &reasoningSections)
	return !skipBackfillAssistantMessage(codexTurnStatusKind(turn.Status), hasContent, turn.Error != nil)
}

func sortedBackfillTurns(thread appserver.Thread) []appserver.Turn {
	turns := slices.Clone(thread.Turns)
	sort.SliceStable(turns, func(i, j int) bool {
		return backfillTurnTime(thread, turns[i]).Before(backfillTurnTime(thread, turns[j]))
	})
	return turns
}

func paginateBackfillMessages(messages []*bridgev2.BackfillMessage, params bridgev2.FetchMessagesParams) *bridgev2.FetchMessagesResponse {
	count := backfillPageCount(params.Count, len(messages))
	if params.Forward {
		start, end := forwardBackfillRange(messages, params, count)
		return forwardBackfillResponse(messages[start:end])
	}
	start, end := backwardBackfillRange(messages, params, count)
	return backwardBackfillResponse(messages[start:end], params, start, len(messages))
}

func paginateBackfillEntries(entries []*backfillEntry, params bridgev2.FetchMessagesParams) ([]*backfillEntry, *bridgev2.FetchMessagesResponse) {
	count := backfillPageCount(params.Count, len(entries))
	if params.Forward {
		start, end := forwardBackfillEntryRange(entries, params, count)
		resp := forwardBackfillResponse(nil)
		return entries[start:end], resp
	}
	start, end := backwardBackfillEntryRange(entries, params, count)
	resp := backwardBackfillResponse(nil, params, start, len(entries))
	return entries[start:end], resp
}

func forwardBackfillResponse(messages []*bridgev2.BackfillMessage) *bridgev2.FetchMessagesResponse {
	return &bridgev2.FetchMessagesResponse{
		Messages: messages,
		Forward:  true,
		MarkRead: true,
	}
}

func backwardBackfillResponse(messages []*bridgev2.BackfillMessage, params bridgev2.FetchMessagesParams, start, total int) *bridgev2.FetchMessagesResponse {
	return &bridgev2.FetchMessagesResponse{
		Messages:             messages,
		Cursor:               backfillPaginationCursor(start),
		HasMore:              start > 0,
		MarkRead:             params.AnchorMessage == nil && params.Cursor == "",
		ApproxTotalCount:     total,
		ApproxRemainingCount: start,
	}
}

func forwardBackfillRange(messages []*bridgev2.BackfillMessage, params bridgev2.FetchMessagesParams, count int) (int, int) {
	start := forwardBackfillStart(messages, params, count)
	return start, min(len(messages), start+count)
}

func backwardBackfillRange(messages []*bridgev2.BackfillMessage, params bridgev2.FetchMessagesParams, count int) (int, int) {
	end := backwardBackfillEnd(messages, params, len(messages))
	return max(0, end-count), end
}

func forwardBackfillEntryRange(entries []*backfillEntry, params bridgev2.FetchMessagesParams, count int) (int, int) {
	start := forwardBackfillEntryStart(entries, params, count)
	return start, min(len(entries), start+count)
}

func backwardBackfillEntryRange(entries []*backfillEntry, params bridgev2.FetchMessagesParams, count int) (int, int) {
	end := backwardBackfillEntryEnd(entries, params, len(entries))
	return max(0, end-count), end
}

func forwardBackfillStart(messages []*bridgev2.BackfillMessage, params bridgev2.FetchMessagesParams, count int) int {
	if params.AnchorMessage == nil {
		if len(messages) > count {
			return len(messages) - count
		}
		return 0
	}
	start := len(messages)
	for i, msg := range messages {
		if msg.ID == params.AnchorMessage.ID {
			return i + 1
		}
		if start == len(messages) && msg.Timestamp.After(params.AnchorMessage.Timestamp) {
			start = i
		}
	}
	return start
}

func forwardBackfillEntryStart(entries []*backfillEntry, params bridgev2.FetchMessagesParams, count int) int {
	if params.AnchorMessage == nil {
		if len(entries) > count {
			return len(entries) - count
		}
		return 0
	}
	start := len(entries)
	for i, entry := range entries {
		if entry.ID == params.AnchorMessage.ID {
			return i + 1
		}
		if start == len(entries) && entry.Timestamp.After(params.AnchorMessage.Timestamp) {
			start = i
		}
	}
	return start
}

func backwardBackfillEnd(messages []*bridgev2.BackfillMessage, params bridgev2.FetchMessagesParams, fallback int) int {
	if params.Cursor != "" {
		if cursorEnd, err := strconv.Atoi(string(params.Cursor)); err == nil {
			return max(0, min(cursorEnd, len(messages)))
		}
		return fallback
	}
	if params.AnchorMessage == nil {
		return fallback
	}
	return backwardAnchorEnd(messages, params.AnchorMessage, fallback)
}

func backwardBackfillEntryEnd(entries []*backfillEntry, params bridgev2.FetchMessagesParams, fallback int) int {
	if params.Cursor != "" {
		if cursorEnd, err := strconv.Atoi(string(params.Cursor)); err == nil {
			return max(0, min(cursorEnd, len(entries)))
		}
		return fallback
	}
	if params.AnchorMessage == nil {
		return fallback
	}
	return backwardAnchorEntryEnd(entries, params.AnchorMessage, fallback)
}

func backwardAnchorEnd(messages []*bridgev2.BackfillMessage, anchor *database.Message, fallback int) int {
	end := fallback
	for i, msg := range messages {
		if msg.ID == anchor.ID {
			return i
		}
		if end == fallback && !msg.Timestamp.Before(anchor.Timestamp) {
			end = i
		}
	}
	return end
}

func backwardAnchorEntryEnd(entries []*backfillEntry, anchor *database.Message, fallback int) int {
	end := fallback
	for i, entry := range entries {
		if entry.ID == anchor.ID {
			return i
		}
		if end == fallback && !entry.Timestamp.Before(anchor.Timestamp) {
			end = i
		}
	}
	return end
}

func backfillPageCount(requested, available int) int {
	if requested <= 0 || requested > available {
		return available
	}
	return requested
}

func backfillPaginationCursor(start int) networkid.PaginationCursor {
	if start <= 0 {
		var cursor networkid.PaginationCursor
		return cursor
	}
	return networkid.PaginationCursor(strconv.Itoa(start))
}

func (cl *Client) backfillUserMessage(ctx context.Context, portal *bridgev2.Portal, thread appserver.Thread, turnID string, item appserver.TurnItem, body string, ts time.Time, streamOrder int64) (*bridgev2.BackfillMessage, error) {
	msgID := backfillUserMessageID(turnID, item)
	content, extra, err := cl.backfillUserMessageContent(ctx, portal, msgID, thread, body, ts)
	if err != nil {
		return nil, err
	}
	return &bridgev2.BackfillMessage{
		ConvertedMessage: &bridgev2.ConvertedMessage{Parts: []*bridgev2.ConvertedMessagePart{{
			ID:         partID("text"),
			Type:       event.EventMessage,
			Content:    content,
			Extra:      extra,
			DBMetadata: backfillUserMetadata(thread.ID, turnID),
		}}},
		Sender:      cl.backfillUserSender(),
		ID:          msgID,
		TxnID:       backfillTransactionID(msgID),
		Timestamp:   ts,
		StreamOrder: streamOrder,
	}, nil
}

func (cl *Client) backfillUserMessageContent(ctx context.Context, portal *bridgev2.Portal, msgID networkid.MessageID, thread appserver.Thread, body string, ts time.Time) (*event.MessageEventContent, map[string]any, error) {
	if len([]byte(body)) <= backfillTextEventBudgetBytes {
		return msgconv.TextContent(body), nil, nil
	}
	if portal != nil && portal.MXID != "" {
		if intent := cl.backfillBotIntent(); intent != nil {
			return backfillUserMessageAIContent(ctx, portal, intent, msgID, thread, body, ts)
		}
	}
	return msgconv.TextContent(utf8PrefixBytes(body, backfillTextEventBudgetBytes-len(backfillOversizedUserMessageNotice)-2) + "\n\n" + backfillOversizedUserMessageNotice), nil, nil
}

func backfillUserMessageAIContent(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, msgID networkid.MessageID, thread appserver.Thread, body string, ts time.Time) (*event.MessageEventContent, map[string]any, error) {
	messageID := string(msgID)
	run := aistream.NewRun("user-"+sanitizeID(messageID), thread.ID, backfillRunModel(thread), "codex", "Codex", ts)
	run.MessageID = messageID
	run.Status = aistream.Status{State: "complete", FinishReason: agui.FinishReasonStop}
	message := aistream.UIMessage{
		ID:        messageID,
		Role:      agui.RoleUser,
		CreatedAt: &ts,
		Parts: []aistream.MessagePart{{
			"type":      "text",
			"id":        string(partID("text")),
			"messageId": messageID,
			"content":   body,
			"state":     agui.PartStateDone,
		}},
	}
	partsRef, err := aimatrix.UploadFinalPartsRef(ctx, portal, intent, *run, message)
	if err != nil {
		return nil, nil, err
	}
	textComplete := false
	run.Final = aistream.FinalDelivery{
		Delivery:      "attachment",
		TextComplete:  &textComplete,
		PartsComplete: false,
		PartsRef:      partsRef,
	}
	inline := aistream.UIMessage{
		ID:        messageID,
		Role:      agui.RoleUser,
		CreatedAt: &ts,
		Parts:     []aistream.MessagePart{},
	}
	extra := map[string]any{aistream.BeeperAIKey: run.AIWithMessage(aistream.AIKindFinal, inline)}
	return msgconv.TextContent(aistream.BoundedPreview(body, aistream.PreviewBudgetBytes)), extra, nil
}

func (cl *Client) backfillUserSender() bridgev2.EventSender {
	return bridgev2.EventSender{Sender: cl.GetUserID(), IsFromMe: true, SenderLogin: cl.UserLogin.ID}
}

func backfillUserMetadata(threadID, turnID string) *MessageMetadata {
	return &MessageMetadata{Role: "user", ThreadID: threadID, TurnID: turnID, StreamStatus: "done"}
}

func backfillUserMessageID(turnID string, item appserver.TurnItem) networkid.MessageID {
	if item.ClientID != "" {
		return networkid.MessageID("user:" + item.ClientID)
	}
	return networkid.MessageID("codex:" + turnID + ":" + item.ID)
}

func (cl *Client) backfillAssistantMessage(ctx context.Context, portal *bridgev2.Portal, thread appserver.Thread, turn appserver.Turn, ts time.Time, streamOrder int64) (*bridgev2.BackfillMessage, bool, error) {
	run := newBackfillRun(thread, turn, ts)
	writer := aistream.NewWriter(run, func() time.Time { return ts })
	writer.Start()

	var reasoningSections reasoningSectionState
	hasContent := mapBackfillAssistantItems(writer, run.MessageID, turn.Items, &reasoningSections)
	statusKind := codexTurnStatusKind(turn.Status)
	if skipBackfillAssistantMessage(statusKind, hasContent, turn.Error != nil) {
		return nil, false, nil
	}
	finishCodexTurn(writer, turn.Status, backfillTurnErrorMessage(turn))

	content, extra, err := matrixFinalContent(ctx, portal, cl.backfillBotIntent(), *run)
	if err != nil {
		return nil, false, err
	}
	msgID := backfillAssistantMessageID(turn.ID)
	return &bridgev2.BackfillMessage{
		ConvertedMessage: &bridgev2.ConvertedMessage{Parts: []*bridgev2.ConvertedMessagePart{{
			ID:         partID("text"),
			Type:       event.EventMessage,
			Content:    content,
			Extra:      extra,
			DBMetadata: finalMessageMetadata(*run, thread.ID, turn.ID),
		}}},
		Sender:      backfillAssistantSender(),
		ID:          msgID,
		TxnID:       backfillTransactionID(msgID),
		Timestamp:   ts,
		StreamOrder: streamOrder,
	}, true, nil
}

func backfillAssistantSender() bridgev2.EventSender {
	return bridgev2.EventSender{Sender: codexUserID}
}

func newBackfillRun(thread appserver.Thread, turn appserver.Turn, ts time.Time) *aistream.Run {
	run := aistream.NewRun(turn.ID, thread.ID, backfillRunModel(thread), "codex", "Codex", ts)
	run.Data["capabilities"] = codexAgentCapabilities()
	return run
}

func backfillRunModel(thread appserver.Thread) string {
	return firstNonEmptyString(codexModelStateRef(thread.Raw, thread.ModelProvider), modelProviderRef(thread.ModelProvider))
}

func mapBackfillAssistantItems(writer *aistream.Writer, messageID string, items []appserver.TurnItem, reasoningSections *reasoningSectionState) bool {
	hasContent := false
	for _, item := range items {
		if isBackfillUserMessage(item) {
			continue
		}
		hasContent = mapBackfillItem(writer, messageID, item, reasoningSections) || hasContent
	}
	return hasContent
}

func backfillTurnErrorMessage(turn appserver.Turn) string {
	if turn.Error == nil {
		return ""
	}
	return turn.Error.Message
}

func backfillTransactionID(msgID networkid.MessageID) networkid.TransactionID {
	return networkid.TransactionID(msgID)
}

func backfillAssistantMessageID(turnID string) networkid.MessageID {
	return networkid.MessageID("codex:" + turnID + ":assistant")
}

func skipBackfillAssistantMessage(statusKind string, hasContent, hasError bool) bool {
	if statusKind == "in_progress" && !hasError {
		return true
	}
	return !hasContent && !hasError && statusKind != "error" && statusKind != "aborted"
}

func (cl *Client) backfillBotIntent() bridgev2.MatrixAPI {
	if cl == nil || cl.Main == nil || cl.Main.Bridge == nil {
		return nil
	}
	return cl.Main.Bridge.Bot
}

func mapBackfillItem(writer *aistream.Writer, messageID string, item appserver.TurnItem, reasoningSections *reasoningSectionState) bool {
	if isCodexToolItemType(item.Type) {
		writeBackfillToolItem(writer, item)
		return true
	}
	switch item.Type {
	case "agentMessage":
		if firstTrimmedNonEmpty(item.Text) == "" {
			return false
		}
		writer.Text(item.Text)
	case "contextCompaction":
		writer.Text(codexCompactionNotice)
	case "reasoning":
		if reasoningSections == nil {
			reasoningSections = &reasoningSectionState{}
		}
		reasoning := backfillReasoningText(item)
		if len(reasoning) == 0 {
			return false
		}
		for index, text := range reasoning {
			i := index
			key := reasoningContentKey(item.ID, reasoningKindContent, &i)
			writer.ReasoningDelta(reasoningSections.index(key), text)
		}
	case "hookPrompt":
		if text := backfillHookPromptText(item); text != "" {
			writer.StateDelta(hookPromptStateDelta(item.ID, text))
		}
	case "enteredReviewMode", "exitedReviewMode":
		writer.Text(reviewModeNoticeText(item.Type, item.Review))
	case "plan":
		data := backfillItemData(item)
		writer.Add(planItemSnapshotActivity(messageID, data))
	default:
		if mapBackfillRawResponseItem(writer, item) {
			return true
		}
		return mapBackfillItemText(writer, item)
	}
	return true
}

func mapBackfillItemText(writer *aistream.Writer, item appserver.TurnItem) bool {
	if firstTrimmedNonEmpty(item.Text) == "" {
		return false
	}
	writer.Text(item.Text)
	return true
}

func backfillReasoningText(item appserver.TurnItem) []string {
	var reasoning []string
	for _, text := range item.Summary {
		appendNonBlankOriginal(&reasoning, text)
	}
	for _, text := range item.ReasoningContent {
		appendNonBlankOriginal(&reasoning, text)
	}
	appendNonBlankOriginal(&reasoning, item.Text)
	if len(reasoning) != 0 {
		return reasoning
	}
	reasoning = append(reasoning, rawTextItems(item.Raw[reasoningKindSummary])...)
	return append(reasoning, rawTextItems(item.Raw[reasoningKindContent])...)
}

func backfillHookPromptText(item appserver.TurnItem) string {
	var fragments []string
	for _, fragment := range item.Fragments {
		appendTrimmedNonEmpty(&fragments, fragment.Text)
	}
	if text := strings.Join(fragments, "\n\n"); text != "" {
		return text
	}
	return liveHookPromptText(backfillItemData(item))
}

func mapBackfillRawResponseItem(writer *aistream.Writer, item appserver.TurnItem) bool {
	data := backfillItemData(item)
	switch item.Type {
	case "message":
		if firstString(data, "role") != agui.RoleAssistant {
			return false
		}
		text := trimmedRawContentText(data["content"])
		if text == "" {
			return false
		}
		writer.Text(text)
	case "function_call", "custom_tool_call", "tool_search_call":
		return mapBackfillRawToolCall(writer, data)
	case "function_call_output", "custom_tool_call_output", "tool_search_output":
		return mapBackfillRawToolCallOutput(writer, data)
	case "local_shell_call", "web_search_call", "image_generation_call":
		return mapBackfillRawCompletingToolCall(writer, data)
	case "context_compaction", "compaction", "compaction_trigger":
		writer.Text(codexCompactionNotice)
	default:
		return false
	}
	return true
}

func mapBackfillRawToolCall(writer *aistream.Writer, data map[string]any) bool {
	callID, name, input, ok := backfillRawToolCallInput(data)
	if !ok {
		return false
	}
	ensureBackfillToolStarted(writer, callID, name, data)
	writeBackfillToolArgs(writer, callID, input)
	if !backfillHasToolEvent(writer, callID, agui.EventToolCallEnd) {
		writer.ToolInputComplete(callID, name, input)
	}
	return true
}

func mapBackfillRawToolCallOutput(writer *aistream.Writer, data map[string]any) bool {
	callID, ok := backfillRawToolCallID(data)
	if !ok {
		return false
	}
	name := rawToolName(data)
	ensureBackfillToolStarted(writer, callID, name, data)
	writeBackfillToolResult(writer, callID, data, agui.ToolResultStateComplete)
	return true
}

func mapBackfillRawCompletingToolCall(writer *aistream.Writer, data map[string]any) bool {
	callID, name, input, ok := backfillRawToolCallInput(data)
	if !ok {
		return false
	}
	status := firstString(data, "status")
	state := toolStateFromStatus(status)
	ensureBackfillToolStarted(writer, callID, name, data)
	hasResult := writeBackfillToolResult(writer, callID, data, state)
	writeBackfillToolArgs(writer, callID, input)
	if state != agui.ToolResultStateStreaming && !backfillHasToolEvent(writer, callID, agui.EventToolCallEnd) {
		finishBackfillTool(writer, callID, name, input, state, status, hasResult)
	}
	return true
}

func backfillRawToolCallInput(data map[string]any) (callID, name string, input any, ok bool) {
	callID, ok = backfillRawToolCallID(data)
	if !ok {
		return "", "", nil, false
	}
	return callID, rawToolName(data), rawToolInput(data), true
}

func backfillRawToolCallID(data map[string]any) (string, bool) {
	callID := rawToolCallID(data)
	return callID, callID != ""
}

func ensureBackfillToolStarted(writer *aistream.Writer, callID, name string, data map[string]any) {
	if backfillHasToolEvent(writer, callID, agui.EventToolCallStart) {
		return
	}
	writer.ToolStartWithMetadata(callID, name, 0, nil, codexToolStartMetadata(data))
}

func writeBackfillToolArgs(writer *aistream.Writer, callID string, input any) {
	if text := rawToolInputText(input); text != "" {
		writer.ToolArgs(callID, text, input)
	}
}

func writeBackfillToolResult(writer *aistream.Writer, callID string, data map[string]any, state string) bool {
	output := rawToolResultText(data)
	return writeBackfillVisibleToolResult(writer, callID, output, state)
}

func writeBackfillVisibleToolResult(writer *aistream.Writer, callID string, output string, state string) bool {
	if output == "" {
		return false
	}
	writer.ToolResult(callID, output, state)
	return true
}

func finishBackfillTool(writer *aistream.Writer, callID, name string, input any, state, status string, hasResult bool) {
	if hasResult {
		writer.ToolInputComplete(callID, name, input)
		return
	}
	writer.ToolEnd(callID, name, input, toolEndResult(state, status))
}

func backfillHasToolEvent(writer *aistream.Writer, toolCallID string, eventType string) bool {
	if writer == nil || writer.Run == nil || toolCallID == "" {
		return false
	}
	for _, event := range writer.Run.Events {
		if event.Type() == eventType && event.String("toolCallId") == toolCallID {
			return true
		}
	}
	return false
}

func writeBackfillToolItem(writer *aistream.Writer, item appserver.TurnItem) {
	data := backfillItemData(item)
	name := codexItem{ID: item.ID, Type: item.Type, Raw: data}.Name()
	state := codexItemToolState(data)
	writer.ToolStartWithMetadata(item.ID, name, 0, nil, codexToolStartMetadata(data))
	input, hasInput := codexItemToolInput(data)
	if hasInput {
		writeBackfillToolArgs(writer, item.ID, input)
	}
	hasResult := writeBackfillCodexToolResult(writer, item.ID, data, state)
	if state == agui.ToolResultStateStreaming {
		return
	}
	status := codexItemStatusText(data, state)
	if !hasInput {
		input = nil
	}
	finishBackfillTool(writer, item.ID, name, input, state, status, hasResult)
}

func writeBackfillCodexToolResult(writer *aistream.Writer, itemID string, data map[string]any, state string) bool {
	result := codexToolResultText(data, true)
	return writeBackfillVisibleToolResult(writer, itemID, result, state)
}

func codexToolResultText(data map[string]any, includeStreamedFields bool) string {
	if !includeStreamedFields {
		return firstToolResultValueText(data, codexToolResultKeys()...)
	}
	if text := patchUpdateText(data); text != "" {
		return text
	}
	if text := codexToolResultValueText(data["aggregatedOutput"]); text != "" {
		return text
	}
	return firstToolResultValueText(data, codexToolResultKeys()...)
}

func codexToolResultKeys() []string {
	return []string{"result", "error", "contentItems", "savedPath"}
}

func codexToolResultValueText(value any) string {
	if text, ok := value.(string); ok {
		return firstTrimmedNonEmpty(text)
	}
	if typed, ok := value.(map[string]any); ok {
		return codexToolResultMapText(typed)
	} else if text := trimmedRawContentText(value); text != "" {
		return text
	}
	return jsonToolResultValueText(value)
}

func codexToolResultMapText(value map[string]any) string {
	if text := trimmedRawContentText(value); isCompleteToolResultText(value, text, contentToolResultIgnoredFields()...) {
		return text
	}
	if message := firstString(value, "message"); isCompleteToolResultText(value, message, messageToolResultIgnoredFields()...) {
		return message
	}
	return jsonToolResultValueText(value)
}

func jsonToolResultValueText(value any) string {
	raw, err := json.Marshal(value)
	if err != nil || len(raw) == 0 || string(raw) == "null" {
		return ""
	}
	return string(raw)
}

func contentToolResultIgnoredFields() []string {
	return []string{"content", "contentItems", "text", "type", "_meta"}
}

func messageToolResultIgnoredFields() []string {
	return []string{"message"}
}

func isCompleteToolResultText(value map[string]any, text string, ignored ...string) bool {
	return text != "" && !hasAdditionalToolResultFields(value, ignored...)
}

func firstToolResultValueText(data map[string]any, keys ...string) string {
	for _, key := range keys {
		if text := codexToolResultValueText(data[key]); text != "" {
			return text
		}
	}
	return ""
}

func codexToolCompletionMetadataText(data map[string]any, state, status string, hasVisibleResult bool) string {
	if !shouldEmitToolCompletionMetadata(state, hasVisibleResult) {
		return ""
	}
	meta := codexToolCompletionMetadata(data, state, status)
	if !hasToolCompletionMetadataExtras(meta) {
		return ""
	}
	raw, err := json.Marshal(meta)
	if err != nil {
		return ""
	}
	return string(raw)
}

func shouldEmitToolCompletionMetadata(state string, hasVisibleResult bool) bool {
	return hasVisibleResult && state != agui.ToolResultStateStreaming
}

func codexToolCompletionMetadata(data map[string]any, state, status string) map[string]any {
	meta := toolEndResult(state, status)
	copyNonEmptyMapFields(meta, data, codexToolCompletionMetadataFields()...)
	return meta
}

func codexToolCompletionMetadataFields() []string {
	return []string{"exitCode", "durationMs", "success"}
}

func hasToolCompletionMetadataExtras(meta map[string]any) bool {
	return len(meta) > 2
}

func hasAdditionalToolResultFields(value map[string]any, ignored ...string) bool {
	for key, field := range value {
		if isIncludedNonEmptyField(key, field, ignored) {
			return true
		}
	}
	return false
}

func backfillItemData(item appserver.TurnItem) map[string]any {
	data := copyStateMap(item.Raw)
	enrichBackfillItemData(data, item)
	return data
}

func enrichBackfillItemData(data map[string]any, item appserver.TurnItem) {
	for _, field := range backfillItemFields(item) {
		setMissingMapField(data, field)
	}
}

type backfillItemField struct {
	key     string
	value   any
	include bool
}

func backfillItemFields(item appserver.TurnItem) []backfillItemField {
	return []backfillItemField{
		{"id", item.ID, item.ID != ""},
		{"clientId", item.ClientID, item.ClientID != ""},
		{"type", item.Type, item.Type != ""},
		{"phase", item.Phase, item.Phase != ""},
		{"text", item.Text, item.Text != ""},
		{"review", item.Review, item.Review != ""},
		{"command", item.Command, item.Command != ""},
		{"aggregatedOutput", item.AggregatedOutput, item.AggregatedOutput != ""},
		{"summary", item.Summary, len(item.Summary) > 0},
		{"content", item.ReasoningContent, len(item.ReasoningContent) > 0},
		{"fragments", item.Fragments, len(item.Fragments) > 0},
		{"arguments", item.Arguments, len(item.Arguments) > 0},
	}
}

func setMissingMapField(data map[string]any, field backfillItemField) {
	if !field.include {
		return
	}
	if _, exists := data[field.key]; exists {
		return
	}
	data[field.key] = field.value
}

func appendNonBlankOriginal(values *[]string, text string) {
	if firstTrimmedNonEmpty(text) != "" {
		*values = append(*values, text)
	}
}

func backfillUserBody(item appserver.TurnItem) string {
	var parts []string
	for _, part := range item.Content {
		appendNonBlankOriginal(&parts, backfillUserPartText(part))
	}
	return strings.Join(parts, "\n\n")
}

func backfillUserPartText(part appserver.InputPart) string {
	switch part.Type {
	case "text":
		if firstTrimmedNonEmpty(part.Text) != "" {
			return part.Text
		}
	case "image":
		return firstTrimmedNonEmpty("Image: " + part.URL)
	case "localImage":
		return firstTrimmedNonEmpty("Local image: " + part.Path)
	case "skill":
		return firstTrimmedNonEmpty("Skill: " + trimmedSpaceJoin(part.Name, part.Path))
	case "mention":
		return firstTrimmedNonEmpty("Mention: " + trimmedSpaceJoin(part.Name, part.Path))
	}
	return ""
}

func backfillUserMessageBody(item appserver.TurnItem) string {
	if !isBackfillUserMessage(item) {
		return ""
	}
	return backfillUserBody(item)
}

func isBackfillUserMessage(item appserver.TurnItem) bool {
	return item.Type == "userMessage"
}

func appendTrimmedNonEmpty(values *[]string, text string) {
	if text := firstTrimmedNonEmpty(text); text != "" {
		*values = append(*values, text)
	}
}

func backfillTurnTime(thread appserver.Thread, turn appserver.Turn) time.Time {
	if ts := firstPositiveInt64(turn.StartedAt, thread.CreatedAt); ts > 0 {
		return time.Unix(ts, 0)
	}
	return time.Now()
}

func nextBackfillStreamOrder(previous int64, ts time.Time) int64 {
	order := ts.UnixNano()
	return max(order, previous+1)
}

func firstPositiveInt64(values ...int64) int64 {
	for _, value := range values {
		if value > 0 {
			return value
		}
	}
	return 0
}
