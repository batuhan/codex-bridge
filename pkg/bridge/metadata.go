package bridge

import (
	"context"
	"strings"

	aistream "github.com/beeper/ai-bridge/pkg/ai-stream"
	"github.com/beeper/ai-bridge/pkg/aiid"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

const (
	codexThreadStateType   = "com.beeper.codex.thread"
	beeperAIModelStateType = aiid.RoomModelType
)

type PortalMetadata struct {
	ThreadID        string `json:"thread_id,omitempty"`
	Cwd             string `json:"cwd,omitempty"`
	BackfillVersion int    `json:"backfill_version,omitempty"`
}

type MessageMetadata struct {
	Role         string                    `json:"role,omitempty"`
	ThreadID     string                    `json:"thread_id,omitempty"`
	TurnID       string                    `json:"turn_id,omitempty"`
	StreamStatus string                    `json:"stream_status,omitempty"`
	Approval     *aistream.ApprovalContext `json:"approval,omitempty"`
}

func (m *MessageMetadata) CopyFrom(other any) {
	if m == nil || other == nil {
		return
	}
	var src *MessageMetadata
	switch typed := other.(type) {
	case *MessageMetadata:
		src = typed
	case MessageMetadata:
		src = &typed
	default:
		return
	}
	if src.Role != "" {
		m.Role = src.Role
	}
	if src.ThreadID != "" {
		m.ThreadID = src.ThreadID
	}
	if src.TurnID != "" {
		m.TurnID = src.TurnID
	}
	if src.StreamStatus != "" {
		m.StreamStatus = src.StreamStatus
	}
	if src.Approval != nil {
		approval := *src.Approval
		m.Approval = &approval
	}
}

func portalMetadata(meta any) *PortalMetadata {
	if typed, ok := meta.(*PortalMetadata); ok && typed != nil {
		return typed
	}
	return &PortalMetadata{}
}

func codexPortalMetadataUpdater(cwd, threadID string) bridgev2.ExtraUpdater[*bridgev2.Portal] {
	return func(ctx context.Context, portal *bridgev2.Portal) bool {
		if portal == nil {
			return false
		}
		meta := portalMetadata(portal.Metadata)
		changed := false
		if cwd != "" && meta.Cwd != cwd {
			meta.Cwd = cwd
			changed = true
		}
		if threadID != "" && meta.ThreadID != threadID {
			meta.ThreadID = threadID
			changed = true
		}
		if changed {
			portal.Metadata = meta
		}
		return changed
	}
}

func normalizeStoredMessageMetadata(msg *database.Message) bool {
	if msg == nil {
		return false
	}
	meta, ok := msg.Metadata.(*MessageMetadata)
	if !ok || meta == nil {
		meta = &MessageMetadata{}
	}
	changed := false
	if meta.Role == "" {
		switch {
		case msg.SenderID == codexUserID:
			meta.Role = "assistant"
			changed = true
		case strings.HasPrefix(string(msg.SenderID), "login:"):
			meta.Role = "user"
			changed = true
		}
	}
	if meta.Role == "assistant" && meta.TurnID == "" {
		if turnID := turnIDFromMessageID(msg.ID); turnID != "" {
			meta.TurnID = turnID
			changed = true
		}
	}
	if meta.StreamStatus == "" {
		switch meta.Role {
		case "assistant":
			meta.StreamStatus = "complete"
			changed = true
		case "user":
			meta.StreamStatus = "done"
			changed = true
		}
	}
	if changed {
		msg.Metadata = meta
	}
	return changed
}

func turnIDFromMessageID(messageID networkid.MessageID) string {
	raw := string(messageID)
	if strings.HasPrefix(raw, "msg-") {
		return strings.TrimPrefix(raw, "msg-")
	}
	if before, turnID, ok := strings.Cut(raw, ":"); ok && before == "codex" {
		turnID, _, _ = strings.Cut(turnID, ":")
		return turnID
	}
	return ""
}
