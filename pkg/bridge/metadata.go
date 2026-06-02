package bridge

import (
	"context"

	aistream "github.com/beeper/ai-bridge/pkg/ai-stream"
	"maunium.net/go/mautrix/bridgev2"
)

const (
	codexThreadStateType   = "com.beeper.codex.thread"
	beeperAIModelStateType = "com.beeper.ai.model"
)

type PortalMetadata struct {
	ThreadID string `json:"thread_id,omitempty"`
	Cwd      string `json:"cwd,omitempty"`
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
