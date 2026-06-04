package bridge

import (
	aistream "github.com/beeper/ai-bridge/pkg/ai-stream"
	"github.com/beeper/ai-bridge/pkg/aiid"
	"maunium.net/go/mautrix/bridgev2"
)

const (
	codexThreadStateType   = "com.beeper.codex.thread"
	beeperAIModelStateType = aiid.RoomModelType

	portalKindProject  = "project"
	portalKindSubagent = "subagent"
)

type PortalMetadata struct {
	ThreadID               string `json:"thread_id,omitempty"`
	Cwd                    string `json:"cwd,omitempty"`
	Kind                   string `json:"kind,omitempty"`
	ParentThreadID         string `json:"parent_thread_id,omitempty"`
	ReadOnly               bool   `json:"read_only,omitempty"`
	BackfillVersion        int    `json:"backfill_version,omitempty"`
	BackfillDisabled       bool   `json:"backfill_disabled,omitempty"`
	NewProjectIntroMessage bool   `json:"new_project_intro_message,omitempty"`
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
	src, ok := messageMetadata(other)
	if !ok {
		return
	}
	m.copyScalarFields(src)
	m.copyApproval(src)
}

func (m *MessageMetadata) copyApproval(src *MessageMetadata) {
	if approval := cloneApprovalContext(src.Approval); approval != nil {
		m.Approval = approval
	}
}

func (m *MessageMetadata) copyScalarFields(src *MessageMetadata) {
	setNonEmptyStringField(&m.Role, src.Role)
	setNonEmptyStringField(&m.ThreadID, src.ThreadID)
	setNonEmptyStringField(&m.TurnID, src.TurnID)
	setNonEmptyStringField(&m.StreamStatus, src.StreamStatus)
}

func cloneApprovalContext(src *aistream.ApprovalContext) *aistream.ApprovalContext {
	if src == nil {
		return nil
	}
	approval := *src
	return &approval
}

func messageMetadata(meta any) (*MessageMetadata, bool) {
	switch typed := meta.(type) {
	case *MessageMetadata:
		return typed, typed != nil
	case MessageMetadata:
		return &typed, true
	default:
		return nil, false
	}
}

func portalMetadata(meta any) *PortalMetadata {
	typed, _ := meta.(*PortalMetadata)
	if typed == nil {
		return &PortalMetadata{}
	}
	return typed
}

func (m *PortalMetadata) applyProject(cwd, threadID string) bool {
	if !m.canApplyProject(cwd, threadID) {
		return false
	}
	changed := false
	m.applyProjectDefaults(&changed)
	m.applyProjectSession(cwd, threadID, &changed)
	return changed
}

func (m *PortalMetadata) applyProjectDefaults(changed *bool) {
	setMetadataField(&m.Kind, portalKindProject, changed)
	setMetadataField(&m.ReadOnly, false, changed)
	setMetadataField(&m.ParentThreadID, "", changed)
}

func (m *PortalMetadata) applyProjectSession(cwd, threadID string, changed *bool) {
	setNonEmptyMetadataString(&m.Cwd, cwd, changed)
	setNonEmptyMetadataString(&m.ThreadID, threadID, changed)
}

func (m *PortalMetadata) canApplyProject(cwd, threadID string) bool {
	return m != nil && (cwd != "" || threadID != "")
}

func (m *PortalMetadata) applySubagent(parentThreadID, threadID, cwd string) bool {
	if !m.canApplySubagent(threadID) {
		return false
	}
	changed := false
	m.applySubagentFields(parentThreadID, threadID, cwd, &changed)
	return changed
}

func (m *PortalMetadata) applySubagentFields(parentThreadID, threadID, cwd string, changed *bool) {
	setMetadataField(&m.Kind, portalKindSubagent, changed)
	setMetadataField(&m.ThreadID, threadID, changed)
	setMetadataField(&m.ParentThreadID, parentThreadID, changed)
	setMetadataField(&m.Cwd, cwd, changed)
	setMetadataField(&m.ReadOnly, true, changed)
}

func (m *PortalMetadata) canApplySubagent(threadID string) bool {
	return m != nil && threadID != ""
}

func setMetadataField[T comparable](field *T, value T, changed *bool) {
	if *field == value {
		return
	}
	*field = value
	*changed = true
}

func setNonEmptyMetadataString(field *string, value string, changed *bool) {
	if value == "" {
		return
	}
	setMetadataField(field, value, changed)
}

func setNonEmptyStringField(field *string, value string) {
	if value == "" {
		return
	}
	*field = value
}

func isReadOnlyPortal(portal *bridgev2.Portal) bool {
	if portal == nil {
		return false
	}
	meta := portalMetadata(portal.Metadata)
	return meta.ReadOnly || meta.Kind == portalKindSubagent
}
