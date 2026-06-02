package appserver

import "encoding/json"

type AccountReadResponse struct {
	Account *Account `json:"account"`
}

type Account struct {
	Type  string `json:"type"`
	Email string `json:"email"`
}

type ThreadListResponse struct {
	Data            []Thread `json:"data"`
	NextCursor      string   `json:"nextCursor"`
	BackwardsCursor string   `json:"backwardsCursor"`
}

type ThreadReadResponse struct {
	Thread Thread `json:"thread"`
}

type ThreadStartResponse struct {
	Thread Thread `json:"thread"`
}

type TurnStartResponse struct {
	Turn Turn `json:"turn"`
}

type Thread struct {
	ID            string         `json:"id"`
	SessionID     string         `json:"sessionId"`
	Name          string         `json:"name"`
	Preview       string         `json:"preview"`
	Cwd           string         `json:"cwd"`
	Path          string         `json:"path"`
	CreatedAt     int64          `json:"createdAt"`
	UpdatedAt     int64          `json:"updatedAt"`
	ModelProvider string         `json:"modelProvider"`
	Turns         []Turn         `json:"turns"`
	Raw           map[string]any `json:"-"`
}

func (t *Thread) UnmarshalJSON(raw []byte) error {
	type threadJSON Thread
	var typed threadJSON
	if err := json.Unmarshal(raw, &typed); err != nil {
		return err
	}
	thread := Thread(typed)
	_ = json.Unmarshal(raw, &thread.Raw)
	*t = thread
	return nil
}

type Turn struct {
	ID     string `json:"id"`
	Status string `json:"status"`
	Error  *struct {
		Message string `json:"message"`
	} `json:"error,omitempty"`
	Items       []TurnItem `json:"items"`
	StartedAt   int64      `json:"startedAt"`
	CompletedAt int64      `json:"completedAt"`
}

type TurnItem struct {
	ID               string           `json:"id"`
	Type             string           `json:"type"`
	Text             string           `json:"text"`
	Phase            string           `json:"phase"`
	Content          []InputPart      `json:"content"`
	Summary          []string         `json:"summary"`
	ReasoningContent []string         `json:"-"`
	Fragments        []PromptFragment `json:"fragments"`
	Review           string           `json:"review"`
	Command          string           `json:"command"`
	AggregatedOutput string           `json:"aggregatedOutput"`
	Arguments        json.RawMessage  `json:"arguments"`
	Raw              map[string]any   `json:"-"`
}

func (t *TurnItem) UnmarshalJSON(raw []byte) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return err
	}
	var item TurnItem
	_ = json.Unmarshal(fields["id"], &item.ID)
	_ = json.Unmarshal(fields["type"], &item.Type)
	_ = json.Unmarshal(fields["text"], &item.Text)
	_ = json.Unmarshal(fields["phase"], &item.Phase)
	_ = json.Unmarshal(fields["command"], &item.Command)
	_ = json.Unmarshal(fields["aggregatedOutput"], &item.AggregatedOutput)
	_ = json.Unmarshal(fields["arguments"], &item.Arguments)
	_ = json.Unmarshal(fields["summary"], &item.Summary)
	_ = json.Unmarshal(fields["fragments"], &item.Fragments)
	_ = json.Unmarshal(fields["review"], &item.Review)
	if item.Type == "reasoning" {
		_ = json.Unmarshal(fields["content"], &item.ReasoningContent)
	} else {
		_ = json.Unmarshal(fields["content"], &item.Content)
	}
	_ = json.Unmarshal(raw, &item.Raw)
	*t = item
	return nil
}

type InputPart struct {
	Type string `json:"type"`
	Text string `json:"text"`
	URL  string `json:"url"`
	Path string `json:"path"`
	Name string `json:"name"`
}

type PromptFragment struct {
	Text      string `json:"text"`
	HookRunID string `json:"hookRunId"`
}
