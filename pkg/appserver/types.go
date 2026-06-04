package appserver

import "encoding/json"

type AccountReadResponse struct {
	Account            *Account `json:"account"`
	RequiresOpenAIAuth bool     `json:"requiresOpenaiAuth"`
}

type Account struct {
	Type     string `json:"type"`
	Email    string `json:"email"`
	PlanType string `json:"planType"`
}

type ThreadListResponse struct {
	Data            []Thread `json:"data"`
	NextCursor      string   `json:"nextCursor"`
	BackwardsCursor string   `json:"backwardsCursor"`
}

type ThreadReadResponse struct {
	Thread Thread `json:"thread"`
}

type ThreadRollbackResponse struct {
	Thread Thread `json:"thread"`
}

type ThreadTurnsListResponse struct {
	Data            []Turn `json:"data"`
	NextCursor      string `json:"nextCursor"`
	BackwardsCursor string `json:"backwardsCursor"`
}

type ThreadOpenResponse struct {
	Thread          Thread `json:"thread"`
	Model           string `json:"model"`
	ModelProvider   string `json:"modelProvider"`
	ServiceTier     string `json:"serviceTier"`
	Cwd             string `json:"cwd"`
	ReasoningEffort string `json:"reasoningEffort"`
}

type ModelListResponse struct {
	Data       []Model `json:"data"`
	NextCursor string  `json:"nextCursor"`
}

type Model struct {
	ID                     string   `json:"id"`
	Model                  string   `json:"model"`
	DisplayName            string   `json:"displayName"`
	Description            string   `json:"description"`
	Hidden                 bool     `json:"hidden"`
	DefaultReasoningEffort string   `json:"defaultReasoningEffort"`
	InputModalities        []string `json:"inputModalities"`
	SupportsPersonality    bool     `json:"supportsPersonality"`
	IsDefault              bool     `json:"isDefault"`
}

type TurnStartResponse struct {
	Turn Turn `json:"turn"`
}

type TurnSteerResponse struct {
	TurnID string `json:"turnId"`
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

func (r ThreadOpenResponse) HydratedThread() Thread {
	thread := r.Thread
	if r.Cwd != "" {
		thread.Cwd = r.Cwd
	}
	if r.ModelProvider != "" {
		thread.ModelProvider = r.ModelProvider
	}
	if thread.Raw == nil {
		thread.Raw = map[string]any{}
	}
	if r.Model != "" {
		thread.Raw["model"] = r.Model
	}
	if r.ModelProvider != "" {
		thread.Raw["modelProvider"] = r.ModelProvider
	}
	if r.ServiceTier != "" {
		thread.Raw["serviceTier"] = r.ServiceTier
	}
	if r.ReasoningEffort != "" {
		thread.Raw["effort"] = r.ReasoningEffort
	}
	if r.Cwd != "" {
		thread.Raw["cwd"] = r.Cwd
	}
	return thread
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
	ClientID         string           `json:"clientId"`
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
	_ = json.Unmarshal(fields["clientId"], &item.ClientID)
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
