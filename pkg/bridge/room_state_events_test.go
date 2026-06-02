package bridge

import (
	"encoding/json"
	"testing"

	"maunium.net/go/mautrix/event"
)

func TestCustomRoomStateTypesParse(t *testing.T) {
	tests := []struct {
		name    string
		typ     event.Type
		raw     string
		wantTyp any
	}{
		{
			name:    "codex thread",
			typ:     roomStateEventType(codexThreadStateType),
			raw:     `{"threadId":"thread-1","cwd":"/tmp/project","arbitrary":{"kept":true}}`,
			wantTyp: (*rawRoomStateEventContent)(nil),
		},
		{
			name:    "ai model",
			typ:     roomStateEventType(beeperAIModelStateType),
			raw:     `{"model":"openai/gpt-5","name":"GPT-5","reasoning":"high","capabilities":{"tools":true}}`,
			wantTyp: (*aiModelStateEventContent)(nil),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			content := event.Content{VeryRaw: json.RawMessage(tt.raw)}
			if err := content.ParseRaw(tt.typ); err != nil {
				t.Fatal(err)
			}
			if _, ok := content.Parsed.(interface{}); !ok || content.Parsed == nil {
				t.Fatalf("expected parsed content, got %#v", content.Parsed)
			}
			switch tt.wantTyp.(type) {
			case *rawRoomStateEventContent:
				parsed, ok := content.Parsed.(*rawRoomStateEventContent)
				if !ok || (*parsed)["threadId"] != "thread-1" {
					t.Fatalf("unexpected parsed thread state: %#v", content.Parsed)
				}
			case *aiModelStateEventContent:
				parsed, ok := content.Parsed.(*aiModelStateEventContent)
				if !ok || (*parsed)["model"] != "openai/gpt-5" || (*parsed)["reasoning"] != "high" || (*parsed)["capabilities"] == nil {
					t.Fatalf("unexpected parsed model state: %#v", content.Parsed)
				}
			}
		})
	}
}
