package appserver

import (
	"encoding/json"
	"testing"
)

func TestThreadUnmarshalPreservesRawMetadata(t *testing.T) {
	var thread Thread
	if err := json.Unmarshal([]byte(`{
		"id":"thread-1",
		"sessionId":"session-1",
		"cwd":"/tmp/project",
		"modelProvider":"openai",
		"status":{"type":"active","activeFlags":["agent"]},
		"gitInfo":{"branch":"main","sha":"abc"},
		"ephemeral":true,
		"turns":[]
	}`), &thread); err != nil {
		t.Fatal(err)
	}
	if thread.ID != "thread-1" || thread.SessionID != "session-1" || thread.Cwd != "/tmp/project" {
		t.Fatalf("structured fields were not decoded: %#v", thread)
	}
	if thread.Raw["gitInfo"] == nil || thread.Raw["status"] == nil || thread.Raw["ephemeral"] != true {
		t.Fatalf("raw thread metadata was not preserved: %#v", thread.Raw)
	}
}

func TestThreadOpenResponseHydratesTopLevelSettings(t *testing.T) {
	var response ThreadOpenResponse
	if err := json.Unmarshal([]byte(`{
		"thread":{"id":"thread-1","sessionId":"session-1","modelProvider":"openai"},
		"model":"gpt-5",
		"modelProvider":"openai",
		"serviceTier":"priority",
		"cwd":"/tmp/project",
		"reasoningEffort":"high"
	}`), &response); err != nil {
		t.Fatal(err)
	}
	thread := response.HydratedThread()
	if thread.Cwd != "/tmp/project" || thread.ModelProvider != "openai" {
		t.Fatalf("top-level thread fields were not hydrated: %#v", thread)
	}
	if thread.Raw["model"] != "gpt-5" || thread.Raw["serviceTier"] != "priority" || thread.Raw["effort"] != "high" {
		t.Fatalf("top-level settings were not preserved in raw state: %#v", thread.Raw)
	}
}

func TestThreadTurnsListResponseDecodesFullItems(t *testing.T) {
	var response ThreadTurnsListResponse
	if err := json.Unmarshal([]byte(`{
		"data":[{
			"id":"turn-1",
			"status":"completed",
			"items":[{"id":"item-1","clientId":"$event:example.com","type":"userMessage","content":[{"type":"text","text":"hello"}]}],
			"itemsView":"full",
			"startedAt":100,
			"completedAt":101
		}],
		"nextCursor":"next",
		"backwardsCursor":"back"
	}`), &response); err != nil {
		t.Fatal(err)
	}
	if response.NextCursor != "next" || response.BackwardsCursor != "back" || len(response.Data) != 1 {
		t.Fatalf("thread turns page decoded incorrectly: %#v", response)
	}
	item := response.Data[0].Items[0]
	if item.ClientID != "$event:example.com" || item.Content[0].Text != "hello" {
		t.Fatalf("turn item decoded incorrectly: %#v", item)
	}
}
