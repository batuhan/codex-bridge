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
