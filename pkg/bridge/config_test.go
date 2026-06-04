package bridge

import "testing"

func TestConfigApplyCommandDefaults(t *testing.T) {
	cfg := Config{DefaultModel: "gpt-5"}
	cfg.ApplyDefaults()
	if cfg.CodexCommand != defaultCodexCommand || cfg.MinVersion != defaultMinVersion || cfg.DefaultModel != "gpt-5" {
		t.Fatalf("unexpected defaulted config: %#v", cfg)
	}

	cfg = Config{CodexCommand: "custom-codex", MinVersion: "1.2.3"}
	cfg.ApplyDefaults()
	if cfg.CodexCommand != "custom-codex" || cfg.MinVersion != "1.2.3" {
		t.Fatalf("existing command defaults should be preserved: %#v", cfg)
	}
}
