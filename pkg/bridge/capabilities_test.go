package bridge

import (
	"context"
	"testing"

	"maunium.net/go/mautrix/event"
)

func TestRoomCapabilitiesMatchImplementedHandlers(t *testing.T) {
	caps := (&Client{}).GetCapabilities(context.Background(), nil)
	if caps.Reaction != event.CapLevelUnsupported {
		t.Fatalf("reactions should not be advertised, got %v", caps.Reaction)
	}
	if caps.Reply != event.CapLevelUnsupported {
		t.Fatalf("reply support should not be advertised until ReplyTo is mapped, got %v", caps.Reply)
	}
	if caps.State[event.StateRoomName.Type].Level != event.CapLevelFullySupported {
		t.Fatalf("room name state not advertised: %#v", caps.State)
	}
	if caps.State[event.StateMSC4391BotCommand.Type].Level != event.CapLevelFullySupported {
		t.Fatalf("command state not advertised: %#v", caps.State)
	}
	if caps.State[codexThreadStateType].Level != event.CapLevelFullySupported {
		t.Fatalf("Codex thread state not advertised: %#v", caps.State)
	}
	if caps.State[beeperAIModelStateType].Level != event.CapLevelFullySupported {
		t.Fatalf("Beeper AI model state not advertised: %#v", caps.State)
	}
}

func TestBridgeInfoVersionTracksRoomCapabilities(t *testing.T) {
	info, capabilities := (&Connector{}).GetBridgeInfoVersion()
	if info != bridgeInfoVersion {
		t.Fatalf("bridge info version = %d, want %d", info, bridgeInfoVersion)
	}
	if capabilities != roomCapabilitiesVersion {
		t.Fatalf("room capabilities version = %d, want %d", capabilities, roomCapabilitiesVersion)
	}
	if capabilities < 3 {
		t.Fatal("room capabilities version must stay bumped for command, thread, and AI model state support")
	}
}
