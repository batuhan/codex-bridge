package bridge

import (
	"context"
	"strings"
	"testing"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/bridgeconfig"
	"maunium.net/go/mautrix/event"
)

func TestRoomCapabilitiesMatchImplementedHandlers(t *testing.T) {
	caps := (&Client{}).GetCapabilities(context.Background(), nil)
	if caps.ID != roomFeaturesID || caps.GetID() != roomFeaturesID {
		t.Fatalf("room capabilities ID = %q / %q, want %q", caps.ID, caps.GetID(), roomFeaturesID)
	}
	if caps.MaxTextLength != 0 {
		t.Fatalf("Matrix prompt text should not be capped by room capabilities, got %d", caps.MaxTextLength)
	}
	if caps.LocationMessage != event.CapLevelUnsupported {
		t.Fatalf("location messages should not be advertised, got %v", caps.LocationMessage)
	}
	if caps.Poll != event.CapLevelUnsupported {
		t.Fatalf("polls should not be advertised, got %v", caps.Poll)
	}
	if caps.Thread != event.CapLevelUnsupported {
		t.Fatalf("Matrix threads should not be advertised, got %v", caps.Thread)
	}
	if caps.Reaction != event.CapLevelUnsupported {
		t.Fatalf("reactions should not be advertised, got %v", caps.Reaction)
	}
	if caps.ReactionCount != 0 || len(caps.AllowedReactions) != 0 || caps.CustomEmojiReactions {
		t.Fatalf("reaction details should not be advertised, got count=%d allowed=%v custom=%v", caps.ReactionCount, caps.AllowedReactions, caps.CustomEmojiReactions)
	}
	if caps.Reply != event.CapLevelUnsupported {
		t.Fatalf("reply support should not be advertised until ReplyTo is mapped, got %v", caps.Reply)
	}
	if caps.Edit != event.CapLevelUnsupported || caps.EditMaxCount != 0 || caps.EditMaxAge != nil {
		t.Fatalf("edits should not be advertised, got level=%v count=%d age=%v", caps.Edit, caps.EditMaxCount, caps.EditMaxAge)
	}
	if caps.Delete != event.CapLevelUnsupported || caps.DeleteForMe || caps.DeleteMaxAge != nil {
		t.Fatalf("message deletes should not be advertised, got level=%v for_me=%v age=%v", caps.Delete, caps.DeleteForMe, caps.DeleteMaxAge)
	}
	if caps.ReadReceipts || caps.TypingNotifications || caps.Archive || caps.MarkAsUnread || caps.DeleteChat || caps.DeleteChatForEveryone {
		t.Fatalf("unsupported room booleans should stay disabled: %#v", caps)
	}
	if len(caps.Formatting) != 0 || len(caps.File) != 0 || len(caps.MemberActions) != 0 || caps.MessageRequest != nil {
		t.Fatalf("unsupported rich room capabilities should stay empty: %#v", caps)
	}
	if caps.State[event.StateRoomName.Type].Level != event.CapLevelFullySupported {
		t.Fatalf("room name state not advertised: %#v", caps.State)
	}
	if caps.State[event.StateTopic.Type].Level != event.CapLevelFullySupported {
		t.Fatalf("room topic state not advertised: %#v", caps.State)
	}
	if _, ok := caps.State[event.StateRoomAvatar.Type]; ok {
		t.Fatalf("room avatar state should not be advertised: %#v", caps.State)
	}
	if _, ok := caps.State[event.StateBeeperDisappearingTimer.Type]; ok {
		t.Fatalf("disappearing timer state should not be advertised: %#v", caps.State)
	}
	if caps.DisappearingTimer != nil {
		t.Fatalf("disappearing timer capability should not be advertised: %#v", caps.DisappearingTimer)
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

func TestCodexMembersAllowUserEditableRoomState(t *testing.T) {
	members := (&Client{}).codexMembers()
	if members.PowerLevels == nil {
		t.Fatal("expected Codex chat power levels")
	}
	for _, evtType := range []event.Type{
		event.StateRoomName,
		event.StateTopic,
		roomStateEventType(beeperAIModelStateType),
	} {
		if level, ok := members.PowerLevels.Events[evtType]; !ok || level != 0 {
			t.Fatalf("expected %s power level 0, got %d (present=%v)", evtType.Type, level, ok)
		}
	}
	for _, evtType := range []event.Type{
		event.StateMSC4391BotCommand,
		roomStateEventType(codexThreadStateType),
	} {
		if _, ok := members.PowerLevels.Events[evtType]; ok {
			t.Fatalf("%s is bridge-owned state and should not be user-editable: %#v", evtType.Type, members.PowerLevels.Events)
		}
	}
}

func TestCodexMembersRemoveStaleBridgeOwnedPowerLevels(t *testing.T) {
	members := (&Client{}).codexMembers()
	content := &event.PowerLevelsEventContent{
		Events: map[string]int{
			event.StateBeeperDisappearingTimer.Type:       0,
			roomStateEventType(codexThreadStateType).Type: 0,
			event.StateRoomName.Type:                      0,
		},
	}
	if !members.PowerLevels.Apply("", content) {
		t.Fatal("expected stale bridge-owned power levels to be removed")
	}
	if _, ok := content.Events[event.StateBeeperDisappearingTimer.Type]; ok {
		t.Fatalf("disappearing timer power level was not removed: %#v", content.Events)
	}
	if _, ok := content.Events[roomStateEventType(codexThreadStateType).Type]; ok {
		t.Fatalf("Codex thread state power level was not removed: %#v", content.Events)
	}
	if content.Events[event.StateRoomName.Type] != 0 {
		t.Fatalf("room name power level should be preserved: %#v", content.Events)
	}
}

func TestClientBridgeV2InterfacesMatchAdvertisedSupport(t *testing.T) {
	client := any(&Client{})
	mustImplement := []struct {
		name string
		ok   bool
	}{
		{"NetworkAPIWithUserID", implements[bridgev2.NetworkAPIWithUserID](client)},
		{"IdentifierResolvingNetworkAPI", implements[bridgev2.IdentifierResolvingNetworkAPI](client)},
		{"GhostDMCreatingNetworkAPI", implements[bridgev2.GhostDMCreatingNetworkAPI](client)},
		{"ContactListingNetworkAPI", implements[bridgev2.ContactListingNetworkAPI](client)},
		{"UserSearchingNetworkAPI", implements[bridgev2.UserSearchingNetworkAPI](client)},
		{"BackfillingNetworkAPI", implements[bridgev2.BackfillingNetworkAPI](client)},
		{"BackfillingNetworkAPIWithLimits", implements[bridgev2.BackfillingNetworkAPIWithLimits](client)},
		{"RoomNameHandlingNetworkAPI", implements[bridgev2.RoomNameHandlingNetworkAPI](client)},
		{"RoomTopicHandlingNetworkAPI", implements[bridgev2.RoomTopicHandlingNetworkAPI](client)},
		{"RoomStateHandlingNetworkAPI", implements[bridgev2.RoomStateHandlingNetworkAPI](client)},
	}
	for _, iface := range mustImplement {
		if !iface.ok {
			t.Fatalf("Client must implement %s", iface.name)
		}
	}

	mustNotImplement := []struct {
		name string
		ok   bool
	}{
		{"EditHandlingNetworkAPI", implements[bridgev2.EditHandlingNetworkAPI](client)},
		{"PollHandlingNetworkAPI", implements[bridgev2.PollHandlingNetworkAPI](client)},
		{"ReactionHandlingNetworkAPI", implements[bridgev2.ReactionHandlingNetworkAPI](client)},
		{"RedactionHandlingNetworkAPI", implements[bridgev2.RedactionHandlingNetworkAPI](client)},
		{"ReadReceiptHandlingNetworkAPI", implements[bridgev2.ReadReceiptHandlingNetworkAPI](client)},
		{"ChatViewingNetworkAPI", implements[bridgev2.ChatViewingNetworkAPI](client)},
		{"TypingHandlingNetworkAPI", implements[bridgev2.TypingHandlingNetworkAPI](client)},
		{"MarkedUnreadHandlingNetworkAPI", implements[bridgev2.MarkedUnreadHandlingNetworkAPI](client)},
		{"MuteHandlingNetworkAPI", implements[bridgev2.MuteHandlingNetworkAPI](client)},
		{"TagHandlingNetworkAPI", implements[bridgev2.TagHandlingNetworkAPI](client)},
		{"RoomAvatarHandlingNetworkAPI", implements[bridgev2.RoomAvatarHandlingNetworkAPI](client)},
		{"DeleteChatHandlingNetworkAPI", implements[bridgev2.DeleteChatHandlingNetworkAPI](client)},
		{"MessageRequestAcceptingNetworkAPI", implements[bridgev2.MessageRequestAcceptingNetworkAPI](client)},
		{"GroupCreatingNetworkAPI", implements[bridgev2.GroupCreatingNetworkAPI](client)},
		{"PersonalFilteringCustomizingNetworkAPI", implements[bridgev2.PersonalFilteringCustomizingNetworkAPI](client)},
		{"MembershipHandlingNetworkAPI", implements[bridgev2.MembershipHandlingNetworkAPI](client)},
		{"PowerLevelHandlingNetworkAPI", implements[bridgev2.PowerLevelHandlingNetworkAPI](client)},
		{"PushableNetworkAPI", implements[bridgev2.PushableNetworkAPI](client)},
		{"StickerImportingNetworkAPI", implements[bridgev2.StickerImportingNetworkAPI](client)},
		{"DisappearTimerChangingNetworkAPI", implements[bridgev2.DisappearTimerChangingNetworkAPI](client)},
	}
	for _, iface := range mustNotImplement {
		if iface.ok {
			t.Fatalf("Client unexpectedly implements %s; update capabilities and tests before enabling it", iface.name)
		}
	}
}

func implements[T any](value any) bool {
	_, ok := value.(T)
	return ok
}

func TestBridgeInfoVersionTracksRoomCapabilities(t *testing.T) {
	info, capabilities := (&Connector{}).GetBridgeInfoVersion()
	if info != bridgeInfoVersion {
		t.Fatalf("bridge info version = %d, want %d", info, bridgeInfoVersion)
	}
	if capabilities != roomCapabilitiesVersion {
		t.Fatalf("room capabilities version = %d, want %d", capabilities, roomCapabilitiesVersion)
	}
	if capabilities < 8 {
		t.Fatal("room capabilities version must stay bumped for explicit room feature removals")
	}
}

func TestBridgeNameUsesSharedAIAvatar(t *testing.T) {
	name := (&Connector{}).GetName()
	if string(name.NetworkIcon) != defaultCodexAvatarMXC {
		t.Fatalf("network icon = %q, want %q", name.NetworkIcon, defaultCodexAvatarMXC)
	}
}

func TestNetworkCapabilitiesDoNotAdvertiseDisappearingMessages(t *testing.T) {
	caps := (&Connector{}).GetCapabilities()
	if caps == nil || caps.DisappearingMessages {
		t.Fatalf("disappearing messages should not be advertised, got %#v", caps)
	}
	if !caps.AggressiveUpdateInfo {
		t.Fatalf("expected aggressive ghost info refresh support, got %#v", caps)
	}
}

func TestStartRequiresSendStateRequestsForAdvertisedRoomState(t *testing.T) {
	connector := &Connector{Bridge: &bridgev2.Bridge{
		Config: &bridgeconfig.BridgeConfig{},
		Matrix: &streamAndStateMatrix{},
	}}
	err := connector.Start(context.Background())
	if err == nil || !strings.Contains(err.Error(), "enable_send_state_requests") {
		t.Fatalf("expected send-state startup error, got %v", err)
	}
}

type streamAndStateMatrix struct {
	fakeMatrixConnector
}

func (streamAndStateMatrix) GetBeeperStreamPublisher() bridgev2.BeeperStreamPublisher {
	return &recordingBeeperStreamPublisher{}
}
