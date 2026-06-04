package bridge

import (
	"reflect"

	"maunium.net/go/mautrix/event"
)

type rawRoomStateEventContent map[string]any

type aiModelStateEventContent map[string]any

func init() {
	registerRoomStateEventContent(codexThreadStateType, rawRoomStateEventContent{})
	registerRoomStateEventContent(beeperAIModelStateType, aiModelStateEventContent{})
}

func registerRoomStateEventContent(stateType string, content any) {
	event.TypeMap[roomStateEventType(stateType)] = reflect.TypeOf(content)
}

func roomStateEventType(stateType string) event.Type {
	return event.Type{Type: stateType, Class: event.StateEventType}
}
