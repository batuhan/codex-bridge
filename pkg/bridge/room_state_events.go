package bridge

import (
	"reflect"

	"maunium.net/go/mautrix/event"
)

type rawRoomStateEventContent map[string]any

type aiModelStateEventContent map[string]any

func init() {
	registerRoomStateEventContentTypes()
}

func registerRoomStateEventContentTypes() {
	event.TypeMap[roomStateEventType(codexThreadStateType)] = reflect.TypeOf(rawRoomStateEventContent{})
	event.TypeMap[roomStateEventType(beeperAIModelStateType)] = reflect.TypeOf(aiModelStateEventContent{})
}

func roomStateEventType(stateType string) event.Type {
	return event.Type{Type: stateType, Class: event.StateEventType}
}
