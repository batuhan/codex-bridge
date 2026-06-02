package bridge

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/event/cmdschema"
)

func codexCommandStateUpdater() bridgev2.ExtraUpdater[*bridgev2.Portal] {
	return func(ctx context.Context, portal *bridgev2.Portal) bool {
		if portal == nil || portal.MXID == "" || portal.Bridge == nil || portal.Bridge.Bot == nil {
			return false
		}
		changed := false
		owner := portal.Bridge.Bot.GetMXID()
		for _, command := range codexCommandDescriptions() {
			stateKey := command.StateKey(owner)
			if _, err := portal.Internal().SendStateWithIntentOrBot(ctx, nil, event.StateMSC4391BotCommand, stateKey, &event.Content{Parsed: command}, time.Now()); err != nil {
				logFromContext(ctx).Err(err).Str("command", command.Command).Msg("Failed to sync Codex command state")
				continue
			}
			changed = true
		}
		return changed
	}
}

func codexCommandDescriptions() []*cmdschema.EventContent {
	return []*cmdschema.EventContent{
		{
			Command:     "approvals",
			Description: event.MakeExtensibleText("List pending Codex approvals and input requests."),
		},
		{
			Command:     "approve",
			Description: event.MakeExtensibleText("Resolve a pending Codex approval."),
			Parameters: []*cmdschema.Parameter{
				stringCommandParam("id", "Approval request ID.", false),
				{
					Key:         "choice",
					Schema:      cmdschema.Enum("approve", "always", "deny"),
					Optional:    true,
					Description: event.MakeExtensibleText("Approval choice."),
				},
			},
		},
		{
			Command:     "answer",
			Description: event.MakeExtensibleText("Answer a Codex input request."),
			Parameters: []*cmdschema.Parameter{
				stringCommandParam("id", "Input request ID.", false),
				stringCommandParam("answer", "Answer text.", false),
			},
			TailParam: "answer",
		},
		{
			Command:     "stop",
			Aliases:     []string{"abort", "interrupt"},
			Description: event.MakeExtensibleText("Stop the active Codex turn."),
		},
	}
}

func stringCommandParam(key, description string, optional bool) *cmdschema.Parameter {
	return &cmdschema.Parameter{
		Key:         key,
		Schema:      cmdschema.PrimitiveTypeString.Schema(),
		Optional:    optional,
		Description: event.MakeExtensibleText(description),
	}
}

func codexCommandFromMSC4391(input *event.MSC4391BotCommandInput) (codexCommand, bool) {
	if input == nil {
		return codexCommand{}, false
	}
	name := canonicalCodexCommandName(input.Command)
	if name == "" {
		return codexCommand{}, false
	}
	args := map[string]any{}
	if len(input.Arguments) > 0 {
		if err := json.Unmarshal(input.Arguments, &args); err != nil {
			return codexCommand{}, false
		}
	}
	switch name {
	case "approve":
		id := stringsFromCommandArgs(args, "id", "approval_id")
		choice := stringsFromCommandArgs(args, "choice")
		if choice == "" {
			return codexCommand{name: name, arg: id}, true
		}
		return codexCommand{name: name, arg: id + " " + choice}, true
	case "answer":
		return codexCommand{name: name, arg: stringsFromCommandArgs(args, "id", "request_id") + " " + stringsFromCommandArgs(args, "answer")}, true
	default:
		return codexCommand{name: name}, true
	}
}

func stringsFromCommandArgs(args map[string]any, keys ...string) string {
	for _, key := range keys {
		if value, ok := args[key]; ok {
			return fmt.Sprint(value)
		}
	}
	return ""
}
