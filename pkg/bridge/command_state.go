package bridge

import (
	"context"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/event/cmdschema"
	"maunium.net/go/mautrix/id"
)

var rawCommandArgumentKeys = []string{"arguments", "args", "parameters"}

func codexCommandStateUpdater() bridgev2.ExtraUpdater[*bridgev2.Portal] {
	return func(ctx context.Context, portal *bridgev2.Portal) bool {
		owner, ok := codexCommandOwner(portal)
		if !ok {
			return false
		}
		changed := false
		for _, command := range codexCommandDescriptions() {
			if !syncCodexCommandState(ctx, portal, owner, command) {
				continue
			}
			changed = true
		}
		return changed
	}
}

func codexCommandOwner(portal *bridgev2.Portal) (id.UserID, bool) {
	if portal == nil || portal.MXID == "" || portal.Bridge == nil || portal.Bridge.Bot == nil {
		return "", false
	}
	return portal.Bridge.Bot.GetMXID(), true
}

func syncCodexCommandState(ctx context.Context, portal *bridgev2.Portal, owner id.UserID, command *cmdschema.EventContent) bool {
	stateKey := command.StateKey(owner)
	if _, err := portal.Internal().SendStateWithIntentOrBot(ctx, nil, event.StateMSC4391BotCommand, stateKey, &event.Content{Parsed: command}, time.Now()); err != nil {
		zerolog.Ctx(ctx).Err(err).Str("command", command.Command).Msg("Failed to sync Codex command state")
		return false
	}
	return true
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
				codexCommandParameter("id", cmdschema.PrimitiveTypeString.Schema(), "Approval request ID.", false),
				codexCommandParameter("choice", cmdschema.Enum("approve", "always", "deny"), "Approval choice.", true),
			},
		},
		{
			Command:     "answer",
			Description: event.MakeExtensibleText("Answer a Codex input request."),
			Parameters: []*cmdschema.Parameter{
				codexCommandParameter("id", cmdschema.PrimitiveTypeString.Schema(), "Input request ID.", false),
				codexCommandParameter("answer", cmdschema.PrimitiveTypeString.Schema(), "Answer text.", false),
			},
			TailParam: "answer",
		},
		{
			Command:     "stop",
			Description: event.MakeExtensibleText("Stop the active Codex turn."),
		},
	}
}

func codexCommandParameter(key string, schema *cmdschema.ParameterSchema, description string, optional bool) *cmdschema.Parameter {
	return &cmdschema.Parameter{
		Key:         key,
		Schema:      schema,
		Optional:    optional,
		Description: event.MakeExtensibleText(description),
	}
}

func codexCommandFromRawContent(raw map[string]any) (codexCommand, bool) {
	if nested := nestedRawCodexCommand(raw); len(nested) > 0 {
		if command, ok := codexCommandFromRawContent(nested); ok {
			return command, true
		}
	}
	name := canonicalCodexCommandName(firstString(raw, "command"))
	if name == "" {
		return codexCommand{}, false
	}
	return codexCommandFromRawArgs(name, raw), true
}

func nestedRawCodexCommand(raw map[string]any) map[string]any {
	nested, _ := raw["org.matrix.msc4391.command"].(map[string]any)
	return nested
}

func codexCommandFromRawArgs(name string, raw map[string]any) codexCommand {
	return codexCommandFromRawArgValue(name, rawCommandArguments(raw), raw)
}

func codexCommandFromRawArgValue(name string, value any, raw map[string]any) codexCommand {
	switch args := value.(type) {
	case map[string]any:
		return codexCommandFromArgsMap(name, args)
	case string:
		return codexCommandWithArg(name, args)
	default:
		return codexCommandFromArgsMap(name, raw)
	}
}

func rawCommandArguments(raw map[string]any) any {
	return firstNonNilMapValue(raw, rawCommandArgumentKeys...)
}

func firstNonNilMapValue(values map[string]any, keys ...string) any {
	for _, key := range keys {
		if value := values[key]; value != nil {
			return value
		}
	}
	return nil
}

func codexCommandFromArgsMap(name string, args map[string]any) codexCommand {
	switch name {
	case "approve":
		return codexRequestArgCommand(name, args, "approval_id", "choice")
	case "answer":
		return codexRequestArgCommand(name, args, "request_id", "answer")
	default:
		return codexCommand{name: name}
	}
}

func codexRequestArgCommand(name string, args map[string]any, legacyIDKey, valueKey string) codexCommand {
	return codexCommand{name: name, arg: trimmedSpaceJoin(codexCommandRequestID(args, legacyIDKey), firstString(args, valueKey))}
}

func codexCommandRequestID(args map[string]any, legacyKey string) string {
	return firstString(args, "id", legacyKey)
}
