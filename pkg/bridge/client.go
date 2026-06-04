package bridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	aistream "github.com/beeper/ai-bridge/pkg/ai-stream"
	"github.com/rs/zerolog"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"maunium.net/go/mautrix/bridgev2/status"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"github.com/beeper/codex-bridge/pkg/appserver"
)

type Client struct {
	Main                    *Connector
	UserLogin               *bridgev2.UserLogin
	loggedIn                bool
	activeStreamJanitorMu   sync.Mutex
	activeStreamJanitorStop context.CancelFunc
}

const newProjectPrompt = "Send a project directory path to start Codex."

const newProjectIntroText = "Send a project directory path to start a Codex session.\n\n" +
	"You can send an absolute path like `/Users/batuhan/projects/codex-bridge`, or a home-relative path like `~/projects/codex-bridge`. Your first path message only picks the project; it is not sent to Codex as a prompt. I'll verify the directory, create or resume a Codex session there, import recent Codex context as backfill, and turn this room into that project chat.\n\n" +
	"Once the project is connected, just message normally. Codex responses stream into the room as Beeper AI parts, long final output is attached as a file when needed, and the contact list will show recent project directories.\n\n" +
	"If Codex needs approval or input, use `/approvals` to see pending requests, `/approve <id> approve|always|deny` to respond to approvals, `/answer <id> <text>` for input requests, and `/stop` to interrupt the active turn."

const (
	defaultThreadListLimit = 100
	threadTurnsListLimit   = 100
	modelListLimit         = 200

	threadListSortKey       = "updated_at"
	sortDirectionAscending  = "asc"
	sortDirectionDescending = "desc"
	threadTurnsItemsView    = "full"
)

var _ bridgev2.NetworkAPI = (*Client)(nil)
var _ bridgev2.NetworkAPIWithUserID = (*Client)(nil)
var _ bridgev2.IdentifierResolvingNetworkAPI = (*Client)(nil)
var _ bridgev2.GhostDMCreatingNetworkAPI = (*Client)(nil)
var _ bridgev2.ContactListingNetworkAPI = (*Client)(nil)
var _ bridgev2.UserSearchingNetworkAPI = (*Client)(nil)
var _ bridgev2.BackfillingNetworkAPI = (*Client)(nil)
var _ bridgev2.BackfillingNetworkAPIWithLimits = (*Client)(nil)
var _ bridgev2.EditHandlingNetworkAPI = (*Client)(nil)
var _ bridgev2.DeleteChatHandlingNetworkAPI = (*Client)(nil)
var _ bridgev2.RoomNameHandlingNetworkAPI = (*Client)(nil)
var _ bridgev2.RoomTopicHandlingNetworkAPI = (*Client)(nil)
var _ bridgev2.RoomStateHandlingNetworkAPI = (*Client)(nil)
var _ status.BridgeStateFiller = (*Client)(nil)

func (cl *Client) Connect(ctx context.Context) {
	cl.loggedIn = true
	if cl.UserLogin != nil && cl.UserLogin.BridgeState != nil {
		cl.UserLogin.BridgeState.Send(status.BridgeState{StateEvent: status.StateConnected})
	}
	if cl.Main != nil && cl.UserLogin != nil {
		syncCtx, syncCancel := context.WithTimeout(context.Background(), bridgeStartupSyncTimeout)
		cl.Main.reconcileLoginPortals(syncCtx, cl.UserLogin.ID)
		syncCancel()
	}
	recoveryCtx, cancel := context.WithTimeout(context.Background(), activeStreamRecoveryTimeout)
	defer cancel()
	cl.failPersistedActiveStreams(recoveryCtx)
	cl.startActiveStreamJanitor()
}

func (cl *Client) Disconnect() {
	cl.loggedIn = false
	cl.stopActiveStreamJanitor()
}

func (cl *Client) IsLoggedIn() bool {
	return cl.loggedIn
}

func (cl *Client) LogoutRemote(ctx context.Context) {
	cl.loggedIn = false
	cl.stopActiveStreamJanitor()
	if cl.UserLogin != nil && cl.UserLogin.BridgeState != nil {
		cl.UserLogin.BridgeState.Send(status.BridgeState{StateEvent: status.StateLoggedOut})
	}
}

func (cl *Client) FillBridgeState(state status.BridgeState) status.BridgeState {
	if cl == nil || cl.Main == nil {
		return state
	}
	codexState := cl.Main.globalBridgeInfo()
	if len(codexState) == 0 {
		return state
	}
	if state.Info == nil {
		state.Info = map[string]any{}
	}
	state.Info["codex"] = codexState
	return state
}

func (cl *Client) GetUserID() networkid.UserID {
	return loginUserID(cl.UserLogin.ID)
}

func (cl *Client) IsThisUser(ctx context.Context, userID networkid.UserID) bool {
	return userID == cl.GetUserID()
}

func (cl *Client) GetChatInfo(ctx context.Context, portal *bridgev2.Portal) (*bridgev2.ChatInfo, error) {
	meta := portalMetadata(nil)
	if portal != nil {
		meta = portalMetadata(portal.Metadata)
		meta = cl.hydratePortalThreadMetadata(ctx, portal, meta)
		if meta.ThreadID != "" && cl.Main != nil {
			cl.Main.rememberThreadRoom(meta.ThreadID, cl, portal.PortalKey, meta.Cwd, nil)
		}
	}
	name := "New Project"
	var state map[string]any
	switch {
	case meta.ThreadID != "":
		thread, err := cl.readThread(ctx, meta.ThreadID, false)
		if err != nil {
			if meta.Cwd != "" {
				name = directoryName(meta.Cwd)
			}
			if isThreadNotFoundError(err) {
				cl.clearMissingThread(ctx, portal, meta)
				if portal != nil {
					meta = portalMetadata(portal.Metadata)
				}
			}
		}
		if err == nil {
			name = threadName(thread)
			state = enrichThreadStateWithModelState(codexThreadInitialState(thread), cl.roomAIModelState(ctx, portal))
			if cl.Main != nil {
				cl.Main.rememberThreadRoom(thread.ID, cl, portal.PortalKey, thread.Cwd, state)
			}
		}
	case meta.Cwd != "":
		name = directoryName(meta.Cwd)
	}
	if meta.ThreadID == "" && meta.Cwd == "" {
		info := cl.newProjectChatInfo()
		applyStoredPortalInfo(info, portal)
		return info, nil
	}
	info := portalInfo(name, cl.codexMembers(), meta.Cwd, meta.ThreadID, state)
	applyStoredPortalInfo(info, portal)
	return info, nil
}

func (cl *Client) GetUserInfo(ctx context.Context, ghost *bridgev2.Ghost) (*bridgev2.UserInfo, error) {
	if ghost == nil {
		return nil, fmt.Errorf("missing ghost")
	}
	switch ghost.ID {
	case codexUserID:
		return codexUserInfo("Codex", false), nil
	default:
		if cwd, ok := parseProjectUserID(ghost.ID); ok {
			return codexUserInfo(cwd, false, cwd), nil
		}
		if strings.HasPrefix(string(ghost.ID), "login:") {
			return loginUserInfo(), nil
		}
		return nil, fmt.Errorf("unknown Codex ghost %s", ghost.ID)
	}
}

func (cl *Client) GetCapabilities(ctx context.Context, portal *bridgev2.Portal) *event.RoomFeatures {
	caps := &event.RoomFeatures{
		ID:                  roomFeaturesID,
		State:               codexRoomStateCapabilities(),
		TypingNotifications: true,
		Edit:                event.CapLevelFullySupported,
		DeleteChat:          true,
	}
	if isReadOnlyPortal(portal) {
		applyReadOnlyRoomCapabilities(caps)
	}
	return caps
}

func codexRoomStateCapabilities() event.StateFeatureMap {
	return event.StateFeatureMap{
		event.StateRoomName.Type: {Level: event.CapLevelFullySupported},
		event.StateTopic.Type:    {Level: event.CapLevelFullySupported},
		beeperAIModelStateType:   {Level: event.CapLevelFullySupported},
	}
}

func applyReadOnlyRoomCapabilities(caps *event.RoomFeatures) {
	caps.TypingNotifications = false
	caps.Edit = event.CapLevelUnsupported
}

func (cl *Client) HandleMatrixMessage(ctx context.Context, msg *bridgev2.MatrixMessage) (*bridgev2.MatrixMessageResponse, error) {
	resp, err := cl.handleMatrixMessage(ctx, msg)
	if err != nil {
		cl.logMatrixMessageError(msg, err, "Codex Matrix message failed")
		err = matrixMessageStatusForCodexError(err)
	}
	return resp, err
}

func (cl *Client) handleMatrixMessage(ctx context.Context, msg *bridgev2.MatrixMessage) (*bridgev2.MatrixMessageResponse, error) {
	if msg == nil || msg.Portal == nil || msg.Content == nil {
		return nil, fmt.Errorf("missing Matrix message")
	}
	log := zerolog.Ctx(ctx).With().
		Str("portal_id", string(msg.Portal.ID)).
		Str("portal_receiver", string(msg.Portal.Receiver)).
		Str("event_id", string(msg.Event.ID)).
		Int("body_len", len(msg.Content.Body)).
		Logger()
	if cl.UserLogin != nil {
		log = log.With().Str("login_id", string(cl.UserLogin.ID)).Logger()
	}
	if isReadOnlyPortal(msg.Portal) {
		return nil, fmt.Errorf("%w: Codex subagent rooms are read-only", bridgev2.ErrUnsupportedMessageType)
	}
	if response, handled, err := cl.handleBridgeCommand(ctx, msg); handled {
		if err != nil {
			log.Err(err).Msg("Failed to handle Codex bridge command")
		}
		return response, err
	}
	if msg.ThreadRoot != nil || msg.ReplyTo != nil {
		return nil, fmt.Errorf("%w: replies and Matrix threads are not supported by Codex prompts", bridgev2.ErrUnsupportedMessageType)
	}
	if err := matrixPromptContentError(msg.Content); err != nil {
		return nil, err
	}
	meta := portalMetadata(msg.Portal.Metadata)
	meta = cl.hydratePortalThreadMetadata(ctx, msg.Portal, meta)
	log.Debug().Str("thread_id", meta.ThreadID).Str("cwd", meta.Cwd).Msg("Handling Codex Matrix message")
	startedThread := false
	directorySelection := false
	if meta.ThreadID == "" {
		cwd, consumedBody := starterThreadCWD(meta, msg.Portal, msg.Content.Body)
		if cwd == "" {
			cl.queueCommandNotice(msg.Portal, "", newProjectPrompt)
			return cl.commandHandledResponse(msg, "directory_prompt"), nil
		}
		if consumedBody {
			cleaned, err := cleanProjectDir(cwd)
			if err != nil {
				cl.queueCommandNotice(msg.Portal, "", "That is not an available project directory.\n\n"+err.Error()+"\n\n"+newProjectPrompt)
				return cl.commandHandledResponse(msg, "directory_error"), nil
			}
			cwd = cleaned
			meta.Cwd = cwd
			directorySelection = true
		} else {
			thread, err := cl.startThreadForPortal(ctx, cwd, msg.Portal)
			if err != nil {
				log.Err(err).Str("cwd", cwd).Msg("Failed to start Codex thread for Matrix message")
				return nil, err
			}
			meta.ThreadID = thread.ID
			meta.Cwd = thread.Cwd
			if synced := cl.syncThreadPortal(ctx, msg.Portal, thread); synced != nil {
				msg.Portal = synced
				meta = portalMetadata(msg.Portal.Metadata)
			}
			startedThread = true
		}
	}
	prompt := matrixPromptText(msg.Content.Body)
	timestamp := matrixMessageTimestamp(msg)
	userDB := &database.Message{
		ID:        networkid.MessageID("user:" + string(msg.Event.ID)),
		PartID:    partID("text"),
		Room:      msg.Portal.PortalKey,
		SenderID:  cl.GetUserID(),
		Timestamp: timestamp,
		Metadata:  &MessageMetadata{Role: "user", ThreadID: meta.ThreadID, StreamStatus: "done"},
	}
	if directorySelection {
		userDB.Metadata = &MessageMetadata{Role: "command", ThreadID: meta.ThreadID, StreamStatus: "directory"}
		resp := matrixMessageResponse(userDB)
		startKey := matrixStartKey(cl.UserLogin.ID, msg.Portal.PortalKey, msg.Event.ID)
		if cl.Main != nil && cl.Main.claimMatrixStart(startKey) {
			cwd := meta.Cwd
			resp.PostSave = func(ctx context.Context, saved *database.Message) {
				go cl.startDirectorySelection(startKey, msg.Portal, cwd, saved.ID, saved.PartID)
			}
		}
		log.Debug().Str("thread_id", meta.ThreadID).Str("cwd", meta.Cwd).Msg("Accepted Codex directory selection")
		return resp, nil
	}
	clientUserMessageID := string(msg.Event.ID)
	resp := matrixMessageResponse(userDB)
	if active := cl.Main.activeRun(meta.ThreadID); active != nil {
		ensureMessageMetadata(userDB).TurnID = active.turnID
		startKey := matrixStartKey(cl.UserLogin.ID, msg.Portal.PortalKey, msg.Event.ID)
		if cl.Main.claimMatrixStart(startKey) {
			threadID := meta.ThreadID
			turnID := active.turnID
			resp.PostSave = func(ctx context.Context, saved *database.Message) {
				go cl.steerMatrixTurn(startKey, msg.Portal, threadID, turnID, clientUserMessageID, prompt)
			}
		}
	} else {
		metaCopy := *meta
		startKey := matrixStartKey(cl.UserLogin.ID, msg.Portal.PortalKey, msg.Event.ID)
		if cl.Main.claimMatrixStart(startKey) {
			resp.PostSave = func(ctx context.Context, saved *database.Message) {
				messageID := userDB.ID
				part := userDB.PartID
				if saved != nil {
					messageID = saved.ID
					part = saved.PartID
				}
				go cl.startMatrixTurn(startKey, msg.Portal, &metaCopy, startedThread, clientUserMessageID, prompt, messageID, part)
			}
		}
	}
	log.Debug().Str("thread_id", meta.ThreadID).Msg("Accepted Codex Matrix message")
	return resp, nil
}

func matrixMessageResponse(msg *database.Message) *bridgev2.MatrixMessageResponse {
	if msg == nil {
		return &bridgev2.MatrixMessageResponse{}
	}
	return &bridgev2.MatrixMessageResponse{DB: msg, StreamOrder: msg.Timestamp.UnixNano()}
}

func (cl *Client) startDirectorySelection(startKey string, portal *bridgev2.Portal, cwd string, messageID networkid.MessageID, part networkid.PartID) {
	defer cl.Main.finishMatrixStart(startKey)
	ctx := context.Background()
	if portal = cl.freshPortal(ctx, portal); portal == nil {
		zerolog.Ctx(ctx).Err(errors.New("missing Codex portal")).Str("cwd", cwd).Msg("Failed to start Codex directory selection")
		return
	}
	thread, err := cl.startThreadForPortal(ctx, cwd, portal)
	if err != nil {
		zerolog.Ctx(ctx).Err(err).Str("cwd", cwd).Msg("Failed to start Codex session from directory selection")
		cl.queueCommandNotice(portal, "", "Failed to start Codex session in "+cwd+":\n\n"+err.Error())
		return
	}
	if synced := cl.syncThreadPortal(ctx, portal, thread); synced != nil {
		portal = synced
	}
	cl.updateStoredDirectorySelectionMetadata(ctx, portal.PortalKey.Receiver, messageID, part, portal.PortalKey, thread.ID)
	cl.queueCommandNotice(portal, thread.ID, "Started Codex session in "+thread.Cwd+".")
}

func (cl *Client) HandleMatrixEdit(ctx context.Context, msg *bridgev2.MatrixEdit) error {
	if err := cl.handleMatrixEdit(ctx, msg); err != nil {
		cl.logMatrixMessageError(&bridgev2.MatrixMessage{MatrixEventBase: msg.MatrixEventBase}, err, "Codex Matrix edit failed")
		return matrixMessageStatusForCodexError(err)
	}
	return nil
}

func (cl *Client) HandleMatrixDeleteChat(ctx context.Context, msg *bridgev2.MatrixDeleteChat) error {
	if msg == nil || msg.Portal == nil {
		return fmt.Errorf("missing Codex chat delete")
	}
	if cl == nil || cl.Main == nil {
		return nil
	}
	meta := portalMetadata(msg.Portal.Metadata)
	if meta.ThreadID != "" {
		cl.Main.stopThreadBridging(ctx, meta.ThreadID)
	}
	zerolog.Ctx(ctx).Info().
		Stringer("portal_key", msg.Portal.PortalKey).
		Str("thread_id", meta.ThreadID).
		Str("cwd", meta.Cwd).
		Msg("Detached Codex chat after Matrix chat delete")
	return nil
}

func (cl *Client) handleMatrixEdit(ctx context.Context, msg *bridgev2.MatrixEdit) error {
	if msg == nil || msg.Portal == nil || msg.Content == nil || msg.EditTarget == nil {
		return fmt.Errorf("missing Matrix edit")
	}
	if cl == nil || cl.Main == nil || cl.UserLogin == nil {
		return fmt.Errorf("missing Codex bridge login")
	}
	if isReadOnlyPortal(msg.Portal) {
		return fmt.Errorf("%w: Codex subagent rooms are read-only", bridgev2.ErrUnsupportedMessageType)
	}
	if err := matrixPromptContentError(msg.Content); err != nil {
		return err
	}
	meta, _ := msg.EditTarget.Metadata.(*MessageMetadata)
	if meta == nil || meta.Role != "user" || meta.ThreadID == "" || meta.TurnID == "" {
		return fmt.Errorf("%w: only previously sent Codex prompts can be edited", bridgev2.ErrUnsupportedMessageType)
	}
	prompt := matrixPromptText(msg.Content.Body)
	if err := cl.prepareMatrixTurnRewrite(ctx, msg.Portal, msg.EditTarget); err != nil {
		return err
	}
	target := *msg.EditTarget
	targetMeta := *meta
	targetMeta.StreamStatus = "edited"
	target.Metadata = &targetMeta
	*msg.EditTarget = target

	startKey := matrixStartKey(cl.UserLogin.ID, msg.Portal.PortalKey, msg.Event.ID)
	if cl.Main != nil && cl.Main.claimMatrixStart(startKey) {
		go cl.startMatrixTurnFromEdit(startKey, msg.Portal, &target, string(msg.Event.ID), prompt)
	}
	return nil
}

func matrixStartKey(loginID networkid.UserLoginID, portalKey networkid.PortalKey, eventID id.EventID) string {
	return string(loginID) + "\x00" + string(portalKey.ID) + "\x00" + string(portalKey.Receiver) + "\x00" + string(eventID)
}

func matrixMessageTimestamp(msg *bridgev2.MatrixMessage) time.Time {
	if msg == nil || msg.Event == nil || msg.Event.Timestamp == 0 {
		return time.Now()
	}
	return time.UnixMilli(msg.Event.Timestamp)
}

func matrixPromptText(body string) string {
	return firstTrimmedNonEmpty(body, "Continue.")
}

func matrixPromptContentError(content *event.MessageEventContent) error {
	switch content.MsgType {
	case "", event.MsgText:
		return nil
	case event.MsgNotice:
		return bridgev2.ErrIgnoringMNotice
	default:
		return fmt.Errorf("%w: %s", bridgev2.ErrUnsupportedMessageType, content.MsgType)
	}
}

func (cl *Client) steerMatrixTurn(startKey string, portal *bridgev2.Portal, threadID, turnID, clientUserMessageID, prompt string) {
	defer cl.Main.finishMatrixStart(startKey)
	ctx := context.Background()
	if err := cl.steerTurn(ctx, threadID, turnID, clientUserMessageID, prompt); err != nil {
		zerolog.Ctx(ctx).Err(err).Str("thread_id", threadID).Str("turn_id", turnID).Msg("Failed to steer Codex turn from Matrix message")
		cl.queueAsyncTurnFailure(ctx, portal, threadID, "Failed to send that message to Codex:\n\n"+err.Error())
	}
}

func (cl *Client) startMatrixTurn(startKey string, portal *bridgev2.Portal, meta *PortalMetadata, startedThread bool, clientUserMessageID, prompt string, messageID networkid.MessageID, part networkid.PartID) {
	defer cl.Main.finishMatrixStart(startKey)
	ctx := context.Background()
	if meta == nil {
		meta = &PortalMetadata{}
	}
	if portal = cl.freshPortal(ctx, portal); portal == nil {
		zerolog.Ctx(ctx).Err(errors.New("missing Codex portal")).Str("thread_id", meta.ThreadID).Msg("Failed to start Codex turn from Matrix message")
		return
	}
	threadID := meta.ThreadID
	if !startedThread && !cl.Main.isWarmThread(meta.ThreadID) {
		thread, err := cl.resumeThreadForPortal(ctx, portal, meta)
		if isThreadNotFoundError(err) {
			oldThreadID := meta.ThreadID
			thread, err = cl.replaceMissingThread(ctx, portal, meta)
			if err == nil {
				zerolog.Ctx(ctx).Warn().Str("old_thread_id", oldThreadID).Str("thread_id", thread.ID).Msg("Replaced missing Codex thread before Matrix turn")
			}
		}
		if err != nil {
			zerolog.Ctx(ctx).Err(err).Str("thread_id", meta.ThreadID).Msg("Failed to resume Codex thread for Matrix message")
			cl.queueAsyncTurnFailure(ctx, portal, meta.ThreadID, "Failed to resume the Codex session:\n\n"+err.Error())
			return
		}
		threadID = thread.ID
		meta.ThreadID = thread.ID
		meta.Cwd = thread.Cwd
		if synced := cl.syncThreadPortal(ctx, portal, thread); synced != nil {
			portal = synced
		}
		cl.updateStoredUserMessageMetadata(ctx, portal.PortalKey.Receiver, messageID, part, threadID, "")
	}
	run, err := cl.startTurn(ctx, portal.PortalKey, threadID, clientUserMessageID, prompt)
	if err != nil && isThreadNotFoundError(err) {
		cl.Main.forgetWarmThread(threadID)
		oldThreadID := threadID
		thread, recoverErr := cl.resumeThreadForPortal(ctx, portal, meta)
		if isThreadNotFoundError(recoverErr) {
			thread, recoverErr = cl.replaceMissingThread(ctx, portal, meta)
		}
		if recoverErr != nil {
			err = recoverErr
		}
		if recoverErr == nil {
			meta.ThreadID = thread.ID
			meta.Cwd = thread.Cwd
			threadID = thread.ID
			if synced := cl.syncThreadPortal(ctx, portal, thread); synced != nil {
				portal = synced
			}
			cl.updateStoredUserMessageMetadata(ctx, portal.PortalKey.Receiver, messageID, part, threadID, "")
			run, err = cl.startTurn(ctx, portal.PortalKey, threadID, clientUserMessageID, prompt)
			if err == nil {
				zerolog.Ctx(ctx).Warn().Str("old_thread_id", oldThreadID).Str("thread_id", threadID).Msg("Recovered missing Codex thread and retried Matrix message")
			}
		}
	}
	if err != nil {
		zerolog.Ctx(ctx).Err(err).Str("thread_id", threadID).Msg("Failed to start Codex turn from Matrix message")
		cl.queueAsyncTurnFailure(ctx, portal, threadID, "Failed to start the Codex response:\n\n"+err.Error())
		return
	}
	if run != nil {
		cl.updateStoredUserMessageMetadata(ctx, portal.PortalKey.Receiver, messageID, part, threadID, run.turnID)
	}
}

func (cl *Client) prepareMatrixTurnRewrite(ctx context.Context, portal *bridgev2.Portal, target *database.Message) error {
	if portal == nil || target == nil {
		return fmt.Errorf("missing Codex edit target")
	}
	meta, _ := target.Metadata.(*MessageMetadata)
	if meta == nil || meta.ThreadID == "" || meta.TurnID == "" {
		return fmt.Errorf("missing Codex edit metadata")
	}
	threadID := meta.ThreadID
	if active := cl.Main.activeRun(threadID); active != nil {
		if err := cl.Main.request(ctx, "turn/interrupt", turnInterruptParams(threadID, active.turnID), nil); err != nil {
			return fmt.Errorf("failed to stop active Codex turn before edit: %w", err)
		}
		active.writeCodexClientRequestState("turn/interrupt", codexClientTurnRequestState(threadID, active.turnID, "", ""))
		cl.Main.setActive(threadID, nil)
	}
	if err := cl.rollbackThreadToTurn(ctx, threadID, meta.TurnID); err != nil {
		return fmt.Errorf("failed to roll back Codex thread for edit: %w", err)
	}
	if err := cl.redactAndDeleteMessagesAfter(ctx, portal, target); err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).Str("thread_id", threadID).Msg("Failed to remove stale messages after Codex edit")
	}
	return nil
}

func (cl *Client) startMatrixTurnFromEdit(startKey string, portal *bridgev2.Portal, target *database.Message, clientUserMessageID, prompt string) {
	defer cl.Main.finishMatrixStart(startKey)
	ctx := context.Background()
	if portal = cl.freshPortal(ctx, portal); portal == nil || target == nil {
		return
	}
	meta, _ := target.Metadata.(*MessageMetadata)
	if meta == nil || meta.ThreadID == "" {
		return
	}
	threadID := meta.ThreadID
	run, err := cl.startTurn(ctx, portal.PortalKey, threadID, clientUserMessageID, prompt)
	if err != nil {
		zerolog.Ctx(ctx).Err(err).Str("thread_id", threadID).Msg("Failed to start Codex turn from Matrix edit")
		cl.queueAsyncTurnFailure(ctx, portal, threadID, "Failed to start the replacement Codex response:\n\n"+err.Error())
		return
	}
	if run != nil {
		cl.updateStoredUserMessageMetadata(ctx, portal.PortalKey.Receiver, target.ID, target.PartID, threadID, run.turnID)
	}
}

func (cl *Client) rollbackThreadToTurn(ctx context.Context, threadID, turnID string) error {
	thread, err := cl.readThread(ctx, threadID, true)
	if err != nil {
		return err
	}
	targetIndex := -1
	for i, turn := range thread.Turns {
		if turn.ID == turnID {
			targetIndex = i
			break
		}
	}
	if targetIndex < 0 {
		return fmt.Errorf("edited turn %s was not found in Codex history", turnID)
	}
	numTurns := len(thread.Turns) - targetIndex
	return cl.Main.request(ctx, "thread/rollback", threadRollbackParams(threadID, numTurns), &appserver.ThreadRollbackResponse{})
}

func (cl *Client) redactAndDeleteMessagesAfter(ctx context.Context, portal *bridgev2.Portal, target *database.Message) error {
	if cl == nil || cl.Main == nil || cl.Main.Bridge == nil || cl.Main.Bridge.DB == nil || portal == nil || target == nil {
		return nil
	}
	count, err := cl.Main.Bridge.DB.Message.CountMessagesInPortal(ctx, portal.PortalKey)
	if err != nil {
		return err
	}
	messages, err := cl.Main.Bridge.DB.Message.GetLastNInPortal(ctx, portal.PortalKey, count)
	if err != nil {
		return err
	}
	seenMXID := map[id.EventID]bool{}
	for _, msg := range messages {
		if msg == nil || msg.RowID <= target.RowID {
			continue
		}
		if msg.MXID != "" && !strings.HasPrefix(string(msg.MXID), "~fake:") && !seenMXID[msg.MXID] {
			seenMXID[msg.MXID] = true
			if err := cl.redactBridgeMessage(ctx, portal, msg.MXID); err != nil {
				return err
			}
		}
		if meta, _ := msg.Metadata.(*MessageMetadata); meta != nil && meta.TurnID != "" && cl.Main.Store != nil && cl.UserLogin != nil {
			if err := cl.Main.Store.DeleteActiveStream(ctx, cl.UserLogin.ID, meta.TurnID); err != nil {
				zerolog.Ctx(ctx).Warn().Err(err).Str("turn_id", meta.TurnID).Msg("Failed to delete stale Codex active stream record after edit")
			}
		}
		if err := cl.Main.Bridge.DB.Message.Delete(ctx, msg.RowID); err != nil {
			return err
		}
	}
	return nil
}

func (cl *Client) redactBridgeMessage(ctx context.Context, portal *bridgev2.Portal, eventID id.EventID) error {
	if cl == nil || cl.Main == nil || cl.Main.Bridge == nil || cl.Main.Bridge.Bot == nil || portal == nil || portal.MXID == "" || eventID == "" {
		return nil
	}
	_, err := cl.Main.Bridge.Bot.SendMessage(ctx, portal.MXID, event.EventRedaction, &event.Content{
		Parsed: &event.RedactionEventContent{
			Redacts: eventID,
			Reason:  "Codex prompt edited",
		},
	}, nil)
	return err
}

func (cl *Client) freshPortal(ctx context.Context, portal *bridgev2.Portal) *bridgev2.Portal {
	if portal == nil || cl == nil || cl.Main == nil || cl.Main.Bridge == nil {
		return portal
	}
	fresh, err := cl.Main.Bridge.GetExistingPortalByKey(ctx, portal.PortalKey)
	if err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).Stringer("portal_key", portal.PortalKey).Msg("Failed to reload Codex portal")
		return portal
	}
	if fresh == nil {
		return portal
	}
	return fresh
}

func (cl *Client) updateStoredUserMessageMetadata(ctx context.Context, receiver networkid.UserLoginID, messageID networkid.MessageID, part networkid.PartID, threadID, turnID string) {
	cl.updateStoredMessagePart(ctx, receiver, messageID, part, "Failed to update Codex user message metadata", "Codex user message row was not stored before metadata update timeout", func(msg *database.Message) {
		applyStoredUserMessageMetadata(msg, threadID, turnID)
	})
}

func (cl *Client) updateStoredDirectorySelectionMetadata(ctx context.Context, receiver networkid.UserLoginID, messageID networkid.MessageID, part networkid.PartID, portalKey networkid.PortalKey, threadID string) {
	cl.updateStoredMessagePart(ctx, receiver, messageID, part, "Failed to update Codex directory selection metadata", "Codex directory selection row was not stored before metadata update timeout", func(msg *database.Message) {
		applyStoredDirectorySelectionMetadata(msg, portalKey, threadID)
	})
}

func applyStoredUserMessageMetadata(msg *database.Message, threadID, turnID string) {
	meta := ensureMessageMetadata(msg)
	setNonEmptyStringField(&meta.ThreadID, threadID)
	setNonEmptyStringField(&meta.TurnID, turnID)
}

func applyStoredDirectorySelectionMetadata(msg *database.Message, portalKey networkid.PortalKey, threadID string) {
	msg.Room = portalKey
	meta := ensureMessageMetadata(msg)
	meta.Role = "command"
	meta.ThreadID = threadID
	meta.StreamStatus = "directory"
}

func ensureMessageMetadata(msg *database.Message) *MessageMetadata {
	meta, ok := messageMetadata(msg.Metadata)
	if !ok {
		meta = &MessageMetadata{}
	}
	msg.Metadata = meta
	return meta
}

func (cl *Client) updateStoredMessagePart(ctx context.Context, receiver networkid.UserLoginID, messageID networkid.MessageID, part networkid.PartID, updateFailure, timeoutMessage string, update func(*database.Message)) {
	if cl == nil || cl.Main == nil || cl.Main.Bridge == nil || cl.Main.Bridge.DB == nil || messageID == "" {
		return
	}
	for attempt := 0; attempt < 40; attempt++ {
		msg, err := cl.Main.Bridge.DB.Message.GetPartByID(ctx, receiver, messageID, part)
		if err == nil && msg != nil {
			update(msg)
			if err = cl.Main.Bridge.DB.Message.Update(ctx, msg); err != nil {
				zerolog.Ctx(ctx).Warn().Err(err).Str("message_id", string(messageID)).Msg(updateFailure)
			}
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(50 * time.Millisecond):
		}
	}
	zerolog.Ctx(ctx).Debug().Str("message_id", string(messageID)).Msg(timeoutMessage)
}

func (cl *Client) queueAsyncTurnFailure(ctx context.Context, portal *bridgev2.Portal, threadID, text string) {
	portal = cl.freshPortal(ctx, portal)
	if portal == nil {
		return
	}
	cl.queueCommandNotice(portal, threadID, text)
}

func (cl *Client) HandleMatrixRoomName(ctx context.Context, msg *bridgev2.MatrixRoomName) (bool, error) {
	if msg == nil || msg.Portal == nil || msg.Content == nil {
		return false, nil
	}
	name := firstTrimmedNonEmpty(msg.Content.Name)
	meta := portalMetadata(msg.Portal.Metadata)
	if meta.ThreadID != "" {
		if err := cl.Main.request(ctx, "thread/name/set", threadNameSetParams(meta.ThreadID, name), nil); err != nil {
			return false, err
		}
	}
	msg.Portal.Name = name
	msg.Portal.NameSet = true
	return true, nil
}

func threadNameSetParams(threadID, name string) map[string]any {
	return map[string]any{
		"threadId": threadID,
		"name":     name,
	}
}

func (cl *Client) HandleMatrixRoomTopic(ctx context.Context, msg *bridgev2.MatrixRoomTopic) (bool, error) {
	if msg == nil || msg.Portal == nil || msg.Content == nil {
		return false, nil
	}
	topic := firstTrimmedNonEmpty(msg.Content.Topic)
	msg.Portal.Topic = topic
	msg.Portal.TopicSet = true
	return true, nil
}

func (cl *Client) HandleMatrixRoomState(ctx context.Context, msg *bridgev2.MatrixRoomState) (bool, error) {
	if msg == nil || msg.Portal == nil {
		return false, nil
	}
	if !isCodexRoomStateType(msg.Type.Type) {
		return false, nil
	}
	if msg.StateKey != "" {
		return false, fmt.Errorf("%s state key must be empty", msg.Type.Type)
	}
	if msg.Type.Type == codexThreadStateType {
		// Codex thread metadata is bridge-owned state. The bridge syncs it for
		// clients, but Matrix users shouldn't rewrite session identity.
		return false, nil
	}
	if cl != nil && cl.Main != nil {
		cl.Main.setModelStateForPortalKey(msg.Portal.PortalKey, msg.Content)
	}
	return true, nil
}

func isCodexRoomStateType(stateType string) bool {
	return stateType == beeperAIModelStateType || stateType == codexThreadStateType
}

func starterThreadCWD(meta *PortalMetadata, portal *bridgev2.Portal, body string) (string, bool) {
	if cwd := portalMetadataCwd(portal, meta); cwd != "" {
		return cwd, false
	}
	cwd := firstTrimmedNonEmpty(body)
	return cwd, cwd != ""
}

func portalMetadataCwd(portal *bridgev2.Portal, meta *PortalMetadata) string {
	if meta != nil && meta.Cwd != "" {
		return meta.Cwd
	}
	if portal == nil {
		return ""
	}
	cwd, _ := parseProjectPortalID(portal.ID)
	return cwd
}

func (cl *Client) queueCodexTyping(portalKey networkid.PortalKey, timeout time.Duration) {
	if cl == nil || cl.UserLogin == nil || cl.UserLogin.Bridge == nil {
		return
	}
	cl.UserLogin.QueueRemoteEvent(&simplevent.Typing{
		EventMeta: remoteEventMeta(bridgev2.RemoteEventTyping, portalKey, codexUserID, time.Now()),
		Timeout:   timeout,
		Type:      bridgev2.TypingTypeText,
	})
}

func (cl *Client) ResolveIdentifier(ctx context.Context, identifier string, createChat bool) (*bridgev2.ResolveIdentifierResponse, error) {
	identifier = firstTrimmedNonEmpty(identifier)
	log := zerolog.Ctx(ctx).With().
		Str("identifier", identifier).
		Bool("create_chat", createChat).
		Logger()
	if cl != nil && cl.UserLogin != nil {
		log = log.With().Str("login_id", string(cl.UserLogin.ID)).Logger()
	}
	log.Debug().Msg("Resolving Codex identifier")
	if isStarterIdentifier(identifier) {
		resp := cl.resolveUser(ctx, codexUserID, codexUserInfo("Codex", false))
		if createChat {
			resp.Chat = cl.newProjectChat()
		}
		log.Debug().Str("user_id", string(resp.UserID)).Bool("has_chat", resp.Chat != nil).Msg("Resolved Codex identifier")
		return resp, nil
	}
	if cwd, ok := parseProjectPortalID(networkid.PortalID(identifier)); ok {
		resp, err := cl.resolveProject(ctx, cwd, createChat)
		if err == nil {
			log.Debug().Str("user_id", string(resp.UserID)).Bool("has_chat", resp.Chat != nil).Msg("Resolved Codex project identifier")
		}
		return resp, err
	}
	cwd, err := cleanProjectDir(identifier)
	if err != nil {
		log.Debug().Err(err).Msg("Rejected Codex identifier")
		return nil, err
	}
	resp, err := cl.resolveProject(ctx, cwd, createChat)
	if err == nil {
		log.Debug().Str("user_id", string(resp.UserID)).Str("cwd", cwd).Bool("has_chat", resp.Chat != nil).Msg("Resolved Codex project identifier")
	}
	return resp, err
}

func isStarterIdentifier(identifier string) bool {
	switch lowerTrimmed(identifier) {
	case "", "new", "new project", "codex":
		return true
	default:
		return false
	}
}

func (cl *Client) resolveProject(ctx context.Context, cwd string, createChat bool) (*bridgev2.ResolveIdentifierResponse, error) {
	cwd, err := cleanProjectDir(cwd)
	if err != nil {
		return nil, err
	}
	thread, threadID, state := cl.latestThreadStateForDirectory(ctx, cwd)
	resp := &bridgev2.ResolveIdentifierResponse{
		UserID:   projectUserID(cwd),
		UserInfo: codexUserInfo(cwd, false, cwd, thread.ID, thread.Name),
	}
	resp = cl.resolveUser(ctx, resp.UserID, resp.UserInfo)
	if createChat {
		resp.Chat = cl.chatForProject(ctx, cwd, threadID, state)
	}
	return resp, nil
}

func (cl *Client) CreateChatWithGhost(ctx context.Context, ghost *bridgev2.Ghost) (*bridgev2.CreateChatResponse, error) {
	if ghost == nil {
		return nil, fmt.Errorf("missing ghost")
	}
	log := zerolog.Ctx(ctx).With().Str("ghost_id", string(ghost.ID)).Logger()
	if cl != nil && cl.UserLogin != nil {
		log = log.With().Str("login_id", string(cl.UserLogin.ID)).Logger()
	}
	log.Debug().Msg("Creating Codex chat with ghost")
	switch ghost.ID {
	case codexUserID:
		resp := cl.newProjectChat()
		log.Debug().Stringer("portal_key", resp.PortalKey).Msg("Created Codex starter chat with ghost")
		return resp, nil
	default:
		cwd, ok := parseProjectUserID(ghost.ID)
		if !ok {
			log.Debug().Msg("Rejected unknown Codex ghost")
			return nil, fmt.Errorf("unknown Codex ghost %s", ghost.ID)
		}
		_, threadID, state := cl.latestThreadStateForDirectory(ctx, cwd)
		resp := cl.chatForProject(ctx, cwd, threadID, state)
		log.Debug().Str("cwd", cwd).Str("thread_id", threadID).Stringer("portal_key", resp.PortalKey).Msg("Created Codex project chat with ghost")
		return resp, nil
	}
}

func (cl *Client) GetContactList(ctx context.Context) ([]*bridgev2.ResolveIdentifierResponse, error) {
	log := *zerolog.Ctx(ctx)
	if cl != nil && cl.UserLogin != nil {
		log = log.With().Str("login_id", string(cl.UserLogin.ID)).Logger()
	}
	log.Debug().Msg("Listing Codex contacts")
	if cl.Main == nil {
		contacts := cl.contactsForThreads(ctx, nil)
		log.Debug().Int("contacts", len(contacts)).Msg("Listed Codex contacts without app-server")
		return contacts, nil
	}
	threads, err := cl.listThreads(ctx, "", defaultThreadListLimit)
	if err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).Msg("Failed to list Codex threads for contacts")
		contacts := cl.contactsForThreads(ctx, cl.Main.cachedThreadsForLogin(cl.UserLogin))
		log.Debug().Int("contacts", len(contacts)).Msg("Listed fallback Codex contacts")
		return contacts, nil
	}
	contacts := cl.contactsForThreads(ctx, threads)
	log.Debug().Int("threads", len(threads)).Int("contacts", len(contacts)).Msg("Listed Codex contacts")
	return contacts, nil
}

func (cl *Client) contactsForThreads(ctx context.Context, threads []appserver.Thread) []*bridgev2.ResolveIdentifierResponse {
	contacts := []*bridgev2.ResolveIdentifierResponse{
		cl.resolveUser(ctx, codexUserID, codexUserInfo("Codex", false)),
	}
	for _, thread := range sortedRecentDirectories(threads) {
		contacts = append(contacts, cl.contactForThread(ctx, thread))
	}
	return contacts
}

func (cl *Client) contactForThread(ctx context.Context, thread appserver.Thread) *bridgev2.ResolveIdentifierResponse {
	contact := cl.resolveUser(ctx, projectUserID(thread.Cwd), projectThreadUserInfo(thread))
	chat := cl.chatForProject(ctx, thread.Cwd, thread.ID, codexThreadInitialState(thread))
	if chat != nil && chat.Portal != nil && chat.Portal.MXID != "" {
		contact.Chat = chat
	}
	return contact
}

func (cl *Client) resolveUser(ctx context.Context, userID networkid.UserID, info *bridgev2.UserInfo) *bridgev2.ResolveIdentifierResponse {
	resp := &bridgev2.ResolveIdentifierResponse{UserID: userID, UserInfo: info}
	if cl == nil || cl.UserLogin == nil || cl.UserLogin.Bridge == nil {
		return resp
	}
	if !isResolvableGhostUserID(userID) {
		return resp
	}
	ghost, err := cl.UserLogin.Bridge.GetGhostByID(ctx, userID)
	if err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).Str("user_id", string(userID)).Msg("Failed to resolve Codex ghost")
		return resp
	}
	resp.Ghost = ghost
	return resp
}

func isResolvableGhostUserID(userID networkid.UserID) bool {
	_, ok := parseProjectUserID(userID)
	return userID == codexUserID || ok
}

func (cl *Client) SearchUsers(ctx context.Context, query string) ([]*bridgev2.ResolveIdentifierResponse, error) {
	rawQuery := firstTrimmedNonEmpty(query)
	query = strings.ToLower(rawQuery)
	log := zerolog.Ctx(ctx).With().Str("query", rawQuery).Logger()
	if cl != nil && cl.UserLogin != nil {
		log = log.With().Str("login_id", string(cl.UserLogin.ID)).Logger()
	}
	log.Debug().Msg("Searching Codex contacts")
	contacts, err := cl.GetContactList(ctx)
	if err != nil {
		if direct, ok := cl.directPathSearchResult(ctx, rawQuery); ok {
			log.Debug().Err(err).Msg("Contact listing failed, returning direct Codex project path")
			return []*bridgev2.ResolveIdentifierResponse{direct}, nil
		}
		log.Debug().Err(err).Msg("Failed to search Codex contacts")
		return contacts, err
	}
	if query == "" {
		log.Debug().Int("results", len(contacts)).Msg("Search query empty, returning all Codex contacts")
		return contacts, nil
	}
	filtered := cl.matchingSearchContacts(ctx, rawQuery, query, contacts)
	log.Debug().Int("contacts", len(contacts)).Int("results", len(filtered)).Msg("Searched Codex contacts")
	return filtered, nil
}

func (cl *Client) matchingSearchContacts(ctx context.Context, rawQuery, query string, contacts []*bridgev2.ResolveIdentifierResponse) []*bridgev2.ResolveIdentifierResponse {
	filtered := contacts[:0]
	seenUserIDs := map[networkid.UserID]struct{}{}
	if direct, ok := cl.directPathSearchResult(ctx, rawQuery); ok {
		filtered = appendUniqueSearchResult(filtered, seenUserIDs, direct)
	}
	for _, contact := range contacts {
		if contactMatchesQuery(contact, query) {
			filtered = appendUniqueSearchResult(filtered, seenUserIDs, contact)
		}
	}
	return filtered
}

func appendUniqueSearchResult(results []*bridgev2.ResolveIdentifierResponse, seen map[networkid.UserID]struct{}, contact *bridgev2.ResolveIdentifierResponse) []*bridgev2.ResolveIdentifierResponse {
	if contact == nil {
		return results
	}
	if _, ok := seen[contact.UserID]; ok {
		return results
	}
	seen[contact.UserID] = struct{}{}
	return append(results, contact)
}

func contactMatchesQuery(contact *bridgev2.ResolveIdentifierResponse, query string) bool {
	info := contact.UserInfo
	name := ""
	if info != nil && info.Name != nil {
		name = *info.Name
	}
	if lowerContains(name, query) || lowerContains(string(contact.UserID), query) {
		return true
	}
	if info == nil {
		return false
	}
	for _, identifier := range info.Identifiers {
		if lowerContains(identifier, query) {
			return true
		}
	}
	return false
}

func (cl *Client) directPathSearchResult(ctx context.Context, query string) (*bridgev2.ResolveIdentifierResponse, bool) {
	cwd, err := cleanProjectDir(query)
	if err != nil || cwd == "" {
		return nil, false
	}
	resp, err := cl.resolveProject(ctx, cwd, false)
	if err != nil {
		return nil, false
	}
	return resp, true
}

func (cl *Client) newProjectChat() *bridgev2.CreateChatResponse {
	return &bridgev2.CreateChatResponse{
		PortalKey:      newProjectPortalKey(cl.loginID()),
		PortalInfo:     cl.newProjectChatInfo(),
		DMRedirectedTo: codexUserID,
	}
}

func (cl *Client) newProjectChatInfo() *bridgev2.ChatInfo {
	roomType := database.RoomTypeDM
	name := "New Project"
	topic := newProjectPrompt
	return &bridgev2.ChatInfo{
		Name:                       &name,
		Topic:                      &topic,
		Avatar:                     codexAvatar(),
		Type:                       &roomType,
		Members:                    cl.codexMembers(),
		ExcludeChangesFromTimeline: true,
		CanBackfill:                false,
		ExtraUpdates: bridgev2.MergeExtraUpdaters(
			codexCommandStateUpdater(),
			cl.newProjectIntroUpdater(),
		),
	}
}

func (cl *Client) newProjectIntroUpdater() bridgev2.ExtraUpdater[*bridgev2.Portal] {
	return func(ctx context.Context, portal *bridgev2.Portal) bool {
		if !cl.canQueueNewProjectIntro(portal) {
			return false
		}
		meta := portalMetadata(portal.Metadata)
		meta.NewProjectIntroMessage = true
		portal.Metadata = meta
		go cl.queueNewProjectIntroMessage(context.WithoutCancel(ctx), portal.PortalKey)
		return true
	}
}

func (cl *Client) canQueueNewProjectIntro(portal *bridgev2.Portal) bool {
	if cl == nil || cl.UserLogin == nil || cl.UserLogin.Bridge == nil || portal == nil || portal.MXID == "" {
		return false
	}
	meta := portalMetadata(portal.Metadata)
	return !meta.NewProjectIntroMessage && meta.ThreadID == "" && meta.Cwd == ""
}

func (cl *Client) queueNewProjectIntroMessage(ctx context.Context, portalKey networkid.PortalKey) {
	now := time.Now()
	msgID := networkid.MessageID("new-project-intro:" + string(portalKey.ID))
	run := commandNoticeRun(newProjectIntroText, string(msgID), string(portalKey.ID), "", now)
	res := cl.UserLogin.QueueRemoteEvent(&simplevent.Message[aistream.Run]{
		EventMeta: remoteEventMeta(bridgev2.RemoteEventMessage, portalKey, codexUserID, now),
		ID:        msgID,
		Data:      run,
		ConvertMessageFunc: func(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, data aistream.Run) (*bridgev2.ConvertedMessage, error) {
			content, extra, err := matrixFinalContent(ctx, portal, intent, data)
			if err != nil {
				return nil, err
			}
			return &bridgev2.ConvertedMessage{Parts: []*bridgev2.ConvertedMessagePart{{
				ID:         partID("intro"),
				Type:       event.EventMessage,
				Content:    content,
				Extra:      extra,
				DBMetadata: &MessageMetadata{Role: "assistant", StreamStatus: "intro"},
			}}}, nil
		},
	})
	if !res.Success {
		logCodexQueueFailure(ctx, res, "Failed to queue Codex new project intro", map[string]any{
			"message_id": string(msgID),
		})
	}
}

func (cl *Client) chatForProject(ctx context.Context, cwd, threadID string, state map[string]any) *bridgev2.CreateChatResponse {
	key := projectPortalKey(cwd, cl.loginID())
	var portal *bridgev2.Portal
	if cl.Main != nil && cl.Main.Bridge != nil {
		var err error
		portal, err = cl.Main.Bridge.GetExistingPortalByKey(ctx, key)
		if err != nil {
			zerolog.Ctx(ctx).Warn().Err(err).Stringer("portal_key", key).Msg("Failed to load existing Codex project portal")
		}
	}
	if portal != nil {
		meta := cl.syncProjectPortalMetadata(ctx, portal, cwd, threadID)
		if meta.ThreadID != "" {
			cl.Main.rememberThreadRoom(meta.ThreadID, cl, portal.PortalKey, meta.Cwd, nil)
		}
	}
	if threadID != "" && cl.Main != nil {
		cl.Main.rememberThreadRoom(threadID, cl, key, cwd, state)
	}
	return &bridgev2.CreateChatResponse{
		PortalKey:      key,
		Portal:         portal,
		PortalInfo:     portalInfo(cwd, cl.codexMembers(), cwd, threadID, state),
		DMRedirectedTo: codexUserID,
	}
}

func (cl *Client) loginID() networkid.UserLoginID {
	if cl.UserLogin == nil {
		return defaultLoginID
	}
	return cl.UserLogin.ID
}

func (cl *Client) latestThreadForDirectory(ctx context.Context, cwd string) (appserver.Thread, bool) {
	if cl == nil || cl.Main == nil {
		return appserver.Thread{}, false
	}
	threads, err := cl.listThreads(ctx, "", defaultThreadListLimit)
	if err != nil {
		return appserver.Thread{}, false
	}
	var latest appserver.Thread
	for _, thread := range threads {
		if !isThreadForDirectory(thread, cwd) {
			continue
		}
		if latest.ID == "" || thread.UpdatedAt > latest.UpdatedAt {
			latest = thread
		}
	}
	return latest, latest.ID != ""
}

func isThreadForDirectory(thread appserver.Thread, cwd string) bool {
	return thread.Cwd == cwd && !isDetachedThread(thread)
}

func (cl *Client) latestThreadStateForDirectory(ctx context.Context, cwd string) (appserver.Thread, string, map[string]any) {
	thread, ok := cl.latestThreadForDirectory(ctx, cwd)
	if !ok {
		return appserver.Thread{}, "", nil
	}
	return thread, thread.ID, codexThreadInitialState(thread)
}

func (cl *Client) codexMembers() *bridgev2.ChatMemberList {
	return &bridgev2.ChatMemberList{
		IsFull:                     true,
		OtherUserID:                codexUserID,
		ExcludeChangesFromTimeline: true,
		MemberMap: bridgev2.ChatMemberMap{
			"": {
				EventSender:      bridgev2.EventSender{IsFromMe: true},
				Membership:       event.MembershipJoin,
				MemberEventExtra: excludeFromTimelineExtra(),
			},
			codexUserID: {
				EventSender:      bridgev2.EventSender{Sender: codexUserID},
				Membership:       event.MembershipJoin,
				UserInfo:         codexUserInfo("Codex", false),
				MemberEventExtra: excludeFromTimelineExtra(),
			},
		},
		PowerLevels: &bridgev2.PowerLevelOverrides{
			Events: map[event.Type]int{
				event.StateRoomName:                        0,
				event.StateTopic:                           0,
				roomStateEventType(beeperAIModelStateType): 0,
			},
			Custom: func(content *event.PowerLevelsEventContent) bool {
				changed := false
				for _, eventType := range bridgeOwnedPowerLevelEvents() {
					if _, ok := content.Events[eventType.Type]; ok {
						delete(content.Events, eventType.Type)
						changed = true
					}
				}
				return changed
			},
		},
	}
}

func bridgeOwnedPowerLevelEvents() []event.Type {
	return []event.Type{
		event.StateBeeperDisappearingTimer,
		event.StateMSC4391BotCommand,
		roomStateEventType(codexThreadStateType),
	}
}

func excludeFromTimelineExtra() map[string]any {
	return map[string]any{"com.beeper.exclude_from_timeline": true}
}

func (cl *Client) listThreads(ctx context.Context, cursor string, limit int) ([]appserver.Thread, error) {
	if limit <= 0 {
		limit = defaultThreadListLimit
	}
	var out []appserver.Thread
	seen := map[string]bool{}
	for {
		params := threadListParams(cursor, limit)
		var resp appserver.ThreadListResponse
		if err := cl.Main.request(ctx, "thread/list", params, &resp); err != nil {
			return nil, err
		}
		out = append(out, resp.Data...)
		if !advanceCursor(seen, &cursor, resp.NextCursor) {
			return out, nil
		}
	}
}

func threadListParams(cursor string, limit int) map[string]any {
	params := requestLimitParams(limit, cursor)
	params["sortKey"] = threadListSortKey
	params["sortDirection"] = sortDirectionDescending
	return params
}

func (cl *Client) readThread(ctx context.Context, threadID string, includeTurns bool) (appserver.Thread, error) {
	var resp appserver.ThreadReadResponse
	err := cl.Main.request(ctx, "thread/read", threadReadParams(threadID, includeTurns), &resp)
	return resp.Thread, err
}

func threadReadParams(threadID string, includeTurns bool) map[string]any {
	params := threadIDParams(threadID)
	params["includeTurns"] = includeTurns
	return params
}

func (cl *Client) readThreadForBackfill(ctx context.Context, threadID string) (appserver.Thread, error) {
	thread, err := cl.readThread(ctx, threadID, false)
	if err != nil {
		return appserver.Thread{}, err
	}
	turns, err := cl.listThreadTurns(ctx, threadID)
	if err != nil {
		if isThreadTurnsListUnavailable(err) {
			return cl.readThread(ctx, threadID, true)
		}
		return appserver.Thread{}, err
	}
	thread.Turns = turns
	return thread, nil
}

func (cl *Client) listThreadTurns(ctx context.Context, threadID string) ([]appserver.Turn, error) {
	var out []appserver.Turn
	cursor := ""
	seen := map[string]bool{}
	for {
		params := threadTurnsListParams(threadID, cursor)
		var resp appserver.ThreadTurnsListResponse
		if err := cl.Main.request(ctx, "thread/turns/list", params, &resp); err != nil {
			return nil, err
		}
		out = append(out, resp.Data...)
		if !advanceCursor(seen, &cursor, resp.NextCursor) {
			return out, nil
		}
	}
}

func threadTurnsListParams(threadID, cursor string) map[string]any {
	params := requestLimitParams(threadTurnsListLimit, cursor)
	params["threadId"] = threadID
	params["sortDirection"] = sortDirectionAscending
	params["itemsView"] = threadTurnsItemsView
	return params
}

func (cl *Client) startThreadForPortal(ctx context.Context, cwd string, portal *bridgev2.Portal) (appserver.Thread, error) {
	var err error
	if cwd, err = cleanProjectDir(cwd); err != nil {
		return appserver.Thread{}, err
	}
	params := threadStartParams(cwd)
	cl.applyValidatedRoomThreadSettings(ctx, params, cl.roomAIModelState(ctx, portal), portal, networkid.PortalKey{})
	var resp appserver.ThreadOpenResponse
	if err := cl.Main.request(ctx, "thread/start", params, &resp); err != nil {
		return appserver.Thread{}, err
	}
	return cl.rememberHydratedThread(resp), nil
}

func (cl *Client) resumeThreadForPortal(ctx context.Context, portal *bridgev2.Portal, meta *PortalMetadata) (appserver.Thread, error) {
	if meta == nil || meta.ThreadID == "" {
		return appserver.Thread{}, fmt.Errorf("missing Codex thread id")
	}
	params := threadResumeParams(meta.ThreadID)
	cwd := portalMetadataCwd(portal, meta)
	if cwd != "" {
		cleaned, err := cleanProjectDir(cwd)
		if err != nil {
			zerolog.Ctx(ctx).Warn().Err(err).Str("thread_id", meta.ThreadID).Str("cwd", cwd).Msg("Omitting unavailable cwd while resuming Codex thread")
		} else {
			params["cwd"] = cleaned
		}
	}
	cl.applyValidatedRoomThreadSettings(ctx, params, cl.roomAIModelState(ctx, portal), portal, networkid.PortalKey{})
	var resp appserver.ThreadOpenResponse
	if err := cl.Main.request(ctx, "thread/resume", params, &resp); err != nil {
		return appserver.Thread{}, err
	}
	return cl.rememberHydratedThread(resp), nil
}

func threadStartParams(cwd string) map[string]any {
	params := threadOpenParams()
	params["cwd"] = cwd
	params["threadSource"] = "user"
	return params
}

func threadResumeParams(threadID string) map[string]any {
	params := threadOpenParams()
	params["threadId"] = threadID
	params["excludeTurns"] = true
	return params
}

func threadOpenParams() map[string]any {
	return map[string]any{"approvalPolicy": codexApprovalPolicyOnRequest}
}

func (cl *Client) rememberHydratedThread(resp appserver.ThreadOpenResponse) appserver.Thread {
	thread := resp.HydratedThread()
	cl.Main.rememberWarmThread(thread.ID)
	return thread
}

func (cl *Client) replaceMissingThread(ctx context.Context, portal *bridgev2.Portal, meta *PortalMetadata) (appserver.Thread, error) {
	cwd := portalMetadataCwd(portal, meta)
	if cwd == "" {
		return appserver.Thread{}, fmt.Errorf("missing cwd for replacement Codex thread")
	}
	thread, err := cl.startThreadForPortal(ctx, cwd, portal)
	if err != nil {
		return appserver.Thread{}, err
	}
	return thread, nil
}

func (cl *Client) hydratePortalThreadMetadata(ctx context.Context, portal *bridgev2.Portal, meta *PortalMetadata) *PortalMetadata {
	if portal == nil || meta == nil || meta.ThreadID != "" {
		return meta
	}
	state := cl.roomCodexThreadState(ctx, portal)
	threadID := firstString(state, "threadId", "sessionId")
	if threadID == "" {
		return meta
	}
	cwd := firstNonEmptyString(firstString(state, "cwd"), meta.Cwd)
	cl.syncProjectPortalMetadata(ctx, portal, cwd, threadID)
	meta = portalMetadata(portal.Metadata)
	if cl.Main != nil {
		cl.Main.rememberThreadRoom(threadID, cl, portal.PortalKey, meta.Cwd, nil)
	}
	zerolog.Ctx(ctx).Info().Str("thread_id", threadID).Str("cwd", meta.Cwd).Msg("Hydrated Codex portal metadata from room state")
	return meta
}

func (cl *Client) roomAIModelState(ctx context.Context, portal *bridgev2.Portal) map[string]any {
	if cl == nil || cl.Main == nil || portal == nil {
		return nil
	}
	state := cl.Main.modelStateForPortalKey(portal.PortalKey)
	if portal.MXID == "" {
		return state
	}
	reader := cl.roomStateReader()
	if reader == nil {
		return state
	}
	roomState, err := readRawRoomState(ctx, reader, portal.MXID, beeperAIModelStateType)
	if err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).Str("room_id", string(portal.MXID)).Msg("Failed to read Codex room model state")
		return state
	}
	return mergeRoomAIModelState(state, roomState)
}

func mergeRoomAIModelState(cached, room map[string]any) map[string]any {
	if len(room) == 0 {
		return cached
	}
	if len(cached) == 0 {
		return room
	}
	merged := copyStateMap(cached)
	maps.Copy(merged, room)
	return merged
}

func (cl *Client) roomAIModelStateForPortalKey(ctx context.Context, portalKey networkid.PortalKey) map[string]any {
	if cl == nil || cl.Main == nil {
		return nil
	}
	state := cl.Main.modelStateForPortalKey(portalKey)
	if cl.Main.Bridge == nil {
		return state
	}
	portal, err := cl.Main.Bridge.GetExistingPortalByKey(ctx, portalKey)
	if err != nil || portal == nil {
		return state
	}
	return cl.roomAIModelState(ctx, portal)
}

func (cl *Client) roomCodexThreadState(ctx context.Context, portal *bridgev2.Portal) map[string]any {
	if portal == nil || portal.MXID == "" {
		return nil
	}
	reader := cl.roomStateReader()
	if reader == nil {
		return nil
	}
	state, err := readRawRoomState(ctx, reader, portal.MXID, codexThreadStateType)
	if err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).Str("room_id", string(portal.MXID)).Msg("Failed to read Codex thread room state")
		return nil
	}
	return state
}

func (cl *Client) roomStateReader() bridgev2.MatrixConnectorWithArbitraryRoomState {
	if cl == nil || cl.Main == nil || cl.Main.Bridge == nil {
		return nil
	}
	reader, _ := cl.Main.Bridge.Matrix.(bridgev2.MatrixConnectorWithArbitraryRoomState)
	return reader
}

func readRawRoomState(ctx context.Context, reader bridgev2.MatrixConnectorWithArbitraryRoomState, roomID id.RoomID, stateType string) (map[string]any, error) {
	evt, err := reader.GetStateEvent(ctx, roomID, roomStateEventType(stateType), "")
	if errors.Is(err, mautrix.MNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if evt == nil {
		return nil, nil
	}
	return rawRoomStateContent(evt)
}

func rawRoomStateContent(evt *event.Event) (map[string]any, error) {
	if evt == nil {
		return nil, nil
	}
	if evt.Content.Raw != nil {
		return evt.Content.Raw, nil
	}
	if evt.Content.Parsed == nil {
		return nil, nil
	}
	var raw map[string]any
	data, err := json.Marshal(evt.Content.Parsed)
	if err != nil {
		return nil, err
	}
	if err = json.Unmarshal(data, &raw); err != nil {
		return nil, err
	}
	return raw, nil
}

func isThreadNotFoundError(err error) bool {
	_, msg, ok := lowerRPCErrorMessage(err)
	return ok && containsAny(msg, "thread not found", "thread not loaded")
}

func isThreadTurnsListUnavailable(err error) bool {
	rpcErr, msg, ok := lowerRPCErrorMessage(err)
	return ok && (rpcErr.Code == -32601 || containsAny(msg, "method not found", "unknown method", "experimental"))
}

func lowerRPCErrorMessage(err error) (*appserver.RPCError, string, bool) {
	var rpcErr *appserver.RPCError
	if !errors.As(err, &rpcErr) {
		return nil, "", false
	}
	return rpcErr, strings.ToLower(rpcErr.Msg), true
}

func cleanProjectDir(path string) (string, error) {
	path = firstTrimmedNonEmpty(path)
	if path == "" {
		return "", nil
	}
	path, err := expandHomePath(path)
	if err != nil {
		return "", err
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	stat, err := os.Stat(abs)
	if err != nil {
		return "", fmt.Errorf("directory %s is not available: %w", abs, err)
	}
	if !stat.IsDir() {
		return "", fmt.Errorf("%s is not a directory", abs)
	}
	return abs, nil
}

func expandHomePath(path string) (string, error) {
	if path == "~" {
		return os.UserHomeDir()
	}
	rest, ok := strings.CutPrefix(path, "~/")
	if !ok {
		return path, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, rest), nil
}

func (cl *Client) syncThreadPortal(ctx context.Context, portal *bridgev2.Portal, thread appserver.Thread) *bridgev2.Portal {
	if portal == nil || thread.ID == "" {
		return portal
	}
	if thread.Cwd != "" && cl != nil && cl.UserLogin != nil {
		target := projectPortalKey(thread.Cwd, cl.UserLogin.ID)
		if portal.PortalKey != target && cl.Main != nil && cl.Main.Bridge != nil {
			portal = cl.canonicalizeThreadPortal(ctx, portal, target)
		}
	}
	cl.syncProjectPortalMetadata(ctx, portal, thread.Cwd, thread.ID)
	name := threadName(thread)
	state := enrichThreadStateWithModelState(codexThreadInitialState(thread), cl.roomAIModelState(ctx, portal))
	cl.Main.rememberThreadRoom(thread.ID, cl, portal.PortalKey, thread.Cwd, state)
	info := portalInfo(name, cl.codexMembers(), thread.Cwd, thread.ID, state)
	updatePortalInfo(ctx, portal, cl.UserLogin, info)
	return portal
}

func (cl *Client) canonicalizeThreadPortal(ctx context.Context, portal *bridgev2.Portal, target networkid.PortalKey) *bridgev2.Portal {
	result, synced, err := cl.Main.Bridge.ReIDPortal(ctx, portal.PortalKey, target)
	if err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).
			Stringer("source_portal_key", portal.PortalKey).
			Stringer("target_portal_key", target).
			Msg("Failed to canonicalize Codex starter portal")
		return portal
	}
	zerolog.Ctx(ctx).Info().
		Int("result", int(result)).
		Stringer("target_portal_key", target).
		Msg("Canonicalized Codex starter portal")
	if synced == nil {
		return portal
	}
	return synced
}

func (cl *Client) syncSubagentPortal(ctx context.Context, portal *bridgev2.Portal, parentThreadID string, thread appserver.Thread) *bridgev2.Portal {
	if cl == nil || cl.Main == nil || portal == nil || thread.ID == "" {
		return portal
	}
	oldThreadID := portalMetadata(portal.Metadata).ThreadID
	ref := subagentRef{ThreadID: thread.ID, Status: firstString(thread.Raw, "status", "state")}
	meta := portalMetadata(portal.Metadata)
	if meta.applySubagent(parentThreadID, ref.ThreadID, thread.Cwd) {
		portal.Metadata = meta
		if err := portal.Save(ctx); err != nil {
			zerolog.Ctx(ctx).Warn().Err(err).Stringer("portal_key", portal.PortalKey).Msg("Failed to save Codex subagent portal metadata")
			return portal
		}
	}
	cl.syncPortalBackfill(ctx, portal, oldThreadID)
	state := subagentThreadState(parentThreadID, ref, thread.Cwd, codexThreadInitialState(thread))
	cl.Main.rememberThreadRoom(thread.ID, cl, portal.PortalKey, thread.Cwd, state)
	info := subagentPortalInfo(cl.codexMembers(), parentThreadID, ref, thread.Cwd, state)
	updatePortalInfo(ctx, portal, cl.UserLogin, info)
	return portal
}

const codexBackfillVersion = 3

func (cl *Client) syncPortalBackfill(ctx context.Context, portal *bridgev2.Portal, oldThreadID string) {
	if cl == nil || cl.Main == nil || cl.Main.Bridge == nil || cl.UserLogin == nil || portal == nil || portal.MXID == "" {
		return
	}
	meta := portalMetadata(portal.Metadata)
	if meta.ThreadID == "" && meta.Cwd == "" {
		return
	}
	reset := oldThreadID != meta.ThreadID
	if meta.BackfillVersion != codexBackfillVersion {
		meta.BackfillVersion = codexBackfillVersion
		portal.Metadata = meta
		if err := portal.Save(ctx); err != nil {
			zerolog.Ctx(ctx).Warn().Err(err).Stringer("portal_key", portal.PortalKey).Msg("Failed to save Codex backfill version")
			return
		}
		reset = true
	}
	if !reset {
		return
	}
	cl.resetPortalBackfillTask(ctx, portal)
}

func (cl *Client) resetPortalBackfillTask(ctx context.Context, portal *bridgev2.Portal) {
	if err := cl.Main.Bridge.DB.BackfillTask.Upsert(ctx, &database.BackfillTask{
		PortalKey:         portal.PortalKey,
		UserLoginID:       cl.UserLogin.ID,
		BatchCount:        -1,
		NextDispatchMinTS: time.Now(),
	}); err != nil {
		zerolog.Ctx(ctx).Err(err).Msg("Failed to reset Codex backfill task")
		return
	}
	cl.Main.Bridge.WakeupBackfillQueue()
}

func (cl *Client) syncProjectPortalMetadata(ctx context.Context, portal *bridgev2.Portal, cwd, threadID string) *PortalMetadata {
	if portal == nil {
		return &PortalMetadata{}
	}
	meta := portalMetadata(portal.Metadata)
	oldThreadID := meta.ThreadID
	if meta.applyProject(cwd, threadID) && !saveProjectPortalMetadata(ctx, portal, meta, threadID) {
		return portalMetadata(portal.Metadata)
	}
	cl.syncPortalBackfill(ctx, portal, oldThreadID)
	return portalMetadata(portal.Metadata)
}

func saveProjectPortalMetadata(ctx context.Context, portal *bridgev2.Portal, meta *PortalMetadata, threadID string) bool {
	portal.Metadata = meta
	if err := portal.Save(ctx); err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).Str("thread_id", threadID).Msg("Failed to save Codex portal metadata")
		return false
	}
	return true
}

func (cl *Client) clearMissingThread(ctx context.Context, portal *bridgev2.Portal, meta *PortalMetadata) {
	if portal == nil || meta == nil || meta.ThreadID == "" {
		return
	}
	oldThreadID := meta.ThreadID
	meta.ThreadID = ""
	portal.Metadata = meta
	if err := portal.Save(ctx); err != nil {
		zerolog.Ctx(ctx).Err(err).Str("thread_id", oldThreadID).Msg("Failed to clear missing Codex thread metadata")
		return
	}
	if cl != nil && cl.Main != nil {
		cl.Main.forgetThread(oldThreadID)
	}
	zerolog.Ctx(ctx).Warn().Str("thread_id", oldThreadID).Str("cwd", meta.Cwd).Msg("Cleared missing Codex thread metadata")
}

func (cl *Client) startTurn(ctx context.Context, portalKey networkid.PortalKey, threadID, clientUserMessageID, prompt string) (*activeRun, error) {
	params := turnStartParams(threadID, prompt, clientUserMessageID)
	cl.applyValidatedRoomTurnSettings(ctx, params, cl.roomAIModelStateForPortalKey(ctx, portalKey), nil, portalKey)
	cl.Main.setPendingTurnStart(threadID, cl, portalKey)
	defer cl.Main.clearPendingTurnStart(threadID)
	var resp appserver.TurnStartResponse
	if err := cl.Main.request(ctx, "turn/start", params, &resp); err != nil {
		return nil, err
	}
	cl.Main.rememberWarmThread(threadID)
	run, err := cl.startActiveTurnRun(ctx, portalKey, threadID, resp.Turn.ID)
	if err != nil {
		return nil, err
	}
	run.writeCodexClientRequestState("turn/start", codexClientTurnRequestState(threadID, resp.Turn.ID, "", clientUserMessageID))
	return run, nil
}

func turnStartParams(threadID, prompt, clientUserMessageID string) map[string]any {
	params := turnInputParams(threadID, prompt)
	params["approvalPolicy"] = codexApprovalPolicyOnRequest
	setNonEmptyMapString(params, "clientUserMessageId", clientUserMessageID)
	return params
}

func (cl *Client) startActiveTurnRun(ctx context.Context, portalKey networkid.PortalKey, threadID, turnID string) (*activeRun, error) {
	run := cl.Main.activeRun(threadID)
	if run != nil && run.turnID == turnID {
		return run, run.start(ctx)
	}
	run = newActiveRun(cl, portalKey, threadID, turnID)
	cl.Main.setActive(threadID, run)
	if err := run.start(ctx); err != nil {
		cl.Main.setActive(threadID, nil)
		return nil, err
	}
	return run, nil
}

func (cl *Client) steerTurn(ctx context.Context, threadID, turnID, clientUserMessageID, prompt string) error {
	params := turnInputParams(threadID, prompt)
	params["expectedTurnId"] = turnID
	setNonEmptyMapString(params, "clientUserMessageId", clientUserMessageID)
	if err := cl.Main.request(ctx, "turn/steer", params, nil); err != nil {
		return err
	}
	if run := cl.Main.activeRun(threadID); run != nil {
		run.writeCodexClientRequestState("turn/steer", codexClientTurnRequestState(threadID, "", turnID, clientUserMessageID))
	}
	return nil
}

func (cl *Client) interruptTurn(ctx context.Context, portal *bridgev2.Portal, threadID, turnID string) {
	if err := cl.Main.request(ctx, "turn/interrupt", turnInterruptParams(threadID, turnID), nil); err != nil {
		cl.queueCommandNotice(portal, threadID, "Failed to stop Codex turn:\n\n"+err.Error())
		return
	}
	if run := cl.Main.activeRun(threadID); run != nil {
		run.writeCodexClientRequestState("turn/interrupt", codexClientTurnRequestState(threadID, turnID, "", ""))
	}
	cl.queueCommandNotice(portal, threadID, "Requested Codex to stop the active turn.")
}

func turnInterruptParams(threadID, turnID string) map[string]any {
	params := threadIDParams(threadID)
	params["turnId"] = turnID
	return params
}

func threadRollbackParams(threadID string, numTurns int) map[string]any {
	params := threadIDParams(threadID)
	params["numTurns"] = numTurns
	return params
}

func turnInputParams(threadID, prompt string) map[string]any {
	params := threadIDParams(threadID)
	params["input"] = turnTextInput(prompt)
	return params
}

func codexClientTurnRequestState(threadID, turnID, expectedTurnID, clientUserMessageID string) map[string]any {
	state := threadIDParams(threadID)
	setCodexClientTurnRequestFields(state, turnID, expectedTurnID, clientUserMessageID)
	return state
}

func threadIDParams(threadID string) map[string]any {
	return map[string]any{"threadId": threadID}
}

func setCodexClientTurnRequestFields(state map[string]any, turnID, expectedTurnID, clientUserMessageID string) {
	setNonEmptyMapString(state, "turnId", turnID)
	setNonEmptyMapString(state, "expectedTurnId", expectedTurnID)
	setNonEmptyMapString(state, "clientUserMessageId", clientUserMessageID)
}

func turnTextInput(prompt string) []map[string]any {
	return []map[string]any{{
		"type":         "text",
		"text":         prompt,
		"textElements": []any{},
	}}
}

func (cl *Client) FetchMessages(ctx context.Context, params bridgev2.FetchMessagesParams) (*bridgev2.FetchMessagesResponse, error) {
	if params.Portal == nil {
		return emptyFetchMessagesResponse(), nil
	}
	meta := portalMetadata(params.Portal.Metadata)
	meta = cl.hydratePortalThreadMetadata(ctx, params.Portal, meta)
	if meta.Kind != portalKindSubagent && meta.Cwd != "" {
		threads, err := cl.readDirectoryThreadsForBackfill(ctx, meta.Cwd)
		if err != nil {
			return nil, err
		}
		if len(threads) > 0 {
			messages, err := cl.directoryBackfillMessages(ctx, params.Portal, threads)
			if err != nil {
				return nil, err
			}
			return paginateBackfillMessages(messages, params), nil
		}
	}
	if meta.ThreadID == "" {
		return emptyFetchMessagesResponse(), nil
	}
	thread, err := cl.readThreadForBackfill(ctx, meta.ThreadID)
	if err != nil {
		if isThreadNotFoundError(err) {
			cl.clearMissingThread(ctx, params.Portal, meta)
			return emptyFetchMessagesResponse(), nil
		}
		return nil, err
	}
	messages, err := cl.projectBackfillMessages(ctx, params.Portal, thread)
	if err != nil {
		return nil, err
	}
	return paginateBackfillMessages(messages, params), nil
}

func emptyFetchMessagesResponse() *bridgev2.FetchMessagesResponse {
	return &bridgev2.FetchMessagesResponse{HasMore: false}
}

func (cl *Client) GetBackfillMaxBatchCount(ctx context.Context, portal *bridgev2.Portal, task *database.BackfillTask) int {
	if portal == nil || portal.RoomType == database.RoomTypeSpace {
		return 0
	}
	meta := portalMetadata(portal.Metadata)
	if meta.ThreadID != "" || meta.Cwd != "" {
		return -1
	}
	return 0
}

func (cl *Client) readDirectoryThreadsForBackfill(ctx context.Context, cwd string) ([]appserver.Thread, error) {
	if cwd == "" {
		return nil, nil
	}
	threads, err := cl.listThreads(ctx, "", defaultThreadListLimit)
	if err != nil {
		return nil, err
	}
	out := make([]appserver.Thread, 0, len(threads))
	for _, listed := range threads {
		thread, ok, err := cl.readListedDirectoryThreadForBackfill(ctx, listed, cwd)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		out = append(out, thread)
	}
	sort.Slice(out, func(i, j int) bool {
		return threadBackfillBefore(out[i], out[j])
	})
	return out, nil
}

func (cl *Client) readListedDirectoryThreadForBackfill(ctx context.Context, listed appserver.Thread, cwd string) (appserver.Thread, bool, error) {
	if !isThreadForDirectory(listed, cwd) {
		return appserver.Thread{}, false, nil
	}
	thread, err := cl.readThreadForBackfill(ctx, listed.ID)
	if err != nil {
		if isThreadNotFoundError(err) {
			return appserver.Thread{}, false, nil
		}
		return appserver.Thread{}, false, err
	}
	if thread.Cwd == "" {
		thread.Cwd = listed.Cwd
	}
	return thread, isThreadForDirectory(thread, cwd), nil
}

func threadBackfillSortTime(thread appserver.Thread) int64 {
	if thread.CreatedAt != 0 {
		return thread.CreatedAt
	}
	return thread.UpdatedAt
}

func threadBackfillBefore(left, right appserver.Thread) bool {
	leftTime := threadBackfillSortTime(left)
	rightTime := threadBackfillSortTime(right)
	if leftTime == rightTime {
		return left.ID < right.ID
	}
	return leftTime < rightTime
}

func (cl *Client) directoryBackfillMessages(ctx context.Context, portal *bridgev2.Portal, threads []appserver.Thread) ([]*bridgev2.BackfillMessage, error) {
	var messages []*bridgev2.BackfillMessage
	for _, thread := range threads {
		threadMessages, err := cl.projectBackfillMessages(ctx, portal, thread)
		if err != nil {
			return nil, err
		}
		messages = append(messages, threadMessages...)
	}
	sort.SliceStable(messages, func(i, j int) bool {
		return backfillMessageBefore(messages[i], messages[j])
	})
	assignBackfillStreamOrder(messages)
	return messages, nil
}

func assignBackfillStreamOrder(messages []*bridgev2.BackfillMessage) {
	var streamOrder int64
	for _, msg := range messages {
		streamOrder = nextBackfillStreamOrder(streamOrder, msg.Timestamp)
		msg.StreamOrder = streamOrder
	}
}

func backfillMessageBefore(left, right *bridgev2.BackfillMessage) bool {
	if left.Timestamp.Equal(right.Timestamp) {
		return left.ID < right.ID
	}
	return left.Timestamp.Before(right.Timestamp)
}

func isArchivedThread(thread appserver.Thread) bool {
	return firstBool(thread.Raw, "archived", "isArchived") || firstString(thread.Raw, "archivedAt") != ""
}

func isDetachedThread(thread appserver.Thread) bool {
	return isArchivedThread(thread) || firstBool(thread.Raw, "closed", "isClosed")
}

func firstBool(values map[string]any, keys ...string) bool {
	for _, key := range keys {
		if value, _ := values[key].(bool); value {
			return true
		}
	}
	return false
}

func (cl *Client) applyValidatedRoomTurnSettings(ctx context.Context, params map[string]any, state map[string]any, portal *bridgev2.Portal, portalKey networkid.PortalKey) {
	applyRoomTurnSettings(params, state)
	delete(params, "model")
	cl.setValidatedRequestModel(ctx, params, state, portal, portalKey)
}

func (cl *Client) applyValidatedRoomThreadSettings(ctx context.Context, params map[string]any, state map[string]any, portal *bridgev2.Portal, portalKey networkid.PortalKey) {
	if params == nil {
		return
	}
	cl.setValidatedRequestModel(ctx, params, state, portal, portalKey)
	setThreadReasoningEffortParam(params, state)
}

func setThreadReasoningEffortParam(params map[string]any, state map[string]any) {
	if effort := roomReasoningEffort(state); effort != "" {
		ensureMapParam(params, "config")["model_reasoning_effort"] = effort
	}
}

func (cl *Client) setValidatedRequestModel(ctx context.Context, params map[string]any, state map[string]any, portal *bridgev2.Portal, portalKey networkid.PortalKey) {
	setNonEmptyMapString(params, "model", cl.validatedRequestModel(ctx, state, portal, portalKey))
}

func ensureMapParam(params map[string]any, key string) map[string]any {
	value, _ := params[key].(map[string]any)
	if value != nil {
		return value
	}
	value = map[string]any{}
	params[key] = value
	return value
}

func applyRoomTurnSettings(params map[string]any, state map[string]any) {
	if params == nil {
		return
	}
	setNonEmptyMapString(params, "model", codexRequestModel(firstString(state, "model")))
	setNonEmptyMapString(params, "effort", roomReasoningEffort(state))
}

func roomReasoningEffort(state map[string]any) string {
	return firstString(state, "effort", "reasoning", "reasoningEffort", "reasoning_effort")
}

func roomReasoningMode(state map[string]any) string {
	return firstString(state, "reasoning_mode", "reasoningMode")
}

func (cl *Client) validatedRequestModel(ctx context.Context, state map[string]any, portal *bridgev2.Portal, portalKey networkid.PortalKey) string {
	if cl == nil || cl.Main == nil {
		return ""
	}
	roomModel, model, fromRoomState := requestModelCandidate(state, cl.Main.Config.DefaultModel)
	if model == "" {
		return ""
	}
	ok, err := cl.supportsModel(ctx, model)
	if err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).Str("model", model).Msg("Failed to validate Codex model; passing it through")
		return model
	}
	if ok {
		return model
	}
	if fromRoomState {
		cl.clearUnsupportedRoomModel(ctx, portal, portalKey, roomModel)
	}
	return ""
}

func requestModelCandidate(state map[string]any, defaultModel string) (roomModel, model string, fromRoomState bool) {
	roomModel = codexModelStateRef(state, firstString(state, "provider", "modelProvider"))
	roomRequestModel := codexRequestModel(roomModel)
	return roomModel, firstNonEmptyString(roomRequestModel, codexRequestModel(defaultModel)), roomRequestModel != ""
}

func (cl *Client) supportsModel(ctx context.Context, model string) (bool, error) {
	if cl == nil || cl.Main == nil {
		return false, nil
	}
	model = firstTrimmedNonEmpty(model)
	if model == "" {
		return false, nil
	}
	cursor := ""
	seen := map[string]bool{}
	for {
		params := modelListParams(cursor)
		var resp appserver.ModelListResponse
		if err := cl.Main.request(ctx, "model/list", params, &resp); err != nil {
			return false, err
		}
		if modelListContains(resp.Data, model) {
			return true, nil
		}
		if !advanceCursor(seen, &cursor, resp.NextCursor) {
			return false, nil
		}
	}
}

func modelListParams(cursor string) map[string]any {
	params := requestLimitParams(modelListLimit, cursor)
	params["includeHidden"] = true
	return params
}

func requestLimitParams(limit int, cursor string) map[string]any {
	params := map[string]any{"limit": limit}
	setNonEmptyMapString(params, "cursor", cursor)
	return params
}

func modelListContains(models []appserver.Model, model string) bool {
	for _, item := range models {
		if item.Model == model || item.ID == model {
			return true
		}
	}
	return false
}

func advanceCursor(seen map[string]bool, current *string, next string) bool {
	cursor, ok := unseenCursor(seen, next)
	if !ok {
		return false
	}
	*current = cursor
	return true
}

func unseenCursor(seen map[string]bool, cursor string) (string, bool) {
	if cursor == "" || seen[cursor] {
		return "", false
	}
	seen[cursor] = true
	return cursor, true
}

func (cl *Client) clearUnsupportedRoomModel(ctx context.Context, portal *bridgev2.Portal, portalKey networkid.PortalKey, model string) {
	if cl == nil || cl.UserLogin == nil || cl.UserLogin.Bridge == nil {
		return
	}
	if cl.Main != nil {
		cl.Main.setModelStateForPortalKey(portalKey, nil)
	}
	if portal == nil && portalKey.ID != "" {
		var err error
		portal, err = cl.UserLogin.Bridge.GetExistingPortalByKey(ctx, portalKey)
		if err != nil {
			zerolog.Ctx(ctx).Warn().Err(err).Stringer("portal_key", portalKey).Msg("Failed to load portal while clearing unsupported Codex model")
		}
	}
	if portal == nil {
		return
	}
	if err := clearRoomAIModelState(ctx, portal); err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).Str("model", model).Msg("Failed to clear unsupported Codex room model state")
	}
	meta := portalMetadata(portal.Metadata)
	cl.queueCommandNotice(portal, meta.ThreadID, "Codex model "+model+" is not available for this login. Cleared the room model and used the Codex default.")
}

func clearRoomAIModelState(ctx context.Context, portal *bridgev2.Portal) error {
	if portal == nil || portal.MXID == "" {
		return nil
	}
	_, err := portal.Internal().SendStateWithIntentOrBot(ctx, nil, roomStateEventType(beeperAIModelStateType), "", &event.Content{Raw: map[string]any{}}, time.Now())
	return err
}

func codexRequestModel(model string) string {
	return strings.TrimPrefix(firstTrimmedNonEmpty(model), "openai/")
}

func threadName(thread appserver.Thread) string {
	return firstNonEmptyString(firstTrimmedNonEmpty(thread.Name, thread.Preview), directoryName(thread.Cwd))
}
