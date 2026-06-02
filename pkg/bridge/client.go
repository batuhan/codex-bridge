package bridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
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

var _ bridgev2.NetworkAPI = (*Client)(nil)
var _ bridgev2.NetworkAPIWithUserID = (*Client)(nil)
var _ bridgev2.IdentifierResolvingNetworkAPI = (*Client)(nil)
var _ bridgev2.GhostDMCreatingNetworkAPI = (*Client)(nil)
var _ bridgev2.ContactListingNetworkAPI = (*Client)(nil)
var _ bridgev2.UserSearchingNetworkAPI = (*Client)(nil)
var _ bridgev2.BackfillingNetworkAPI = (*Client)(nil)
var _ bridgev2.BackfillingNetworkAPIWithLimits = (*Client)(nil)
var _ bridgev2.EditHandlingNetworkAPI = (*Client)(nil)
var _ bridgev2.RoomNameHandlingNetworkAPI = (*Client)(nil)
var _ bridgev2.RoomTopicHandlingNetworkAPI = (*Client)(nil)
var _ bridgev2.RoomStateHandlingNetworkAPI = (*Client)(nil)
var _ status.BridgeStateFiller = (*Client)(nil)

func (cl *Client) Connect(ctx context.Context) {
	cl.loggedIn = true
	if cl.UserLogin != nil && cl.UserLogin.BridgeState != nil {
		cl.UserLogin.BridgeState.Send(status.BridgeState{StateEvent: status.StateConnected})
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
	return networkid.UserID("login:" + string(cl.UserLogin.ID))
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
			cl.Main.rememberThreadRoom(meta.ThreadID, cl, portal.PortalKey, meta.Cwd)
		}
	}
	name := "New Project"
	var state map[string]any
	if meta.ThreadID != "" {
		if thread, err := cl.readThread(ctx, meta.ThreadID, false); err == nil {
			name = threadName(thread)
			state = enrichThreadStateWithModelState(codexThreadInitialState(thread), cl.roomAIModelState(ctx, portal))
			if cl.Main != nil {
				cl.Main.rememberThreadRoom(thread.ID, cl, portal.PortalKey, thread.Cwd, thread.ModelProvider, threadModelRef(thread), threadReasoningEffort(thread), firstStateString(state, "modelName", "name"), firstStateString(state, "reasoning_mode", "reasoningMode"))
			}
		} else {
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
	} else if meta.Cwd != "" {
		name = directoryName(meta.Cwd)
	}
	if meta.ThreadID == "" && meta.Cwd == "" {
		info := cl.newProjectChatInfo(name)
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
	case newProjectUserID:
		return codexUserInfo("New Project", false), nil
	default:
		if cwd, ok := parseProjectUserID(ghost.ID); ok {
			return codexUserInfo(directoryName(cwd), false, cwd), nil
		}
		if strings.HasPrefix(string(ghost.ID), "login:") {
			return loginUserInfo(), nil
		}
		return nil, fmt.Errorf("unknown Codex ghost %s", ghost.ID)
	}
}

func (cl *Client) GetCapabilities(ctx context.Context, portal *bridgev2.Portal) *event.RoomFeatures {
	return &event.RoomFeatures{
		ID: roomFeaturesID,
		State: event.StateFeatureMap{
			event.StateRoomName.Type: {Level: event.CapLevelFullySupported},
			event.StateTopic.Type:    {Level: event.CapLevelFullySupported},
			beeperAIModelStateType:   {Level: event.CapLevelFullySupported},
		},
		TypingNotifications: true,
		Edit:                event.CapLevelFullySupported,
	}
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
	log := logFromContext(ctx).With().
		Str("portal_id", string(msg.Portal.ID)).
		Str("portal_receiver", string(msg.Portal.Receiver)).
		Str("event_id", string(msg.Event.ID)).
		Int("body_len", len(msg.Content.Body)).
		Logger()
	if cl.UserLogin != nil {
		log = log.With().Str("login_id", string(cl.UserLogin.ID)).Logger()
	}
	if response, handled, err := cl.handleBridgeCommand(ctx, msg); handled {
		if err != nil {
			log.Err(err).Msg("Failed to handle Codex bridge command")
		}
		return response, err
	}
	if err := validateMatrixPromptMessage(msg); err != nil {
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
	prompt := strings.TrimSpace(msg.Content.Body)
	if prompt == "" {
		prompt = "Continue."
	}
	userDB := &database.Message{
		ID:        networkid.MessageID("user:" + string(msg.Event.ID)),
		PartID:    partID("text"),
		Room:      msg.Portal.PortalKey,
		SenderID:  cl.GetUserID(),
		Timestamp: matrixEventTime(msg.Event),
		Metadata:  &MessageMetadata{Role: "user", ThreadID: meta.ThreadID, StreamStatus: "done"},
	}
	if directorySelection {
		userDB.Metadata = &MessageMetadata{Role: "command", ThreadID: meta.ThreadID, StreamStatus: "directory"}
		resp := &bridgev2.MatrixMessageResponse{DB: userDB, StreamOrder: userDB.Timestamp.UnixNano()}
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
	resp := &bridgev2.MatrixMessageResponse{DB: userDB, StreamOrder: userDB.Timestamp.UnixNano()}
	if active := cl.Main.activeRun(meta.ThreadID); active != nil {
		setMessageMetadataTurnID(userDB, active.turnID)
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

func (cl *Client) startDirectorySelection(startKey string, portal *bridgev2.Portal, cwd string, messageID networkid.MessageID, part networkid.PartID) {
	defer cl.Main.finishMatrixStart(startKey)
	ctx := context.Background()
	if portal = cl.freshPortal(ctx, portal); portal == nil {
		logFromContext(ctx).Err(errors.New("missing Codex portal")).Str("cwd", cwd).Msg("Failed to start Codex directory selection")
		return
	}
	thread, err := cl.startThreadForPortal(ctx, cwd, portal)
	if err != nil {
		logFromContext(ctx).Err(err).Str("cwd", cwd).Msg("Failed to start Codex session from directory selection")
		cl.queueCommandNotice(portal, "", "Failed to start Codex session in "+cwd+":\n\n"+err.Error())
		return
	}
	if synced := cl.syncThreadPortal(ctx, portal, thread); synced != nil {
		portal = synced
	}
	cl.updateStoredDirectorySelectionMetadata(ctx, portal.PortalKey.Receiver, messageID, part, portal.PortalKey, thread.ID)
	cl.queueCommandNotice(portal, thread.ID, "Started Codex session in "+thread.Cwd+".")
}

func validateMatrixPromptMessage(msg *bridgev2.MatrixMessage) error {
	if msg == nil || msg.Content == nil {
		return fmt.Errorf("missing Matrix message")
	}
	if msg.ThreadRoot != nil || msg.ReplyTo != nil {
		return fmt.Errorf("%w: replies and Matrix threads are not supported by Codex prompts", bridgev2.ErrUnsupportedMessageType)
	}
	switch msg.Content.MsgType {
	case "", event.MsgText:
		return nil
	case event.MsgNotice:
		return bridgev2.ErrIgnoringMNotice
	default:
		return fmt.Errorf("%w: %s", bridgev2.ErrUnsupportedMessageType, msg.Content.MsgType)
	}
}

func (cl *Client) HandleMatrixEdit(ctx context.Context, msg *bridgev2.MatrixEdit) error {
	if err := cl.handleMatrixEdit(ctx, msg); err != nil {
		cl.logMatrixMessageError(&bridgev2.MatrixMessage{MatrixEventBase: msg.MatrixEventBase}, err, "Codex Matrix edit failed")
		return matrixMessageStatusForCodexError(err)
	}
	return nil
}

func (cl *Client) handleMatrixEdit(ctx context.Context, msg *bridgev2.MatrixEdit) error {
	if msg == nil || msg.Portal == nil || msg.Content == nil || msg.EditTarget == nil {
		return fmt.Errorf("missing Matrix edit")
	}
	if cl == nil || cl.Main == nil || cl.UserLogin == nil {
		return fmt.Errorf("missing Codex bridge login")
	}
	if err := validateMatrixPromptEdit(msg); err != nil {
		return err
	}
	meta, _ := msg.EditTarget.Metadata.(*MessageMetadata)
	if meta == nil || meta.Role != "user" || meta.ThreadID == "" || meta.TurnID == "" {
		return fmt.Errorf("%w: only previously sent Codex prompts can be edited", bridgev2.ErrUnsupportedMessageType)
	}
	prompt := strings.TrimSpace(msg.Content.Body)
	if prompt == "" {
		prompt = "Continue."
	}
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

func validateMatrixPromptEdit(msg *bridgev2.MatrixEdit) error {
	if msg == nil || msg.Content == nil {
		return fmt.Errorf("missing Matrix edit")
	}
	switch msg.Content.MsgType {
	case "", event.MsgText:
		return nil
	case event.MsgNotice:
		return bridgev2.ErrIgnoringMNotice
	default:
		return fmt.Errorf("%w: %s", bridgev2.ErrUnsupportedMessageType, msg.Content.MsgType)
	}
}

func matrixStartKey(loginID networkid.UserLoginID, portalKey networkid.PortalKey, eventID id.EventID) string {
	return string(loginID) + "\x00" + string(portalKey.ID) + "\x00" + string(portalKey.Receiver) + "\x00" + string(eventID)
}

func setMessageMetadataTurnID(msg *database.Message, turnID string) {
	if msg == nil || turnID == "" {
		return
	}
	meta, _ := msg.Metadata.(*MessageMetadata)
	if meta == nil {
		meta = &MessageMetadata{}
	}
	meta.TurnID = turnID
	msg.Metadata = meta
}

func setMessageMetadataThreadID(msg *database.Message, threadID string) {
	if msg == nil || threadID == "" {
		return
	}
	meta, _ := msg.Metadata.(*MessageMetadata)
	if meta == nil {
		meta = &MessageMetadata{}
	}
	meta.ThreadID = threadID
	msg.Metadata = meta
}

func (cl *Client) steerMatrixTurn(startKey string, portal *bridgev2.Portal, threadID, turnID, clientUserMessageID, prompt string) {
	defer cl.Main.finishMatrixStart(startKey)
	ctx := context.Background()
	if err := cl.steerTurn(ctx, threadID, turnID, clientUserMessageID, prompt); err != nil {
		logFromContext(ctx).Err(err).Str("thread_id", threadID).Str("turn_id", turnID).Msg("Failed to steer Codex turn from Matrix message")
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
		logFromContext(ctx).Err(errors.New("missing Codex portal")).Str("thread_id", meta.ThreadID).Msg("Failed to start Codex turn from Matrix message")
		return
	}
	threadID := meta.ThreadID
	if !startedThread && !cl.Main.isWarmThread(meta.ThreadID) {
		thread, err := cl.resumeThreadForPortal(ctx, portal, meta)
		if isThreadNotFoundError(err) {
			oldThreadID := meta.ThreadID
			thread, err = cl.replaceMissingThread(ctx, portal, meta)
			if err == nil {
				logFromContext(ctx).Warn().Str("old_thread_id", oldThreadID).Str("thread_id", thread.ID).Msg("Replaced missing Codex thread before Matrix turn")
			}
		}
		if err != nil {
			logFromContext(ctx).Err(err).Str("thread_id", meta.ThreadID).Msg("Failed to resume Codex thread for Matrix message")
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
		} else {
			meta.ThreadID = thread.ID
			meta.Cwd = thread.Cwd
			threadID = thread.ID
			if synced := cl.syncThreadPortal(ctx, portal, thread); synced != nil {
				portal = synced
			}
			cl.updateStoredUserMessageMetadata(ctx, portal.PortalKey.Receiver, messageID, part, threadID, "")
			run, err = cl.startTurn(ctx, portal.PortalKey, threadID, clientUserMessageID, prompt)
			if err == nil {
				logFromContext(ctx).Warn().Str("old_thread_id", oldThreadID).Str("thread_id", threadID).Msg("Recovered missing Codex thread and retried Matrix message")
			}
		}
	}
	if err != nil {
		logFromContext(ctx).Err(err).Str("thread_id", threadID).Msg("Failed to start Codex turn from Matrix message")
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
		if err := cl.Main.request(ctx, "turn/interrupt", map[string]any{"threadId": threadID, "turnId": active.turnID}, nil); err != nil {
			return fmt.Errorf("failed to stop active Codex turn before edit: %w", err)
		}
		active.writeCodexClientRequestState("turn/interrupt", codexClientTurnRequestState(threadID, active.turnID, "", ""))
		cl.Main.setActive(threadID, nil)
	}
	if err := cl.rollbackThreadToTurn(ctx, threadID, meta.TurnID); err != nil {
		return fmt.Errorf("failed to roll back Codex thread for edit: %w", err)
	}
	if err := cl.redactAndDeleteMessagesAfter(ctx, portal, target); err != nil {
		logFromContext(ctx).Warn().Err(err).Str("thread_id", threadID).Msg("Failed to remove stale messages after Codex edit")
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
		logFromContext(ctx).Err(err).Str("thread_id", threadID).Msg("Failed to start Codex turn from Matrix edit")
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
	if numTurns <= 0 {
		return nil
	}
	var resp appserver.ThreadRollbackResponse
	if err := cl.Main.request(ctx, "thread/rollback", map[string]any{"threadId": threadID, "numTurns": numTurns}, &resp); err != nil {
		return err
	}
	return nil
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
				logFromContext(ctx).Warn().Err(err).Str("turn_id", meta.TurnID).Msg("Failed to delete stale Codex active stream record after edit")
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
		logFromContext(ctx).Warn().Err(err).Stringer("portal_key", portal.PortalKey).Msg("Failed to reload Codex portal")
		return portal
	}
	if fresh == nil {
		return portal
	}
	return fresh
}

func (cl *Client) updateStoredUserMessageMetadata(ctx context.Context, receiver networkid.UserLoginID, messageID networkid.MessageID, part networkid.PartID, threadID, turnID string) {
	if cl == nil || cl.Main == nil || cl.Main.Bridge == nil || cl.Main.Bridge.DB == nil || messageID == "" {
		return
	}
	for attempt := 0; attempt < 40; attempt++ {
		msg, err := cl.Main.Bridge.DB.Message.GetPartByID(ctx, receiver, messageID, part)
		if err == nil && msg != nil {
			setMessageMetadataThreadID(msg, threadID)
			setMessageMetadataTurnID(msg, turnID)
			if err = cl.Main.Bridge.DB.Message.Update(ctx, msg); err != nil {
				logFromContext(ctx).Warn().Err(err).Str("message_id", string(messageID)).Msg("Failed to update Codex user message metadata")
			}
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(50 * time.Millisecond):
		}
	}
	logFromContext(ctx).Debug().Str("message_id", string(messageID)).Msg("Codex user message row was not stored before metadata update timeout")
}

func (cl *Client) updateStoredDirectorySelectionMetadata(ctx context.Context, receiver networkid.UserLoginID, messageID networkid.MessageID, part networkid.PartID, portalKey networkid.PortalKey, threadID string) {
	if cl == nil || cl.Main == nil || cl.Main.Bridge == nil || cl.Main.Bridge.DB == nil || messageID == "" {
		return
	}
	for attempt := 0; attempt < 40; attempt++ {
		msg, err := cl.Main.Bridge.DB.Message.GetPartByID(ctx, receiver, messageID, part)
		if err == nil && msg != nil {
			msg.Room = portalKey
			meta, _ := msg.Metadata.(*MessageMetadata)
			if meta == nil {
				meta = &MessageMetadata{}
			}
			meta.Role = "command"
			meta.ThreadID = threadID
			meta.StreamStatus = "directory"
			msg.Metadata = meta
			if err = cl.Main.Bridge.DB.Message.Update(ctx, msg); err != nil {
				logFromContext(ctx).Warn().Err(err).Str("message_id", string(messageID)).Msg("Failed to update Codex directory selection metadata")
			}
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(50 * time.Millisecond):
		}
	}
	logFromContext(ctx).Debug().Str("message_id", string(messageID)).Msg("Codex directory selection row was not stored before metadata update timeout")
}

func (cl *Client) queueAsyncTurnFailure(ctx context.Context, portal *bridgev2.Portal, threadID, text string) {
	if portal = cl.freshPortal(ctx, portal); portal != nil {
		cl.queueCommandNotice(portal, threadID, text)
	}
}

func (cl *Client) HandleMatrixRoomName(ctx context.Context, msg *bridgev2.MatrixRoomName) (bool, error) {
	if msg == nil || msg.Portal == nil || msg.Content == nil {
		return false, nil
	}
	name := strings.TrimSpace(msg.Content.Name)
	meta := portalMetadata(msg.Portal.Metadata)
	if meta.ThreadID != "" {
		if err := cl.Main.request(ctx, "thread/name/set", map[string]any{
			"threadId": meta.ThreadID,
			"name":     name,
		}, nil); err != nil {
			return false, err
		}
	}
	msg.Portal.Name = name
	msg.Portal.NameSet = true
	return true, nil
}

func (cl *Client) HandleMatrixRoomTopic(ctx context.Context, msg *bridgev2.MatrixRoomTopic) (bool, error) {
	if msg == nil || msg.Portal == nil || msg.Content == nil {
		return false, nil
	}
	topic := strings.TrimSpace(msg.Content.Topic)
	msg.Portal.Topic = topic
	msg.Portal.TopicSet = true
	return true, nil
}

func (cl *Client) HandleMatrixRoomState(ctx context.Context, msg *bridgev2.MatrixRoomState) (bool, error) {
	if msg == nil || msg.Portal == nil {
		return false, nil
	}
	switch msg.Type.Type {
	case beeperAIModelStateType, codexThreadStateType:
	default:
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

func starterThreadCWD(meta *PortalMetadata, portal *bridgev2.Portal, body string) (string, bool) {
	if meta != nil && meta.Cwd != "" {
		return meta.Cwd, false
	}
	if portal != nil {
		if cwd, ok := parseProjectPortalID(portal.ID); ok {
			return cwd, false
		}
	}
	cwd := strings.TrimSpace(body)
	return cwd, cwd != ""
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
	identifier = strings.TrimSpace(identifier)
	log := logFromContext(ctx).With().
		Str("identifier", identifier).
		Bool("create_chat", createChat).
		Logger()
	if cl != nil && cl.UserLogin != nil {
		log = log.With().Str("login_id", string(cl.UserLogin.ID)).Logger()
	}
	log.Debug().Msg("Resolving Codex identifier")
	if identifier == "" ||
		strings.EqualFold(identifier, "new") ||
		strings.EqualFold(identifier, "new project") ||
		strings.EqualFold(identifier, string(newProjectUserID)) {
		resp := cl.resolveUser(ctx, newProjectUserID, codexUserInfo("New Project", false))
		if createChat {
			resp.Chat = cl.newProjectChat("New Project")
		}
		log.Debug().Str("user_id", string(resp.UserID)).Bool("has_chat", resp.Chat != nil).Msg("Resolved Codex identifier")
		return resp, nil
	}
	if strings.EqualFold(identifier, "codex") {
		resp := cl.resolveUser(ctx, codexUserID, codexUserInfo("Codex", false))
		if createChat {
			resp.Chat = cl.newProjectChat("New Project")
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

func (cl *Client) resolveProject(ctx context.Context, cwd string, createChat bool) (*bridgev2.ResolveIdentifierResponse, error) {
	cwd, err := cleanProjectDir(cwd)
	if err != nil {
		return nil, err
	}
	name := directoryName(cwd)
	threadID := ""
	var state map[string]any
	thread, ok := cl.latestThreadForDirectory(ctx, cwd)
	if ok {
		name = threadName(thread)
		threadID = thread.ID
		state = codexThreadInitialState(thread)
	}
	resp := &bridgev2.ResolveIdentifierResponse{
		UserID:   projectUserID(cwd),
		UserInfo: projectUserInfo(thread, cwd, name),
	}
	resp = cl.resolveUser(ctx, resp.UserID, resp.UserInfo)
	if createChat {
		resp.Chat = cl.chatForProject(ctx, cwd, name, threadID, state)
	}
	return resp, nil
}

func (cl *Client) CreateChatWithGhost(ctx context.Context, ghost *bridgev2.Ghost) (*bridgev2.CreateChatResponse, error) {
	if ghost == nil {
		return nil, fmt.Errorf("missing ghost")
	}
	log := logFromContext(ctx).With().Str("ghost_id", string(ghost.ID)).Logger()
	if cl != nil && cl.UserLogin != nil {
		log = log.With().Str("login_id", string(cl.UserLogin.ID)).Logger()
	}
	log.Debug().Msg("Creating Codex chat with ghost")
	switch ghost.ID {
	case newProjectUserID, codexUserID:
		resp := cl.newProjectChat("New Project")
		log.Debug().Stringer("portal_key", resp.PortalKey).Msg("Created Codex starter chat with ghost")
		return resp, nil
	default:
		cwd, ok := parseProjectUserID(ghost.ID)
		if !ok {
			log.Debug().Msg("Rejected unknown Codex ghost")
			return nil, fmt.Errorf("unknown Codex ghost %s", ghost.ID)
		}
		name := directoryName(cwd)
		threadID := ""
		var state map[string]any
		if thread, ok := cl.latestThreadForDirectory(ctx, cwd); ok {
			name = threadName(thread)
			threadID = thread.ID
			state = codexThreadInitialState(thread)
		}
		resp := cl.chatForProject(ctx, cwd, name, threadID, state)
		log.Debug().Str("cwd", cwd).Str("thread_id", threadID).Stringer("portal_key", resp.PortalKey).Msg("Created Codex project chat with ghost")
		return resp, nil
	}
}

func (cl *Client) GetContactList(ctx context.Context) ([]*bridgev2.ResolveIdentifierResponse, error) {
	log := *logFromContext(ctx)
	if cl != nil && cl.UserLogin != nil {
		log = log.With().Str("login_id", string(cl.UserLogin.ID)).Logger()
	}
	log.Debug().Msg("Listing Codex contacts")
	if cl.Main == nil {
		contacts := cl.contactsForThreads(ctx, nil)
		log.Debug().Int("contacts", len(contacts)).Msg("Listed Codex contacts without app-server")
		return contacts, nil
	}
	threads, err := cl.listThreads(ctx, "", 100)
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
		cl.resolveUser(ctx, newProjectUserID, codexUserInfo("New Project", false)),
	}
	for _, thread := range sortedRecentDirectories(threads) {
		contact := cl.resolveUser(ctx, projectUserID(thread.Cwd), projectUserInfo(thread, thread.Cwd, directoryName(thread.Cwd)))
		contact.Chat = cl.existingChatForProject(ctx, thread.Cwd, directoryName(thread.Cwd), thread.ID, codexThreadInitialState(thread))
		contacts = append(contacts, contact)
	}
	return contacts
}

func (cl *Client) resolveUser(ctx context.Context, userID networkid.UserID, info *bridgev2.UserInfo) *bridgev2.ResolveIdentifierResponse {
	resp := &bridgev2.ResolveIdentifierResponse{UserID: userID, UserInfo: info}
	if cl == nil || cl.UserLogin == nil || cl.UserLogin.Bridge == nil || !isContactGhost(userID) {
		return resp
	}
	ghost, err := cl.UserLogin.Bridge.GetGhostByID(ctx, userID)
	if err != nil {
		logFromContext(ctx).Warn().Err(err).Str("user_id", string(userID)).Msg("Failed to resolve Codex ghost")
		return resp
	}
	resp.Ghost = ghost
	return resp
}

func isContactGhost(userID networkid.UserID) bool {
	if userID == codexUserID || userID == newProjectUserID {
		return true
	}
	_, ok := parseProjectUserID(userID)
	return ok
}

func projectUserInfo(thread appserver.Thread, cwd, name string) *bridgev2.UserInfo {
	return codexUserInfo(name, false, cwd, thread.ID, thread.Name)
}

func (cl *Client) SearchUsers(ctx context.Context, query string) ([]*bridgev2.ResolveIdentifierResponse, error) {
	query = strings.ToLower(strings.TrimSpace(query))
	log := logFromContext(ctx).With().Str("query", query).Logger()
	if cl != nil && cl.UserLogin != nil {
		log = log.With().Str("login_id", string(cl.UserLogin.ID)).Logger()
	}
	log.Debug().Msg("Searching Codex contacts")
	contacts, err := cl.GetContactList(ctx)
	if err != nil || query == "" {
		if err != nil {
			log.Debug().Err(err).Msg("Failed to search Codex contacts")
		} else {
			log.Debug().Int("results", len(contacts)).Msg("Search query empty, returning all Codex contacts")
		}
		return contacts, err
	}
	filtered := contacts[:0]
	for _, contact := range contacts {
		name := ""
		if contact.UserInfo != nil && contact.UserInfo.Name != nil {
			name = strings.ToLower(*contact.UserInfo.Name)
		}
		if strings.Contains(name, query) || strings.Contains(strings.ToLower(string(contact.UserID)), query) || identifiersContain(contact.UserInfo, query) {
			filtered = append(filtered, contact)
		}
	}
	log.Debug().Int("contacts", len(contacts)).Int("results", len(filtered)).Msg("Searched Codex contacts")
	return filtered, nil
}

func identifiersContain(info *bridgev2.UserInfo, query string) bool {
	if info == nil {
		return false
	}
	for _, identifier := range info.Identifiers {
		if strings.Contains(strings.ToLower(identifier), query) {
			return true
		}
	}
	return false
}

func (cl *Client) newProjectChat(name string) *bridgev2.CreateChatResponse {
	loginID := defaultLoginID
	if cl.UserLogin != nil {
		loginID = cl.UserLogin.ID
	}
	return &bridgev2.CreateChatResponse{
		PortalKey:      newProjectPortalKey(loginID),
		PortalInfo:     cl.newProjectChatInfo(name),
		DMRedirectedTo: codexUserID,
	}
}

func (cl *Client) newProjectChatInfo(name string) *bridgev2.ChatInfo {
	roomType := database.RoomTypeDM
	return &bridgev2.ChatInfo{
		Name:                       stringPtr(name),
		Topic:                      stringPtr(newProjectPrompt),
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
		if cl == nil || cl.UserLogin == nil || cl.UserLogin.Bridge == nil || portal == nil || portal.MXID == "" {
			return false
		}
		meta := portalMetadata(portal.Metadata)
		if meta.NewProjectIntroMessage || meta.ThreadID != "" || meta.Cwd != "" {
			return false
		}
		meta.NewProjectIntroMessage = true
		portal.Metadata = meta
		go cl.queueNewProjectIntroMessage(context.WithoutCancel(ctx), portal.PortalKey)
		return true
	}
}

func (cl *Client) queueNewProjectIntroMessage(ctx context.Context, portalKey networkid.PortalKey) {
	now := time.Now()
	msgID := networkid.MessageID("new-project-intro:" + string(portalKey.ID))
	run := commandNoticeRun(newProjectIntroText, string(msgID), string(portalKey.ID), activeRunInitialModel(cl, ""), now)
	res := cl.UserLogin.QueueRemoteEvent(&simplevent.Message[aistream.Run]{
		EventMeta: remoteEventMeta(bridgev2.RemoteEventMessage, portalKey, codexUserID, now),
		ID:        msgID,
		Data:      run,
		ConvertMessageFunc: func(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, data aistream.Run) (*bridgev2.ConvertedMessage, error) {
			content, extra, err := matrixFinalContentWithAttachment(ctx, portal, intent, data)
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

func (cl *Client) chatForProject(ctx context.Context, cwd, name, threadID string, state map[string]any) *bridgev2.CreateChatResponse {
	loginID := defaultLoginID
	if cl.UserLogin != nil {
		loginID = cl.UserLogin.ID
	}
	key := projectPortalKey(cwd, loginID)
	var portal *bridgev2.Portal
	if cl.Main != nil && cl.Main.Bridge != nil {
		portal = cl.existingProjectPortal(ctx, cwd, key)
	}
	if portal != nil {
		meta := portalMetadata(portal.Metadata)
		oldThreadID := meta.ThreadID
		if cwd != "" {
			meta.Cwd = cwd
		}
		if threadID != "" {
			meta.ThreadID = threadID
		}
		portal.Metadata = meta
		_ = portal.Save(ctx)
		if meta.ThreadID != "" {
			cl.Main.rememberThreadRoom(meta.ThreadID, cl, portal.PortalKey, meta.Cwd)
		}
		if oldThreadID != meta.ThreadID {
			cl.resetBackfillTask(ctx, portal)
		}
	}
	if threadID != "" && cl.Main != nil {
		cl.Main.rememberThreadRoom(threadID, cl, key, cwd, firstStateString(state, "modelProvider"), codexModelStateRef(state, ""), firstStateString(state, "effort", "reasoning", "reasoningEffort", "reasoning_effort"), firstStateString(state, "modelName", "name"), firstStateString(state, "reasoning_mode", "reasoningMode"))
	}
	return &bridgev2.CreateChatResponse{
		PortalKey:      key,
		Portal:         portal,
		PortalInfo:     portalInfo(name, cl.codexMembers(), cwd, threadID, state),
		DMRedirectedTo: codexUserID,
	}
}

func (cl *Client) existingChatForProject(ctx context.Context, cwd, name, threadID string, state map[string]any) *bridgev2.CreateChatResponse {
	chat := cl.chatForProject(ctx, cwd, name, threadID, state)
	if chat == nil || chat.Portal == nil || chat.Portal.MXID == "" {
		return nil
	}
	return chat
}

func (cl *Client) existingProjectPortal(ctx context.Context, cwd string, key networkid.PortalKey) *bridgev2.Portal {
	if cl == nil || cl.Main == nil || cl.Main.Bridge == nil {
		return nil
	}
	bridge := cl.Main.Bridge
	portal, err := bridge.GetExistingPortalByKey(ctx, key)
	if err != nil {
		logFromContext(ctx).Warn().Err(err).Stringer("portal_key", key).Msg("Failed to load existing Codex project portal")
	}
	if portal != nil && portal.MXID != "" {
		return portal
	}
	if cwd == "" {
		return portal
	}
	portals, err := bridge.GetAllPortals(ctx)
	if err != nil {
		logFromContext(ctx).Warn().Err(err).Str("cwd", cwd).Msg("Failed to scan Codex project portals")
		return portal
	}
	for _, candidate := range portals {
		if candidate == nil || candidate.PortalKey == key || candidate.PortalKey.Receiver != key.Receiver || candidate.MXID == "" {
			continue
		}
		if portalMetadata(candidate.Metadata).Cwd != cwd {
			continue
		}
		result, reIDPortal, err := bridge.ReIDPortal(ctx, candidate.PortalKey, key)
		if err != nil {
			logFromContext(ctx).Warn().Err(err).
				Stringer("source_portal_key", candidate.PortalKey).
				Stringer("target_portal_key", key).
				Msg("Failed to canonicalize Codex project portal")
			return portal
		}
		logFromContext(ctx).Info().
			Int("result", int(result)).
			Stringer("source_portal_key", candidate.PortalKey).
			Stringer("target_portal_key", key).
			Msg("Canonicalized Codex project portal")
		if reIDPortal != nil {
			return reIDPortal
		}
		return portal
	}
	return portal
}

func (cl *Client) latestThreadForDirectory(ctx context.Context, cwd string) (appserver.Thread, bool) {
	if cl == nil || cl.Main == nil {
		return appserver.Thread{}, false
	}
	threads, err := cl.listThreads(ctx, "", 100)
	if err != nil {
		return appserver.Thread{}, false
	}
	var latest appserver.Thread
	for _, thread := range threads {
		if thread.Cwd != cwd {
			continue
		}
		if latest.ID == "" || thread.UpdatedAt > latest.UpdatedAt {
			latest = thread
		}
	}
	return latest, latest.ID != ""
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
				MemberEventExtra: syntheticMemberEventExtra(),
			},
			codexUserID: {
				EventSender:      bridgev2.EventSender{Sender: codexUserID},
				Membership:       event.MembershipJoin,
				UserInfo:         codexUserInfo("Codex", false),
				MemberEventExtra: syntheticMemberEventExtra(),
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
				for _, eventType := range []event.Type{
					event.StateBeeperDisappearingTimer,
					event.StateMSC4391BotCommand,
					roomStateEventType(codexThreadStateType),
				} {
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

func syntheticMemberEventExtra() map[string]any {
	return map[string]any{"com.beeper.exclude_from_timeline": true}
}

func (cl *Client) listThreads(ctx context.Context, cursor string, limit int) ([]appserver.Thread, error) {
	if limit <= 0 {
		limit = 100
	}
	var out []appserver.Thread
	seen := map[string]bool{}
	for {
		page, next, err := cl.listThreadPage(ctx, cursor, limit)
		if err != nil {
			return nil, err
		}
		out = append(out, page...)
		if next == "" || seen[next] {
			return out, nil
		}
		seen[next] = true
		cursor = next
	}
}

func (cl *Client) listThreadPage(ctx context.Context, cursor string, limit int) ([]appserver.Thread, string, error) {
	params := map[string]any{"limit": limit, "sortKey": "updated_at", "sortDirection": "desc"}
	if cursor != "" {
		params["cursor"] = cursor
	}
	var resp appserver.ThreadListResponse
	if err := cl.Main.request(ctx, "thread/list", params, &resp); err != nil {
		return nil, "", err
	}
	return resp.Data, resp.NextCursor, nil
}

func (cl *Client) readThread(ctx context.Context, threadID string, includeTurns bool) (appserver.Thread, error) {
	var resp appserver.ThreadReadResponse
	err := cl.Main.request(ctx, "thread/read", map[string]any{"threadId": threadID, "includeTurns": includeTurns}, &resp)
	return resp.Thread, err
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
		params := map[string]any{
			"threadId":      threadID,
			"limit":         100,
			"sortDirection": "asc",
			"itemsView":     "full",
		}
		if cursor != "" {
			params["cursor"] = cursor
		}
		var resp appserver.ThreadTurnsListResponse
		if err := cl.Main.request(ctx, "thread/turns/list", params, &resp); err != nil {
			return nil, err
		}
		out = append(out, resp.Data...)
		if resp.NextCursor == "" || seen[resp.NextCursor] {
			return out, nil
		}
		seen[resp.NextCursor] = true
		cursor = resp.NextCursor
	}
}

func (cl *Client) startThread(ctx context.Context, cwd string) (appserver.Thread, error) {
	return cl.startThreadForPortal(ctx, cwd, nil)
}

func (cl *Client) startThreadForPortal(ctx context.Context, cwd string, portal *bridgev2.Portal) (appserver.Thread, error) {
	var err error
	if cwd, err = cleanProjectDir(cwd); err != nil {
		return appserver.Thread{}, err
	}
	params := map[string]any{
		"cwd":            cwd,
		"approvalPolicy": "on-request",
		"threadSource":   "user",
	}
	cl.applyValidatedRoomThreadSettings(ctx, params, cl.roomAIModelState(ctx, portal), portal, networkid.PortalKey{})
	var resp appserver.ThreadOpenResponse
	if err := cl.Main.request(ctx, "thread/start", params, &resp); err != nil {
		return appserver.Thread{}, err
	}
	thread := resp.HydratedThread()
	cl.Main.rememberWarmThread(thread.ID)
	return thread, nil
}

func (cl *Client) resumeThreadForPortal(ctx context.Context, portal *bridgev2.Portal, meta *PortalMetadata) (appserver.Thread, error) {
	if meta == nil || meta.ThreadID == "" {
		return appserver.Thread{}, fmt.Errorf("missing Codex thread id")
	}
	params := map[string]any{
		"threadId":       meta.ThreadID,
		"approvalPolicy": "on-request",
		"excludeTurns":   true,
	}
	if cwd := threadCwdForPortal(portal, meta); cwd != "" {
		cleaned, err := cleanProjectDir(cwd)
		if err == nil {
			params["cwd"] = cleaned
		} else {
			logFromContext(ctx).Warn().Err(err).Str("thread_id", meta.ThreadID).Str("cwd", cwd).Msg("Omitting unavailable cwd while resuming Codex thread")
		}
	}
	cl.applyValidatedRoomThreadSettings(ctx, params, cl.roomAIModelState(ctx, portal), portal, networkid.PortalKey{})
	var resp appserver.ThreadOpenResponse
	if err := cl.Main.request(ctx, "thread/resume", params, &resp); err != nil {
		return appserver.Thread{}, err
	}
	thread := resp.HydratedThread()
	cl.Main.rememberWarmThread(thread.ID)
	return thread, nil
}

func (cl *Client) replaceMissingThread(ctx context.Context, portal *bridgev2.Portal, meta *PortalMetadata) (appserver.Thread, error) {
	cwd := ""
	if meta != nil {
		cwd = meta.Cwd
	}
	if cwd == "" && portal != nil {
		cwd, _ = parseProjectPortalID(portal.ID)
	}
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
	threadID := firstStateString(state, "threadId")
	if threadID == "" {
		threadID = firstStateString(state, "sessionId")
	}
	if threadID == "" {
		return meta
	}
	cwd := firstStateString(state, "cwd")
	if cwd == "" {
		cwd = meta.Cwd
	}
	cl.setPortalThreadMetadata(ctx, portal, threadID, cwd)
	meta = portalMetadata(portal.Metadata)
	if cl.Main != nil {
		cl.Main.rememberThreadRoom(threadID, cl, portal.PortalKey, meta.Cwd)
	}
	logFromContext(ctx).Info().Str("thread_id", threadID).Str("cwd", meta.Cwd).Msg("Hydrated Codex portal metadata from room state")
	return meta
}

func (cl *Client) roomModel(ctx context.Context, portal *bridgev2.Portal) string {
	return firstStateString(cl.roomAIModelState(ctx, portal), "model")
}

func (cl *Client) roomAIModelState(ctx context.Context, portal *bridgev2.Portal) map[string]any {
	if cl == nil || cl.Main == nil || portal == nil {
		return nil
	}
	state := cl.Main.modelStateForPortalKey(portal.PortalKey)
	if cl.Main.Bridge == nil || portal.MXID == "" {
		return state
	}
	reader, ok := cl.Main.Bridge.Matrix.(bridgev2.MatrixConnectorWithArbitraryRoomState)
	if !ok {
		return state
	}
	roomState, err := readRawRoomState(ctx, reader, portal.MXID, beeperAIModelStateType)
	if err != nil {
		logFromContext(ctx).Warn().Err(err).Str("room_id", string(portal.MXID)).Msg("Failed to read Codex room model state")
		return state
	}
	if len(roomState) == 0 {
		return state
	}
	state = mergeModelState(state, roomState)
	return state
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

func mergeModelState(base, override map[string]any) map[string]any {
	if len(base) == 0 {
		return override
	}
	if len(override) == 0 {
		return base
	}
	merged := make(map[string]any, len(base)+len(override))
	for key, value := range base {
		merged[key] = value
	}
	for key, value := range override {
		merged[key] = value
	}
	return merged
}

func (cl *Client) roomCodexThreadState(ctx context.Context, portal *bridgev2.Portal) map[string]any {
	if cl == nil || cl.Main == nil || cl.Main.Bridge == nil || portal == nil || portal.MXID == "" {
		return nil
	}
	reader, ok := cl.Main.Bridge.Matrix.(bridgev2.MatrixConnectorWithArbitraryRoomState)
	if !ok {
		return nil
	}
	state, err := readRawRoomState(ctx, reader, portal.MXID, codexThreadStateType)
	if err != nil {
		logFromContext(ctx).Warn().Err(err).Str("room_id", string(portal.MXID)).Msg("Failed to read Codex thread room state")
		return nil
	}
	return state
}

func readRawRoomState(ctx context.Context, reader bridgev2.MatrixConnectorWithArbitraryRoomState, roomID id.RoomID, stateType string) (map[string]any, error) {
	evt, err := reader.GetStateEvent(ctx, roomID, event.Type{Type: stateType, Class: event.StateEventType}, "")
	if errors.Is(err, mautrix.MNotFound) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
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
	var rpcErr *appserver.RPCError
	if !errors.As(err, &rpcErr) {
		return false
	}
	msg := strings.ToLower(rpcErr.Msg)
	return strings.Contains(msg, "thread not found") || strings.Contains(msg, "thread not loaded")
}

func isThreadTurnsListUnavailable(err error) bool {
	var rpcErr *appserver.RPCError
	if !errors.As(err, &rpcErr) {
		return false
	}
	msg := strings.ToLower(rpcErr.Msg)
	return rpcErr.Code == -32601 ||
		strings.Contains(msg, "method not found") ||
		strings.Contains(msg, "unknown method") ||
		strings.Contains(msg, "experimental")
}

func threadCwdForPortal(portal *bridgev2.Portal, meta *PortalMetadata) string {
	if meta != nil && meta.Cwd != "" {
		return meta.Cwd
	}
	if portal != nil {
		if cwd, ok := parseProjectPortalID(portal.ID); ok {
			return cwd
		}
	}
	return ""
}

func cleanProjectDir(path string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", nil
	}
	if strings.HasPrefix(path, "~/") || path == "~" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		if path == "~" {
			path = home
		} else {
			path = filepath.Join(home, strings.TrimPrefix(path, "~/"))
		}
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	if stat, err := os.Stat(abs); err != nil {
		return "", fmt.Errorf("directory %s is not available: %w", abs, err)
	} else if !stat.IsDir() {
		return "", fmt.Errorf("%s is not a directory", abs)
	}
	return abs, nil
}

func (cl *Client) syncThreadPortal(ctx context.Context, portal *bridgev2.Portal, thread appserver.Thread) *bridgev2.Portal {
	if portal == nil || thread.ID == "" {
		return portal
	}
	if thread.Cwd != "" && cl != nil && cl.UserLogin != nil {
		target := projectPortalKey(thread.Cwd, cl.UserLogin.ID)
		if portal.PortalKey != target && cl.Main != nil && cl.Main.Bridge != nil {
			result, synced, err := cl.Main.Bridge.ReIDPortal(ctx, portal.PortalKey, target)
			if err != nil {
				logFromContext(ctx).Warn().Err(err).
					Stringer("source_portal_key", portal.PortalKey).
					Stringer("target_portal_key", target).
					Msg("Failed to canonicalize Codex starter portal")
			} else {
				logFromContext(ctx).Info().
					Int("result", int(result)).
					Stringer("target_portal_key", target).
					Msg("Canonicalized Codex starter portal")
				if synced != nil {
					portal = synced
				}
			}
		}
	}
	cl.setPortalThreadMetadata(ctx, portal, thread.ID, thread.Cwd)
	cl.ensureBackfillVersion(ctx, portal)
	name := threadName(thread)
	state := enrichThreadStateWithModelState(codexThreadInitialState(thread), cl.roomAIModelState(ctx, portal))
	cl.Main.rememberThreadRoom(thread.ID, cl, portal.PortalKey, thread.Cwd, firstStateString(state, "modelProvider", "provider"), codexModelStateRef(state, ""), firstStateString(state, "effort", "reasoning", "reasoningEffort", "reasoning_effort"), firstStateString(state, "modelName", "name"), firstStateString(state, "reasoning_mode", "reasoningMode"))
	info := portalInfo(name, cl.codexMembers(), thread.Cwd, thread.ID, state)
	applyStoredPortalInfo(info, portal)
	portal.UpdateInfo(ctx, info, cl.UserLogin, nil, time.Now())
	if cl.Main != nil && cl.Main.Bridge != nil {
		cl.Main.Bridge.WakeupBackfillQueue()
	}
	return portal
}

const codexBackfillVersion = 3

func (cl *Client) ensureBackfillVersion(ctx context.Context, portal *bridgev2.Portal) {
	if portal == nil || portal.MXID == "" {
		return
	}
	meta := portalMetadata(portal.Metadata)
	if meta.BackfillVersion == codexBackfillVersion {
		return
	}
	meta.BackfillVersion = codexBackfillVersion
	portal.Metadata = meta
	if err := portal.Save(ctx); err != nil {
		logFromContext(ctx).Warn().Err(err).Stringer("portal_key", portal.PortalKey).Msg("Failed to save Codex backfill version")
		return
	}
	cl.resetBackfillTask(ctx, portal)
}

func (cl *Client) setPortalThreadMetadata(ctx context.Context, portal *bridgev2.Portal, threadID, cwd string) {
	if portal == nil || threadID == "" {
		return
	}
	meta := portalMetadata(portal.Metadata)
	oldThreadID := meta.ThreadID
	changed := false
	if meta.ThreadID != threadID {
		meta.ThreadID = threadID
		changed = true
	}
	if cwd != "" && meta.Cwd != cwd {
		meta.Cwd = cwd
		changed = true
	}
	if changed {
		portal.Metadata = meta
		if err := portal.Save(ctx); err != nil {
			logFromContext(ctx).Warn().Err(err).Str("thread_id", threadID).Msg("Failed to save Codex portal metadata")
			return
		}
	}
	if oldThreadID != threadID {
		cl.resetBackfillTask(ctx, portal)
	}
}

func (cl *Client) clearMissingThread(ctx context.Context, portal *bridgev2.Portal, meta *PortalMetadata) {
	if portal == nil || meta == nil || meta.ThreadID == "" {
		return
	}
	oldThreadID := meta.ThreadID
	meta.ThreadID = ""
	portal.Metadata = meta
	if err := portal.Save(ctx); err != nil {
		logFromContext(ctx).Err(err).Str("thread_id", oldThreadID).Msg("Failed to clear missing Codex thread metadata")
		return
	}
	if cl != nil && cl.Main != nil {
		cl.Main.forgetThread(oldThreadID)
	}
	logFromContext(ctx).Warn().Str("thread_id", oldThreadID).Str("cwd", meta.Cwd).Msg("Cleared missing Codex thread metadata")
}

func (cl *Client) resetBackfillTask(ctx context.Context, portal *bridgev2.Portal) {
	if cl == nil || cl.Main == nil || cl.Main.Bridge == nil || cl.UserLogin == nil || portal == nil || portal.MXID == "" {
		return
	}
	if err := cl.Main.Bridge.DB.BackfillTask.Upsert(ctx, &database.BackfillTask{
		PortalKey:         portal.PortalKey,
		UserLoginID:       cl.UserLogin.ID,
		BatchCount:        -1,
		NextDispatchMinTS: time.Now(),
	}); err != nil {
		logFromContext(ctx).Err(err).Msg("Failed to reset Codex backfill task")
		return
	}
	cl.Main.Bridge.WakeupBackfillQueue()
}

func (cl *Client) startTurn(ctx context.Context, portalKey networkid.PortalKey, threadID, clientUserMessageID, prompt string) (*activeRun, error) {
	params := map[string]any{
		"threadId":       threadID,
		"approvalPolicy": "on-request",
		"input":          turnTextInput(prompt),
	}
	if clientUserMessageID != "" {
		params["clientUserMessageId"] = clientUserMessageID
	}
	cl.applyValidatedRoomTurnSettings(ctx, params, cl.roomAIModelStateForPortalKey(ctx, portalKey), nil, portalKey)
	cl.Main.setPendingTurnStart(threadID, cl, portalKey)
	defer cl.Main.clearPendingTurnStart(threadID)
	var resp appserver.TurnStartResponse
	if err := cl.Main.request(ctx, "turn/start", params, &resp); err != nil {
		return nil, err
	}
	cl.Main.rememberWarmThread(threadID)
	if run := cl.Main.activeRun(threadID); run != nil && run.turnID == resp.Turn.ID {
		if err := run.start(ctx); err != nil {
			return nil, err
		}
		run.writeCodexClientRequestState("turn/start", codexClientTurnRequestState(threadID, resp.Turn.ID, "", clientUserMessageID))
		return run, nil
	}
	run := newActiveRun(cl, portalKey, threadID, resp.Turn.ID)
	cl.Main.setActive(threadID, run)
	if err := run.start(ctx); err != nil {
		cl.Main.setActive(threadID, nil)
		return nil, err
	}
	run.writeCodexClientRequestState("turn/start", codexClientTurnRequestState(threadID, resp.Turn.ID, "", clientUserMessageID))
	return run, nil
}

func (cl *Client) steerTurn(ctx context.Context, threadID, turnID, clientUserMessageID, prompt string) error {
	params := map[string]any{
		"threadId":       threadID,
		"expectedTurnId": turnID,
		"input":          turnTextInput(prompt),
	}
	if clientUserMessageID != "" {
		params["clientUserMessageId"] = clientUserMessageID
	}
	if err := cl.Main.request(ctx, "turn/steer", params, nil); err != nil {
		return err
	}
	if run := cl.Main.activeRun(threadID); run != nil {
		run.writeCodexClientRequestState("turn/steer", codexClientTurnRequestState(threadID, "", turnID, clientUserMessageID))
	}
	return nil
}

func (cl *Client) interruptTurn(ctx context.Context, portal *bridgev2.Portal, threadID, turnID string) {
	if err := cl.Main.request(ctx, "turn/interrupt", map[string]any{"threadId": threadID, "turnId": turnID}, nil); err != nil {
		cl.queueCommandNotice(portal, threadID, "Failed to stop Codex turn:\n\n"+err.Error())
		return
	}
	if run := cl.Main.activeRun(threadID); run != nil {
		run.writeCodexClientRequestState("turn/interrupt", codexClientTurnRequestState(threadID, turnID, "", ""))
	}
	cl.queueCommandNotice(portal, threadID, "Requested Codex to stop the active turn.")
}

func codexClientTurnRequestState(threadID, turnID, expectedTurnID, clientUserMessageID string) map[string]any {
	state := map[string]any{
		"threadId": threadID,
	}
	if turnID != "" {
		state["turnId"] = turnID
	}
	if expectedTurnID != "" {
		state["expectedTurnId"] = expectedTurnID
	}
	if clientUserMessageID != "" {
		state["clientUserMessageId"] = clientUserMessageID
	}
	return state
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
		return &bridgev2.FetchMessagesResponse{HasMore: false}, nil
	}
	meta := portalMetadata(params.Portal.Metadata)
	meta = cl.hydratePortalThreadMetadata(ctx, params.Portal, meta)
	if meta.ThreadID == "" {
		return &bridgev2.FetchMessagesResponse{HasMore: false}, nil
	}
	thread, err := cl.readThreadForBackfill(ctx, meta.ThreadID)
	if err != nil {
		if isThreadNotFoundError(err) {
			cl.clearMissingThread(ctx, params.Portal, meta)
			return &bridgev2.FetchMessagesResponse{HasMore: false}, nil
		}
		return nil, err
	}
	messages, err := cl.backfillMessages(ctx, params.Portal, thread)
	if err != nil {
		return nil, err
	}
	return paginateBackfillMessages(messages, params), nil
}

func (cl *Client) GetBackfillMaxBatchCount(ctx context.Context, portal *bridgev2.Portal, task *database.BackfillTask) int {
	if portal == nil || portal.RoomType == database.RoomTypeSpace {
		return 0
	}
	if portalMetadata(portal.Metadata).ThreadID == "" {
		return 0
	}
	return -1
}

func (cl *Client) backfillMessages(ctx context.Context, portal *bridgev2.Portal, thread appserver.Thread) ([]*bridgev2.BackfillMessage, error) {
	return cl.projectBackfillMessages(ctx, portal, thread)
}

func (cl *Client) applyValidatedRoomTurnSettings(ctx context.Context, params map[string]any, state map[string]any, portal *bridgev2.Portal, portalKey networkid.PortalKey) {
	applyRoomTurnSettings(params, state)
	delete(params, "model")
	if model := cl.validatedRequestModel(ctx, state, portal, portalKey); model != "" {
		params["model"] = model
	}
}

func (cl *Client) applyValidatedRoomThreadSettings(ctx context.Context, params map[string]any, state map[string]any, portal *bridgev2.Portal, portalKey networkid.PortalKey) {
	if params == nil {
		return
	}
	if model := cl.validatedRequestModel(ctx, state, portal, portalKey); model != "" {
		params["model"] = model
	}
	if effort := firstStateString(state, "effort", "reasoning", "reasoningEffort", "reasoning_effort"); effort != "" {
		config, _ := params["config"].(map[string]any)
		if config == nil {
			config = map[string]any{}
			params["config"] = config
		}
		config["model_reasoning_effort"] = effort
	}
}

func applyRoomTurnSettings(params map[string]any, state map[string]any) {
	if params == nil {
		return
	}
	if model := codexRequestModel(firstStateString(state, "model")); model != "" {
		params["model"] = model
	}
	if effort := firstStateString(state, "effort", "reasoning", "reasoningEffort", "reasoning_effort"); effort != "" {
		params["effort"] = effort
	}
}

func (cl *Client) validatedRequestModel(ctx context.Context, state map[string]any, portal *bridgev2.Portal, portalKey networkid.PortalKey) string {
	if cl == nil || cl.Main == nil {
		return ""
	}
	roomModel := codexModelStateRef(state, firstStateString(state, "provider", "modelProvider"))
	model := codexRequestModel(roomModel)
	fromRoomState := model != ""
	if model == "" && cl.Main.Config.DefaultModel != "" {
		model = codexRequestModel(cl.Main.Config.DefaultModel)
	}
	if model == "" {
		return ""
	}
	ok, err := cl.supportsModel(ctx, model)
	if err != nil {
		logFromContext(ctx).Warn().Err(err).Str("model", model).Msg("Failed to validate Codex model; passing it through")
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

func (cl *Client) supportsModel(ctx context.Context, model string) (bool, error) {
	if cl == nil || cl.Main == nil {
		return false, nil
	}
	model = strings.TrimSpace(model)
	if model == "" {
		return false, nil
	}
	cursor := ""
	seen := map[string]bool{}
	for {
		params := map[string]any{
			"includeHidden": true,
			"limit":         200,
		}
		if cursor != "" {
			params["cursor"] = cursor
		}
		var resp appserver.ModelListResponse
		if err := cl.Main.request(ctx, "model/list", params, &resp); err != nil {
			return false, err
		}
		for _, item := range resp.Data {
			if item.Model == model || item.ID == model {
				return true, nil
			}
		}
		if resp.NextCursor == "" || seen[resp.NextCursor] {
			return false, nil
		}
		seen[resp.NextCursor] = true
		cursor = resp.NextCursor
	}
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
			logFromContext(ctx).Warn().Err(err).Stringer("portal_key", portalKey).Msg("Failed to load portal while clearing unsupported Codex model")
		}
	}
	if portal == nil {
		return
	}
	if portal.MXID != "" {
		_, err := portal.Internal().SendStateWithIntentOrBot(ctx, nil, event.Type{Type: beeperAIModelStateType, Class: event.StateEventType}, "", &event.Content{Raw: map[string]any{}}, time.Now())
		if err != nil {
			logFromContext(ctx).Warn().Err(err).Str("model", model).Msg("Failed to clear unsupported Codex room model state")
		}
	}
	meta := portalMetadata(portal.Metadata)
	cl.queueCommandNotice(portal, meta.ThreadID, "Codex model "+model+" is not available for this login. Cleared the room model and used the Codex default.")
}

func codexRequestModel(model string) string {
	model = strings.TrimSpace(model)
	if strings.HasPrefix(model, "openai/") {
		return strings.TrimPrefix(model, "openai/")
	}
	return model
}

func threadName(thread appserver.Thread) string {
	if strings.TrimSpace(thread.Name) != "" {
		return thread.Name
	}
	if strings.TrimSpace(thread.Preview) != "" {
		return thread.Preview
	}
	return directoryName(thread.Cwd)
}

func matrixEventTime(evt *event.Event) time.Time {
	if evt == nil || evt.Timestamp == 0 {
		return time.Now()
	}
	return time.UnixMilli(evt.Timestamp)
}
