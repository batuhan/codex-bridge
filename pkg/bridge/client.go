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

var _ bridgev2.NetworkAPI = (*Client)(nil)
var _ bridgev2.NetworkAPIWithUserID = (*Client)(nil)
var _ bridgev2.IdentifierResolvingNetworkAPI = (*Client)(nil)
var _ bridgev2.GhostDMCreatingNetworkAPI = (*Client)(nil)
var _ bridgev2.ContactListingNetworkAPI = (*Client)(nil)
var _ bridgev2.UserSearchingNetworkAPI = (*Client)(nil)
var _ bridgev2.BackfillingNetworkAPI = (*Client)(nil)
var _ bridgev2.BackfillingNetworkAPIWithLimits = (*Client)(nil)
var _ bridgev2.RoomNameHandlingNetworkAPI = (*Client)(nil)
var _ bridgev2.RoomTopicHandlingNetworkAPI = (*Client)(nil)
var _ bridgev2.RoomStateHandlingNetworkAPI = (*Client)(nil)
var _ bridgev2.DisappearTimerChangingNetworkAPI = (*Client)(nil)
var _ status.BridgeStateFiller = (*Client)(nil)

func (cl *Client) Connect(ctx context.Context) {
	cl.loggedIn = true
	if cl.UserLogin != nil && cl.UserLogin.BridgeState != nil {
		cl.UserLogin.BridgeState.Send(status.BridgeState{StateEvent: status.StateConnected})
	}
	cl.failPersistedActiveStreams(ctx)
	cl.startActiveStreamJanitor(ctx)
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
			state = codexThreadInitialState(thread)
			if cl.Main != nil {
				cl.Main.rememberThreadRoom(thread.ID, cl, portal.PortalKey, thread.Cwd, thread.ModelProvider, threadModelRef(thread), threadReasoningEffort(thread))
			}
		} else {
			if meta.Cwd != "" {
				name = directoryName(meta.Cwd)
			}
			if isThreadNotFoundError(err) {
				cl.clearMissingThread(ctx, portal, meta)
			}
		}
	} else if meta.Cwd != "" {
		name = directoryName(meta.Cwd)
	}
	info := portalInfo(name, cl.codexMembers(), meta.Cwd, meta.ThreadID, state)
	if meta.ThreadID == "" && meta.Cwd == "" {
		info.Topic = stringPtr(newProjectPrompt)
	}
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
			event.StateRoomName.Type:                {Level: event.CapLevelFullySupported},
			event.StateTopic.Type:                   {Level: event.CapLevelFullySupported},
			event.StateBeeperDisappearingTimer.Type: {Level: event.CapLevelFullySupported},
			event.StateMSC4391BotCommand.Type:       {Level: event.CapLevelFullySupported},
			codexThreadStateType:                    {Level: event.CapLevelFullySupported},
			beeperAIModelStateType:                  {Level: event.CapLevelFullySupported},
		},
		DisappearingTimer: &event.DisappearingTimerCapability{
			Types: []event.DisappearingType{
				event.DisappearingTypeAfterSend,
				event.DisappearingTypeAfterRead,
			},
		},
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
		}
		thread, err := cl.startThreadForPortal(ctx, cwd, msg.Portal)
		if err != nil {
			log.Err(err).Str("cwd", cwd).Msg("Failed to start Codex thread for Matrix message")
			return nil, err
		}
		meta.ThreadID = thread.ID
		meta.Cwd = thread.Cwd
		directorySelection = consumedBody
		if synced := cl.syncThreadPortal(ctx, msg.Portal, thread); synced != nil {
			msg.Portal = synced
			meta = portalMetadata(msg.Portal.Metadata)
		}
		startedThread = true
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
		cl.queueCommandNotice(msg.Portal, meta.ThreadID, "Started Codex session in "+meta.Cwd+".")
		log.Debug().Str("thread_id", meta.ThreadID).Str("cwd", meta.Cwd).Msg("Accepted Codex directory selection")
		return &bridgev2.MatrixMessageResponse{DB: userDB, StreamOrder: userDB.Timestamp.UnixNano()}, nil
	}
	clientUserMessageID := string(msg.Event.ID)
	if active := cl.Main.activeRun(meta.ThreadID); active != nil {
		if err := cl.steerTurn(ctx, meta.ThreadID, active.turnID, clientUserMessageID, prompt); err != nil {
			log.Err(err).Str("thread_id", meta.ThreadID).Str("turn_id", active.turnID).Msg("Failed to steer Codex turn from Matrix message")
			return nil, err
		}
	} else {
		if !startedThread {
			thread, err := cl.resumeThreadForPortal(ctx, msg.Portal, meta)
			if isThreadNotFoundError(err) {
				oldThreadID := meta.ThreadID
				thread, err = cl.replaceMissingThread(ctx, msg.Portal, meta)
				if err != nil {
					log.Err(err).Str("thread_id", oldThreadID).Msg("Failed to replace missing Codex thread")
					return nil, err
				}
				log.Warn().Str("old_thread_id", oldThreadID).Str("thread_id", thread.ID).Msg("Replaced missing Codex thread before Matrix turn")
			} else if err != nil {
				log.Err(err).Str("thread_id", meta.ThreadID).Msg("Failed to resume Codex thread for Matrix message")
				return nil, err
			}
			meta.ThreadID = thread.ID
			meta.Cwd = thread.Cwd
			userDB.Metadata = &MessageMetadata{Role: "user", ThreadID: meta.ThreadID, StreamStatus: "done"}
			if synced := cl.syncThreadPortal(ctx, msg.Portal, thread); synced != nil {
				msg.Portal = synced
				userDB.Room = msg.Portal.PortalKey
			}
		}
		if _, err := cl.startTurn(ctx, msg.Portal.PortalKey, meta.ThreadID, clientUserMessageID, prompt); err != nil {
			if isThreadNotFoundError(err) {
				oldThreadID := meta.ThreadID
				thread, err := cl.resumeThreadForPortal(ctx, msg.Portal, meta)
				if isThreadNotFoundError(err) {
					thread, err = cl.replaceMissingThread(ctx, msg.Portal, meta)
				}
				if err != nil {
					log.Err(err).Str("thread_id", oldThreadID).Msg("Failed to recover missing Codex thread")
					return nil, err
				}
				meta.ThreadID = thread.ID
				meta.Cwd = thread.Cwd
				userDB.Metadata = &MessageMetadata{Role: "user", ThreadID: meta.ThreadID, StreamStatus: "done"}
				if synced := cl.syncThreadPortal(ctx, msg.Portal, thread); synced != nil {
					msg.Portal = synced
					userDB.Room = msg.Portal.PortalKey
				}
				if _, retryErr := cl.startTurn(ctx, msg.Portal.PortalKey, meta.ThreadID, clientUserMessageID, prompt); retryErr != nil {
					log.Err(retryErr).Str("old_thread_id", oldThreadID).Str("thread_id", meta.ThreadID).Msg("Failed to start Codex turn after replacing missing thread")
					return nil, retryErr
				}
				log.Warn().Str("old_thread_id", oldThreadID).Str("thread_id", meta.ThreadID).Msg("Recovered missing Codex thread and retried Matrix message")
				log.Debug().Str("thread_id", meta.ThreadID).Msg("Accepted Codex Matrix message")
				return &bridgev2.MatrixMessageResponse{DB: userDB, StreamOrder: userDB.Timestamp.UnixNano()}, nil
			}
			log.Err(err).Str("thread_id", meta.ThreadID).Msg("Failed to start Codex turn from Matrix message")
			return nil, err
		}
	}
	log.Debug().Str("thread_id", meta.ThreadID).Msg("Accepted Codex Matrix message")
	return &bridgev2.MatrixMessageResponse{DB: userDB, StreamOrder: userDB.Timestamp.UnixNano()}, nil
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
	msg.Portal.NameSet = name != ""
	return true, nil
}

func (cl *Client) HandleMatrixRoomTopic(ctx context.Context, msg *bridgev2.MatrixRoomTopic) (bool, error) {
	if msg == nil || msg.Portal == nil || msg.Content == nil {
		return false, nil
	}
	topic := strings.TrimSpace(msg.Content.Topic)
	msg.Portal.Topic = topic
	msg.Portal.TopicSet = topic != ""
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
		cl.handleCodexThreadRoomState(msg)
		return true, nil
	}
	if cl != nil && cl.Main != nil {
		cl.Main.setModelStateForPortalKey(msg.Portal.PortalKey, msg.Content)
	}
	return true, nil
}

func (cl *Client) handleCodexThreadRoomState(msg *bridgev2.MatrixRoomState) {
	if cl == nil || msg == nil || msg.Portal == nil {
		return
	}
	state := copyStateMap(msg.Content)
	normalizeThreadState("room/state", state)
	threadID := firstStateString(state, "threadId", "sessionId")
	cwd := firstStateString(state, "cwd", "settings.cwd")
	meta := portalMetadata(msg.Portal.Metadata)
	oldThreadID := meta.ThreadID
	if threadID == "" {
		threadID = meta.ThreadID
	}
	if cwd == "" {
		cwd = meta.Cwd
	}
	if cwd == "" {
		cwd, _ = parseProjectPortalID(msg.Portal.ID)
	}
	if threadID != "" {
		meta.ThreadID = threadID
	}
	if cwd != "" {
		meta.Cwd = cwd
	}
	if meta.ThreadID != "" || meta.Cwd != "" {
		msg.Portal.Metadata = meta
	}
	if cl.Main == nil || threadID == "" {
		return
	}
	if oldThreadID != "" && oldThreadID != threadID {
		cl.Main.forgetThread(oldThreadID)
	}
	provider := firstStateString(state, "modelProvider", "provider")
	model := codexModelStateRef(state, provider)
	effort := firstStateString(state, "effort", "reasoning", "reasoningEffort", "reasoning_effort")
	cl.Main.rememberThreadRoom(threadID, cl, msg.Portal.PortalKey, cwd, provider, model, effort)
	cl.Main.setModelStateForPortalKey(msg.Portal.PortalKey, state)
}

func (cl *Client) HandleMatrixDisappearingTimer(ctx context.Context, msg *bridgev2.MatrixDisappearingTimer) (bool, error) {
	if msg == nil || msg.Portal == nil {
		return false, nil
	}
	msg.Portal.Disappear = database.DisappearingSettingFromEvent(msg.Content)
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
		contacts := cl.contactsForThreads(ctx, nil)
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
	info := portalInfo(name, cl.codexMembers(), "", "", nil)
	info.Topic = stringPtr(newProjectPrompt)
	return &bridgev2.CreateChatResponse{
		PortalKey:      newProjectPortalKey(loginID),
		PortalInfo:     info,
		DMRedirectedTo: codexUserID,
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
		cl.Main.rememberThreadRoom(threadID, cl, key, cwd, firstStateString(state, "modelProvider"), codexModelStateRef(state, ""), firstStateString(state, "effort", "reasoning", "reasoningEffort", "reasoning_effort"))
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
				event.StateBeeperDisappearingTimer:         0,
				roomStateEventType(codexThreadStateType):   0,
				roomStateEventType(beeperAIModelStateType): 0,
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
	if model := cl.validatedRequestModel(ctx, cl.roomAIModelState(ctx, portal), portal, networkid.PortalKey{}); model != "" {
		params["model"] = model
	}
	var resp appserver.ThreadOpenResponse
	if err := cl.Main.request(ctx, "thread/start", params, &resp); err != nil {
		return appserver.Thread{}, err
	}
	return resp.HydratedThread(), nil
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
	if model := cl.validatedRequestModel(ctx, cl.roomAIModelState(ctx, portal), portal, networkid.PortalKey{}); model != "" {
		params["model"] = model
	}
	var resp appserver.ThreadOpenResponse
	if err := cl.Main.request(ctx, "thread/resume", params, &resp); err != nil {
		return appserver.Thread{}, err
	}
	return resp.HydratedThread(), nil
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
	return errors.As(err, &rpcErr) && strings.Contains(strings.ToLower(rpcErr.Msg), "thread not found")
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
	cl.Main.rememberThreadRoom(thread.ID, cl, portal.PortalKey, thread.Cwd, thread.ModelProvider, threadModelRef(thread), threadReasoningEffort(thread))
	name := threadName(thread)
	portal.UpdateInfo(ctx, portalInfo(name, cl.codexMembers(), thread.Cwd, thread.ID, codexThreadInitialState(thread)), cl.UserLogin, nil, time.Now())
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
	if run := cl.Main.activeRun(threadID); run != nil && run.turnID == resp.Turn.ID {
		if err := run.start(ctx); err != nil {
			return nil, err
		}
		return run, nil
	}
	run := newActiveRun(cl, portalKey, threadID, resp.Turn.ID)
	cl.Main.setActive(threadID, run)
	if err := run.start(ctx); err != nil {
		cl.Main.setActive(threadID, nil)
		return nil, err
	}
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
	return nil
}

func (cl *Client) interruptTurn(ctx context.Context, portal *bridgev2.Portal, threadID, turnID string) {
	if err := cl.Main.request(ctx, "turn/interrupt", map[string]any{"threadId": threadID, "turnId": turnID}, nil); err != nil {
		cl.queueCommandNotice(portal, threadID, "Failed to stop Codex turn:\n\n"+err.Error())
		return
	}
	cl.queueCommandNotice(portal, threadID, "Requested Codex to stop the active turn.")
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
	roomModel := firstStateString(state, "model")
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
