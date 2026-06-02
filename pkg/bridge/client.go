package bridge

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"maunium.net/go/mautrix/bridgev2/status"
	"maunium.net/go/mautrix/event"

	"github.com/beeper/codex-bridge/pkg/appserver"
)

type Client struct {
	Main      *Connector
	UserLogin *bridgev2.UserLogin
	loggedIn  bool
}

var _ bridgev2.NetworkAPI = (*Client)(nil)
var _ bridgev2.NetworkAPIWithUserID = (*Client)(nil)
var _ bridgev2.IdentifierResolvingNetworkAPI = (*Client)(nil)
var _ bridgev2.GhostDMCreatingNetworkAPI = (*Client)(nil)
var _ bridgev2.ContactListingNetworkAPI = (*Client)(nil)
var _ bridgev2.UserSearchingNetworkAPI = (*Client)(nil)
var _ bridgev2.BackfillingNetworkAPI = (*Client)(nil)
var _ bridgev2.RoomNameHandlingNetworkAPI = (*Client)(nil)
var _ status.BridgeStateFiller = (*Client)(nil)

func (cl *Client) Connect(ctx context.Context) {
	cl.loggedIn = true
	if cl.UserLogin != nil && cl.UserLogin.BridgeState != nil {
		cl.UserLogin.BridgeState.Send(status.BridgeState{StateEvent: status.StateConnected})
	}
}

func (cl *Client) Disconnect() {
	cl.loggedIn = false
}

func (cl *Client) IsLoggedIn() bool {
	return cl.loggedIn
}

func (cl *Client) LogoutRemote(ctx context.Context) {
	cl.loggedIn = false
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
		if meta.ThreadID != "" {
			cl.Main.rememberThreadRoom(meta.ThreadID, cl, portal.PortalKey, meta.Cwd)
		}
	}
	name := "New Project"
	var state map[string]any
	if meta.ThreadID != "" {
		if thread, err := cl.readThread(ctx, meta.ThreadID, false); err == nil {
			name = threadName(thread)
			state = codexThreadInitialState(thread)
		} else if meta.Cwd != "" {
			name = directoryName(meta.Cwd)
		}
	} else if meta.Cwd != "" {
		name = directoryName(meta.Cwd)
	}
	return portalInfo(name, cl.codexMembers(), meta.Cwd, meta.ThreadID, state), nil
}

func (cl *Client) GetUserInfo(ctx context.Context, ghost *bridgev2.Ghost) (*bridgev2.UserInfo, error) {
	switch ghost.ID {
	case codexUserID:
		return userInfo("Codex", true), nil
	case newProjectUserID:
		return userInfo("New Project", true), nil
	default:
		if cwd, ok := parseProjectUserID(ghost.ID); ok {
			return userInfo(directoryName(cwd), true, cwd), nil
		}
		return userInfo("Codex", true), nil
	}
}

func (cl *Client) GetCapabilities(ctx context.Context, portal *bridgev2.Portal) *event.RoomFeatures {
	return &event.RoomFeatures{
		State: event.StateFeatureMap{
			event.StateRoomName.Type:          {Level: event.CapLevelFullySupported},
			event.StateMSC4391BotCommand.Type: {Level: event.CapLevelFullySupported},
			codexThreadStateType:              {Level: event.CapLevelFullySupported},
			beeperAIModelStateType:            {Level: event.CapLevelFullySupported},
		},
	}
}

func (cl *Client) HandleMatrixMessage(ctx context.Context, msg *bridgev2.MatrixMessage) (*bridgev2.MatrixMessageResponse, error) {
	if msg == nil || msg.Portal == nil || msg.Content == nil {
		return nil, fmt.Errorf("missing Matrix message")
	}
	if response, handled, err := cl.handleBridgeCommand(ctx, msg); handled {
		return response, err
	}
	meta := portalMetadata(msg.Portal.Metadata)
	if meta.ThreadID == "" {
		cwd := meta.Cwd
		if cwd == "" {
			if parsed, ok := parseProjectPortalID(msg.Portal.ID); ok {
				cwd = parsed
			}
		}
		if cwd == "" && msg.Content.Body != "" {
			cwd = strings.TrimSpace(msg.Content.Body)
		}
		thread, err := cl.startThread(ctx, cwd)
		if err != nil {
			return nil, err
		}
		meta.ThreadID = thread.ID
		meta.Cwd = thread.Cwd
		msg.Portal.Metadata = meta
		_ = msg.Portal.Save(ctx)
		cl.Main.rememberThreadRoom(thread.ID, cl, msg.Portal.PortalKey, thread.Cwd)
		cl.syncThreadPortal(ctx, msg.Portal, thread)
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
	clientUserMessageID := string(msg.Event.ID)
	if active := cl.Main.activeRun(meta.ThreadID); active != nil {
		if err := cl.steerTurn(ctx, meta.ThreadID, active.turnID, clientUserMessageID, prompt); err != nil {
			return nil, err
		}
	} else {
		if _, err := cl.startTurn(ctx, msg.Portal.PortalKey, meta.ThreadID, clientUserMessageID, prompt); err != nil {
			return nil, err
		}
	}
	return &bridgev2.MatrixMessageResponse{DB: userDB}, nil
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
	if identifier == "" || strings.EqualFold(identifier, "new") || strings.EqualFold(identifier, "new project") {
		resp := &bridgev2.ResolveIdentifierResponse{
			UserID:   newProjectUserID,
			UserInfo: userInfo("New Project", true),
		}
		if createChat {
			resp.Chat = cl.newProjectChat("New Project")
		}
		return resp, nil
	}
	if strings.EqualFold(identifier, "codex") {
		resp := &bridgev2.ResolveIdentifierResponse{
			UserID:   codexUserID,
			UserInfo: userInfo("Codex", true),
		}
		if createChat {
			resp.Chat = cl.newProjectChat("New Project")
		}
		return resp, nil
	}
	cwd, err := cleanProjectDir(identifier)
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
	if createChat {
		resp.Chat = cl.chatForProject(ctx, cwd, name, threadID, state)
	}
	return resp, nil
}

func (cl *Client) CreateChatWithGhost(ctx context.Context, ghost *bridgev2.Ghost) (*bridgev2.CreateChatResponse, error) {
	if ghost == nil {
		return nil, fmt.Errorf("missing ghost")
	}
	switch ghost.ID {
	case newProjectUserID, codexUserID:
		return cl.newProjectChat("New Project"), nil
	default:
		cwd, ok := parseProjectUserID(ghost.ID)
		if !ok {
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
		return cl.chatForProject(ctx, cwd, name, threadID, state), nil
	}
}

func (cl *Client) GetContactList(ctx context.Context) ([]*bridgev2.ResolveIdentifierResponse, error) {
	if cl.Main == nil {
		return cl.contactsForThreads(ctx, nil), nil
	}
	threads, err := cl.listThreads(ctx, "", 100)
	if err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).Msg("Failed to list Codex threads for contacts")
		return cl.contactsForThreads(ctx, nil), nil
	}
	return cl.contactsForThreads(ctx, threads), nil
}

func (cl *Client) contactsForThreads(ctx context.Context, threads []appserver.Thread) []*bridgev2.ResolveIdentifierResponse {
	contacts := []*bridgev2.ResolveIdentifierResponse{
		{
			UserID:   codexUserID,
			UserInfo: userInfo("Codex", true),
		},
		{
			UserID:   newProjectUserID,
			UserInfo: userInfo("New Project", true),
		},
	}
	for _, thread := range sortedRecentDirectories(threads) {
		contacts = append(contacts, &bridgev2.ResolveIdentifierResponse{
			UserID:   projectUserID(thread.Cwd),
			UserInfo: projectUserInfo(thread, thread.Cwd, directoryName(thread.Cwd)),
			Chat:     cl.chatForProject(ctx, thread.Cwd, directoryName(thread.Cwd), thread.ID, codexThreadInitialState(thread)),
		})
	}
	return contacts
}

func projectUserInfo(thread appserver.Thread, cwd, name string) *bridgev2.UserInfo {
	return userInfo(name, true, cwd, thread.ID, thread.Name, thread.Preview)
}

func (cl *Client) SearchUsers(ctx context.Context, query string) ([]*bridgev2.ResolveIdentifierResponse, error) {
	query = strings.ToLower(strings.TrimSpace(query))
	contacts, err := cl.GetContactList(ctx)
	if err != nil || query == "" {
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
		PortalInfo:     portalInfo(name, cl.codexMembers(), "", "", nil),
		DMRedirectedTo: codexUserID,
	}
}

func (cl *Client) chatForProject(ctx context.Context, cwd, name, threadID string, state map[string]any) *bridgev2.CreateChatResponse {
	loginID := defaultLoginID
	if cl.UserLogin != nil {
		loginID = cl.UserLogin.ID
	}
	key := projectPortalKey(cwd, loginID)
	if threadID != "" && cl.Main != nil {
		if room, ok := cl.Main.threadRoom(threadID); ok && room.portalKey.Receiver == loginID {
			key = room.portalKey
		}
	}
	var portal *bridgev2.Portal
	if cl.Main != nil && cl.Main.Bridge != nil {
		portal, _ = cl.Main.Bridge.GetPortalByKey(ctx, key)
	}
	if portal != nil {
		meta := portalMetadata(portal.Metadata)
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
	}
	return &bridgev2.CreateChatResponse{
		PortalKey:      key,
		Portal:         portal,
		PortalInfo:     portalInfo(name, cl.codexMembers(), cwd, threadID, state),
		DMRedirectedTo: codexUserID,
	}
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
		IsFull:      true,
		OtherUserID: codexUserID,
		MemberMap: bridgev2.ChatMemberMap{
			cl.GetUserID(): {
				EventSender: bridgev2.EventSender{IsFromMe: true, Sender: cl.GetUserID(), SenderLogin: cl.UserLogin.ID},
				Membership:  event.MembershipJoin,
			},
			codexUserID: {
				EventSender: bridgev2.EventSender{Sender: codexUserID},
				Membership:  event.MembershipJoin,
				UserInfo:    userInfo("Codex", true),
			},
		},
	}
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

func (cl *Client) startThread(ctx context.Context, cwd string) (appserver.Thread, error) {
	var err error
	if cwd, err = cleanProjectDir(cwd); err != nil {
		return appserver.Thread{}, err
	}
	params := map[string]any{
		"cwd":            cwd,
		"approvalPolicy": "on-request",
		"threadSource":   "user",
	}
	if cl.Main.Config.DefaultModel != "" {
		params["model"] = cl.Main.Config.DefaultModel
	}
	var resp appserver.ThreadStartResponse
	if err := cl.Main.request(ctx, "thread/start", params, &resp); err != nil {
		return appserver.Thread{}, err
	}
	return resp.Thread, nil
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

func (cl *Client) syncThreadPortal(ctx context.Context, portal *bridgev2.Portal, thread appserver.Thread) {
	if portal == nil || thread.ID == "" {
		return
	}
	cl.setPortalThreadMetadata(ctx, portal, thread.ID, thread.Cwd)
	cl.Main.rememberThreadRoom(thread.ID, cl, portal.PortalKey, thread.Cwd)
	name := threadName(thread)
	portal.UpdateInfo(ctx, portalInfo(name, cl.codexMembers(), thread.Cwd, thread.ID, codexThreadInitialState(thread)), cl.UserLogin, nil, time.Now())
	if cl.Main != nil && cl.Main.Bridge != nil {
		cl.Main.Bridge.WakeupBackfillQueue()
	}
}

func (cl *Client) setPortalThreadMetadata(ctx context.Context, portal *bridgev2.Portal, threadID, cwd string) {
	if portal == nil || threadID == "" {
		return
	}
	meta := portalMetadata(portal.Metadata)
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
		_ = portal.Save(ctx)
	}
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
	if meta.ThreadID == "" {
		return &bridgev2.FetchMessagesResponse{HasMore: false}, nil
	}
	thread, err := cl.readThread(ctx, meta.ThreadID, true)
	if err != nil {
		return nil, err
	}
	messages, err := cl.backfillMessages(ctx, params.Portal, thread)
	if err != nil {
		return nil, err
	}
	return paginateBackfillMessages(messages, params), nil
}

func (cl *Client) backfillMessages(ctx context.Context, portal *bridgev2.Portal, thread appserver.Thread) ([]*bridgev2.BackfillMessage, error) {
	return cl.projectBackfillMessages(ctx, portal, thread)
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
