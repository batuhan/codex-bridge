package bridge

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/util/dbutil"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	matrixconnector "maunium.net/go/mautrix/bridgev2/matrix"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"maunium.net/go/mautrix/bridgev2/status"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"github.com/beeper/ai-bridge/pkg/aidb"
	"github.com/beeper/codex-bridge/pkg/appserver"
)

const appServerStartupTimeout = 15 * time.Second
const contactGhostSyncLimit = 20
const messageMetadataSyncLimit = 500
const bridgeInfoVersion = 2
const roomCapabilitiesVersion = 5
const roomFeaturesID = "com.beeper.codex.capabilities.2026_06_02.no_typing+state"
const defaultCodexAvatarMXC = "mxc://beeper.com/51a668657dd9e0132cc823ad9402c6c2d0fc3321"

type Connector struct {
	Bridge *bridgev2.Bridge
	Config Config
	Store  *aidb.Store

	appMu sync.Mutex
	app   *appserver.Client

	activeMu sync.Mutex
	active   map[string]*activeRun

	pendingStartMu sync.Mutex
	pendingStarts  map[string]pendingTurnStart

	threadMu    sync.Mutex
	threadRooms map[string]threadRoom

	processMu sync.Mutex
	processes map[string]*activeRun

	globalMu    sync.Mutex
	globalState map[string]any
}

type threadRoom struct {
	portalKey       networkid.PortalKey
	login           *bridgev2.UserLogin
	cwd             string
	modelProvider   string
	model           string
	reasoningEffort string
}

type pendingTurnStart struct {
	client    *Client
	portalKey networkid.PortalKey
}

var _ bridgev2.NetworkConnector = (*Connector)(nil)
var _ bridgev2.ConfigValidatingNetwork = (*Connector)(nil)
var _ bridgev2.PortalBridgeInfoFillingNetwork = (*Connector)(nil)

func (c *Connector) GetName() bridgev2.BridgeName {
	return bridgev2.BridgeName{
		DisplayName:          "Codex",
		NetworkURL:           "https://github.com/openai/codex",
		NetworkIcon:          id.ContentURIString(defaultCodexAvatarMXC),
		NetworkID:            networkID,
		BeeperBridgeType:     beeperBridgeType,
		DefaultPort:          29345,
		DefaultCommandPrefix: "!codex",
	}
}

func (c *Connector) GetDBMetaTypes() database.MetaTypes {
	return database.MetaTypes{
		Portal:  func() any { return &PortalMetadata{} },
		Message: func() any { return &MessageMetadata{} },
	}
}

func (c *Connector) GetCapabilities() *bridgev2.NetworkGeneralCapabilities {
	return &bridgev2.NetworkGeneralCapabilities{
		DisappearingMessages: true,
		AggressiveUpdateInfo: true,
		Provisioning: bridgev2.ProvisioningCapabilities{
			ResolveIdentifier: bridgev2.ResolveIdentifierCapabilities{
				CreateDM:       true,
				LookupUsername: true,
				ContactList:    true,
				Search:         true,
			},
		},
	}
}

func (c *Connector) Init(bridge *bridgev2.Bridge) {
	c.Config.ApplyDefaults()
	c.Bridge = bridge
	if bridge != nil && bridge.DB != nil {
		c.Store = aidb.NewStore(bridge.DB.Database, bridge.ID, dbutil.ZeroLogger(bridge.Log.With().Str("db_section", "ai").Logger()))
	}
	c.active = map[string]*activeRun{}
	c.pendingStarts = map[string]pendingTurnStart{}
	c.threadRooms = map[string]threadRoom{}
	c.processes = map[string]*activeRun{}
	c.globalState = map[string]any{}
}

func (c *Connector) ValidateConfig() error {
	c.Config.ApplyDefaults()
	_, err := appserver.CheckBinary(context.Background(), c.Config.CodexCommand, c.Config.MinVersion)
	return err
}

func (c *Connector) Start(ctx context.Context) error {
	c.Config.ApplyDefaults()
	if c.Bridge.GetBeeperStreamPublisher() == nil {
		return fmt.Errorf("Codex bridge requires a Matrix connector with Beeper stream support")
	}
	if _, ok := c.Bridge.Matrix.(bridgev2.MatrixConnectorWithArbitraryRoomState); !ok {
		return fmt.Errorf("Codex bridge requires a Matrix connector with arbitrary room state support")
	}
	if c.Bridge.Config == nil || !c.Bridge.Config.EnableSendStateRequests {
		return fmt.Errorf("Codex bridge requires bridge.enable_send_state_requests for Beeper room state support")
	}
	if c.Store != nil {
		if err := c.Store.Upgrade(ctx); err != nil {
			return bridgev2.DBUpgradeError{Err: err, Section: "ai"}
		}
	}
	path, err := appserver.CheckBinary(ctx, c.Config.CodexCommand, c.Config.MinVersion)
	if err != nil {
		return err
	}
	c.Bridge.Log.Info().Str("path", path).Msg("Starting Codex app-server")
	app, err := appserver.Start(ctx, c.Config.CodexCommand, nil)
	if err != nil {
		return err
	}
	c.Bridge.Log.Debug().Msg("Codex app-server process started")
	startupCtx, cancel := context.WithTimeout(ctx, appServerStartupTimeout)
	defer cancel()
	if err = appserver.Initialize(startupCtx, app); err != nil {
		app.Close()
		return err
	}
	c.appMu.Lock()
	c.app = app
	c.appMu.Unlock()
	c.seedConfiguredLogins(startupCtx)
	c.hydrateThreadRooms(startupCtx)
	c.normalizeStoredMessageMetadata(startupCtx)
	c.syncBaseContactGhosts(startupCtx)
	go c.syncRecentContactGhosts(ctx)
	go c.dispatchAppServer()
	return nil
}

func (c *Connector) Stop() {
	c.appMu.Lock()
	app := c.app
	c.app = nil
	c.appMu.Unlock()
	if app != nil {
		app.Close()
	}
}

func (c *Connector) normalizeStoredMessageMetadata(ctx context.Context) {
	if c == nil || c.Bridge == nil || c.Bridge.DB == nil {
		return
	}
	portals, err := c.Bridge.GetAllPortals(ctx)
	if err != nil {
		logFromContext(ctx).Warn().Err(err).Msg("Failed to load portals for Codex message metadata normalization")
		return
	}
	updated := 0
	for _, portal := range portals {
		if portal == nil {
			continue
		}
		messages, err := c.Bridge.DB.Message.GetLastNInPortal(ctx, portal.PortalKey, messageMetadataSyncLimit)
		if err != nil {
			logFromContext(ctx).Warn().Err(err).Stringer("portal_key", portal.PortalKey).Msg("Failed to load messages for Codex metadata normalization")
			continue
		}
		for _, msg := range messages {
			if !normalizeStoredMessageMetadata(msg) {
				continue
			}
			if err := c.Bridge.DB.Message.Update(ctx, msg); err != nil {
				logFromContext(ctx).Warn().Err(err).Str("message_id", string(msg.ID)).Msg("Failed to normalize Codex message metadata")
				continue
			}
			updated++
		}
	}
	if updated > 0 {
		logFromContext(ctx).Info().Int("updated", updated).Msg("Normalized Codex message metadata")
	}
}

func (c *Connector) GetBridgeInfoVersion() (info, capabilities int) {
	return bridgeInfoVersion, roomCapabilitiesVersion
}

func (c *Connector) FillPortalBridgeInfo(portal *bridgev2.Portal, content *event.BridgeEventContent) {
	if portal == nil || content == nil {
		return
	}
	content.BeeperBridgeName = string(portal.Receiver)
	content.BeeperSelfHosted = true
}

func (c *Connector) LoadUserLogin(ctx context.Context, login *bridgev2.UserLogin) error {
	login.Client = &Client{Main: c, UserLogin: login, loggedIn: true}
	return nil
}

func (c *Connector) appClient() (*appserver.Client, error) {
	c.appMu.Lock()
	defer c.appMu.Unlock()
	if c.app == nil {
		return nil, fmt.Errorf("Codex app-server is not running")
	}
	return c.app, nil
}

func (c *Connector) request(ctx context.Context, method string, params any, out any) error {
	app, err := c.appClient()
	if err != nil {
		return err
	}
	logCodexRequest(ctx, method, params).Msg("Sending Codex app-server request")
	err = app.Request(ctx, method, params, out)
	if err != nil {
		logCodexRequest(ctx, method, params).Err(err).Msg("Codex app-server request failed")
	}
	return err
}

func logCodexRequest(ctx context.Context, method string, params any) *zerolog.Event {
	logger := logFromContext(ctx).With().Str("method", method).Logger()
	ev := logger.Debug()
	fields, _ := params.(map[string]any)
	for _, key := range []string{"threadId", "turnId", "expectedTurnId", "cwd", "model", "approvalPolicy", "clientUserMessageId"} {
		if value, _ := fields[key].(string); value != "" {
			ev = ev.Str(fieldNameForLog(key), value)
		}
	}
	return ev
}

func fieldNameForLog(key string) string {
	var out strings.Builder
	for i, ch := range key {
		if i > 0 && ch >= 'A' && ch <= 'Z' {
			out.WriteByte('_')
		}
		out.WriteRune(ch)
	}
	return strings.ToLower(out.String())
}

func (c *Connector) dispatchAppServer() {
	app, err := c.appClient()
	if err != nil {
		return
	}
	for msg := range app.Incoming() {
		if msg.Method == "" {
			continue
		}
		if msg.ID != nil {
			go c.handleServerRequest(msg)
			continue
		}
		c.handleNotification(msg.Method, msg.Params)
	}
}

func (c *Connector) handleServerRequest(msg appserver.Message) {
	app, err := c.appClient()
	if err != nil {
		return
	}
	ctx := context.Background()
	if code, message, ok := directServerRequestError(msg.Method); ok {
		_ = app.RespondError(ctx, msg.ID, code, message)
		return
	}
	active := c.activeRunForRequest(msg.Params)
	if active == nil {
		_ = app.RespondError(ctx, msg.ID, -32000, "No active Codex turn for server request", map[string]any{
			"method": msg.Method,
		})
		return
	}
	result, err := active.handleServerRequest(ctx, msg)
	if err != nil {
		_ = app.RespondError(ctx, msg.ID, -32001, err.Error(), map[string]any{
			"method": msg.Method,
		})
		return
	}
	_ = app.Respond(ctx, msg.ID, result)
}

func (c *Connector) handleNotification(method string, params json.RawMessage) {
	threadID := notificationThreadID(params)
	if threadID == "" {
		if active := c.activeRunForProcess(params); active != nil {
			active.handle(method, params)
			return
		}
		c.handleGlobalNotification(method, params)
		return
	}
	c.handleThreadMetadataNotification(method, threadID, params)
	c.handleThreadNoticeNotification(method, threadID, params)
	c.activeMu.Lock()
	active := c.active[threadID]
	c.activeMu.Unlock()
	if active == nil && canStartActiveRunFromNotification(method) {
		active = c.startActiveRunFromNotification(threadID, params)
	}
	if active == nil {
		return
	}
	active.handle(method, params)
}

func (c *Connector) handleGlobalNotification(method string, params json.RawMessage) {
	payload := rawPayload(params)
	switch method {
	case "account/updated":
		c.updateGlobalState(method, payload)
		go c.refreshAccountState(context.Background())
	case "account/login/completed":
		c.updateGlobalState(method, payload)
		if success, ok := payload["success"].(bool); ok && !success {
			message, _ := payload["error"].(string)
			if strings.TrimSpace(message) == "" {
				message = "Codex login failed"
			}
			c.broadcastBridgeState(status.StateLoggedOut, message)
			return
		}
		go c.refreshAccountState(context.Background())
	case "account/rateLimits/updated",
		"mcpServer/oauthLogin/completed",
		"mcpServer/startupStatus/updated",
		"app/list/updated",
		"remoteControl/status/changed",
		"externalAgentConfig/import/completed",
		"skills/changed",
		"windows/worldWritableWarning",
		"windowsSandbox/setupCompleted",
		"fuzzyFileSearch/sessionUpdated",
		"fuzzyFileSearch/sessionCompleted",
		"fs/changed":
		c.updateGlobalState(method, payload)
		c.broadcastBridgeState(status.StateConnected, "")
	case "warning", "configWarning", "deprecationNotice", "guardianWarning":
		text := threadNoticeText(method, params)
		c.updateGlobalState(method, payload)
		c.broadcastBridgeState(status.StateConnected, text)
	case "error":
		text := errorNoticeText(payload)
		c.updateGlobalState(method, payload)
		c.broadcastBridgeState(status.StateUnknownError, text)
	default:
		logFromContext(context.Background()).Debug().Str("method", method).RawJSON("params", params).Msg("Ignoring global Codex notification")
	}
}

func (c *Connector) updateGlobalState(method string, payload map[string]any) {
	if c == nil {
		return
	}
	c.globalMu.Lock()
	if c.globalState == nil {
		c.globalState = map[string]any{}
	}
	c.globalState["lastNotification"] = method
	c.globalState["updatedAt"] = time.Now().Format(time.RFC3339Nano)
	c.globalState[method] = payload
	c.globalMu.Unlock()
}

func (c *Connector) globalBridgeInfo() map[string]any {
	if c == nil {
		return nil
	}
	c.globalMu.Lock()
	defer c.globalMu.Unlock()
	if len(c.globalState) == 0 {
		return nil
	}
	out := make(map[string]any, len(c.globalState))
	for key, value := range c.globalState {
		out[key] = value
	}
	return out
}

func (c *Connector) broadcastBridgeState(stateEvent status.BridgeStateEvent, message string) {
	if c == nil || c.Bridge == nil {
		return
	}
	for _, login := range c.Bridge.GetAllCachedUserLogins() {
		if login == nil || login.BridgeState == nil {
			continue
		}
		state := status.BridgeState{StateEvent: stateEvent}
		if strings.TrimSpace(message) != "" {
			state.Message = strings.TrimSpace(message)
		}
		login.BridgeState.Send(state)
	}
}

func (c *Connector) refreshAccountState(ctx context.Context) {
	account, err := c.readAccount(ctx)
	if err != nil {
		logFromContext(ctx).Err(err).Msg("Failed to refresh Codex account after notification")
		c.broadcastBridgeState(status.StateUnknownError, err.Error())
		return
	}
	if account == nil {
		c.broadcastBridgeState(status.StateLoggedOut, "Codex is not logged in locally")
		return
	}
	displayName := accountName(account)
	c.updateGlobalState("account/read", map[string]any{"type": account.Type, "email": account.Email})
	if c.Bridge != nil {
		for _, login := range c.Bridge.GetAllCachedUserLogins() {
			if login == nil {
				continue
			}
			login.RemoteName = displayName
			login.RemoteProfile.Name = displayName
			if err := login.Save(ctx); err != nil {
				logFromContext(ctx).Err(err).Str("login_id", string(login.ID)).Msg("Failed to save refreshed Codex login")
			}
		}
	}
	c.broadcastBridgeState(status.StateConnected, "")
}

func notificationThreadID(params json.RawMessage) string {
	var payload map[string]any
	if err := json.Unmarshal(params, &payload); err != nil {
		return ""
	}
	return threadIDFromMap(payload, 0)
}

func (c *Connector) activeRunForRequest(params json.RawMessage) *activeRun {
	threadID := notificationThreadID(params)
	if threadID == "" {
		return nil
	}
	c.activeMu.Lock()
	active := c.active[threadID]
	c.activeMu.Unlock()
	if active == nil {
		active = c.startActiveRunFromNotification(threadID, params)
	}
	return active
}

func notificationTurnID(params json.RawMessage) string {
	var payload map[string]any
	if err := json.Unmarshal(params, &payload); err != nil {
		return ""
	}
	return turnIDFromMap(payload, 0)
}

func notificationProcessID(params json.RawMessage) string {
	var payload map[string]any
	if err := json.Unmarshal(params, &payload); err != nil {
		return ""
	}
	for _, key := range []string{"processId", "processHandle"} {
		if value, _ := payload[key].(string); value != "" {
			return value
		}
	}
	return ""
}

func canStartActiveRunFromNotification(method string) bool {
	return isActiveRunNotification(method)
}

func isActiveRunNotification(method string) bool {
	if strings.HasPrefix(method, "item/") ||
		strings.HasPrefix(method, "hook/") ||
		strings.HasPrefix(method, "thread/realtime/") {
		return true
	}
	switch method {
	case "thread/started",
		"thread/status/changed",
		"thread/archived",
		"thread/unarchived",
		"thread/closed",
		"thread/name/updated",
		"thread/goal/updated",
		"thread/goal/cleared",
		"thread/settings/updated",
		"thread/tokenUsage/updated",
		"thread/compacted",
		"turn/started",
		"turn/completed",
		"turn/diff/updated",
		"turn/plan/updated",
		"command/exec/outputDelta",
		"process/outputDelta",
		"process/exited",
		"rawResponseItem/completed",
		"serverRequest/resolved",
		"model/rerouted",
		"model/verification",
		"warning",
		"guardianWarning",
		"deprecationNotice",
		"configWarning",
		"error":
		return true
	default:
		return false
	}
}

func threadIDFromMap(payload map[string]any, depth int) string {
	if depth > 3 || payload == nil {
		return ""
	}
	for _, key := range []string{"threadId", "conversationId"} {
		if value, _ := payload[key].(string); value != "" {
			return value
		}
	}
	if thread, _ := payload["thread"].(map[string]any); thread != nil {
		if value, _ := thread["id"].(string); value != "" {
			return value
		}
	}
	for _, key := range []string{"turn", "item", "params", "request"} {
		if nested, _ := payload[key].(map[string]any); nested != nil {
			if value := threadIDFromMap(nested, depth+1); value != "" {
				return value
			}
		}
	}
	return ""
}

func turnIDFromMap(payload map[string]any, depth int) string {
	if depth > 3 || payload == nil {
		return ""
	}
	for _, key := range []string{"turnId", "expectedTurnId"} {
		if value, _ := payload[key].(string); value != "" {
			return value
		}
	}
	if turn, _ := payload["turn"].(map[string]any); turn != nil {
		if value, _ := turn["id"].(string); value != "" {
			return value
		}
	}
	for _, key := range []string{"item", "params", "request"} {
		if nested, _ := payload[key].(map[string]any); nested != nil {
			if value := turnIDFromMap(nested, depth+1); value != "" {
				return value
			}
		}
	}
	return ""
}

func (c *Connector) startActiveRunFromNotification(threadID string, params json.RawMessage) *activeRun {
	turnID := notificationTurnID(params)
	if threadID == "" || turnID == "" {
		return nil
	}
	if active := c.activeRun(threadID); active != nil {
		return active
	}
	pending, hasPending := c.pendingTurnStart(threadID)
	client := pending.client
	portalKey := pending.portalKey
	if !hasPending {
		room, ok := c.lookupThreadRoom(context.Background(), threadID)
		if !ok {
			return nil
		}
		client = &Client{Main: c, UserLogin: room.login, loggedIn: true}
		portalKey = room.portalKey
	}
	if client == nil || portalKey.ID == "" {
		return nil
	}
	run := newActiveRun(client, portalKey, threadID, turnID)
	c.activeMu.Lock()
	if c.active == nil {
		c.active = map[string]*activeRun{}
	}
	if active := c.active[threadID]; active != nil {
		c.activeMu.Unlock()
		return active
	}
	c.active[threadID] = run
	c.activeMu.Unlock()
	c.rememberThreadRoom(threadID, run.client, run.portalKey, "")
	if hasPending {
		if err := run.start(context.Background()); err != nil {
			c.setActive(threadID, nil)
			if client.Main != nil {
				client.Main.queueThreadNotice(threadID, "Failed to start Codex stream:\n\n"+err.Error())
			}
			logFromContext(context.Background()).Err(err).
				Str("thread_id", threadID).
				Str("turn_id", turnID).
				Str("source", "notification").
				Msg("Failed to start Codex stream")
			return nil
		}
	} else {
		run.startAsync(context.Background(), "notification")
	}
	return run
}

func (c *Connector) setPendingTurnStart(threadID string, client *Client, portalKey networkid.PortalKey) {
	if c == nil || threadID == "" || client == nil || portalKey.ID == "" {
		return
	}
	c.pendingStartMu.Lock()
	if c.pendingStarts == nil {
		c.pendingStarts = map[string]pendingTurnStart{}
	}
	c.pendingStarts[threadID] = pendingTurnStart{client: client, portalKey: portalKey}
	c.pendingStartMu.Unlock()
}

func (c *Connector) clearPendingTurnStart(threadID string) {
	if c == nil || threadID == "" {
		return
	}
	c.pendingStartMu.Lock()
	delete(c.pendingStarts, threadID)
	c.pendingStartMu.Unlock()
}

func (c *Connector) pendingTurnStart(threadID string) (pendingTurnStart, bool) {
	if c == nil || threadID == "" {
		return pendingTurnStart{}, false
	}
	c.pendingStartMu.Lock()
	defer c.pendingStartMu.Unlock()
	pending, ok := c.pendingStarts[threadID]
	return pending, ok
}

func (c *Connector) setActive(threadID string, run *activeRun) {
	if threadID == "" {
		return
	}
	c.activeMu.Lock()
	if run == nil {
		c.forgetProcessesForThread(threadID, c.active[threadID])
		delete(c.active, threadID)
	} else {
		c.active[threadID] = run
	}
	c.activeMu.Unlock()
	if run != nil {
		c.rememberThreadRoom(threadID, run.client, run.portalKey, "")
	}
}

func (c *Connector) activeRun(threadID string) *activeRun {
	if threadID == "" {
		return nil
	}
	c.activeMu.Lock()
	defer c.activeMu.Unlock()
	return c.active[threadID]
}

func (c *Connector) forgetThread(threadID string) {
	if c == nil || threadID == "" {
		return
	}
	c.activeMu.Lock()
	run := c.active[threadID]
	delete(c.active, threadID)
	c.activeMu.Unlock()
	c.forgetProcessesForThread(threadID, run)
	c.threadMu.Lock()
	delete(c.threadRooms, threadID)
	c.threadMu.Unlock()
}

func (c *Connector) rememberProcess(processID string, run *activeRun) {
	if c == nil || processID == "" || run == nil {
		return
	}
	c.processMu.Lock()
	if c.processes == nil {
		c.processes = map[string]*activeRun{}
	}
	c.processes[processID] = run
	c.processMu.Unlock()
}

func (c *Connector) activeRunForProcess(params json.RawMessage) *activeRun {
	processID := notificationProcessID(params)
	if c == nil || processID == "" {
		return nil
	}
	c.processMu.Lock()
	defer c.processMu.Unlock()
	return c.processes[processID]
}

func (c *Connector) forgetProcessesForThread(threadID string, run *activeRun) {
	if c == nil || (threadID == "" && run == nil) {
		return
	}
	c.processMu.Lock()
	for processID, active := range c.processes {
		if active == run || (active != nil && active.threadID == threadID) {
			delete(c.processes, processID)
		}
	}
	c.processMu.Unlock()
}

func (c *Connector) rememberThreadRoom(threadID string, cl *Client, portalKey networkid.PortalKey, cwd string, modelInfo ...string) {
	if threadID == "" || cl == nil || cl.UserLogin == nil {
		return
	}
	c.threadMu.Lock()
	for existingThreadID, existing := range c.threadRooms {
		if existingThreadID != threadID && existing.portalKey == portalKey && portalKey.ID != "" {
			delete(c.threadRooms, existingThreadID)
		}
	}
	room := c.threadRooms[threadID]
	room.portalKey = portalKey
	room.login = cl.UserLogin
	if cwd != "" {
		room.cwd = cwd
	}
	if len(modelInfo) > 0 && strings.TrimSpace(modelInfo[0]) != "" {
		room.modelProvider = strings.TrimSpace(modelInfo[0])
	}
	if len(modelInfo) > 1 && strings.TrimSpace(modelInfo[1]) != "" {
		room.model = codexModelRef(room.modelProvider, modelInfo[1])
	}
	if len(modelInfo) > 2 && strings.TrimSpace(modelInfo[2]) != "" {
		room.reasoningEffort = strings.TrimSpace(modelInfo[2])
	}
	c.threadRooms[threadID] = room
	c.threadMu.Unlock()
}

func (c *Connector) threadRoom(threadID string) (threadRoom, bool) {
	c.threadMu.Lock()
	defer c.threadMu.Unlock()
	room, ok := c.threadRooms[threadID]
	return room, ok && room.login != nil && room.portalKey.ID != ""
}

func (c *Connector) modelStateForPortalKey(portalKey networkid.PortalKey) map[string]any {
	if c == nil || portalKey.ID == "" {
		return nil
	}
	c.threadMu.Lock()
	defer c.threadMu.Unlock()
	for _, room := range c.threadRooms {
		if room.portalKey != portalKey {
			continue
		}
		state := map[string]any{}
		if room.model != "" {
			state["model"] = room.model
		}
		if room.modelProvider != "" {
			state["provider"] = room.modelProvider
			state["modelProvider"] = room.modelProvider
		}
		if room.reasoningEffort != "" {
			state["effort"] = room.reasoningEffort
			state["reasoning"] = room.reasoningEffort
			state["reasoningEffort"] = room.reasoningEffort
		}
		if len(state) > 0 {
			return state
		}
	}
	return nil
}

func (c *Connector) setModelStateForPortalKey(portalKey networkid.PortalKey, state map[string]any) {
	if c == nil || portalKey.ID == "" {
		return
	}
	model := codexModelStateRef(state, firstStateString(state, "provider", "modelProvider"))
	provider := firstStateString(state, "provider", "modelProvider")
	effort := firstStateString(state, "effort", "reasoning", "reasoningEffort", "reasoning_effort")
	c.threadMu.Lock()
	for threadID, room := range c.threadRooms {
		if room.portalKey != portalKey {
			continue
		}
		room.model = model
		room.modelProvider = provider
		room.reasoningEffort = effort
		c.threadRooms[threadID] = room
	}
	c.threadMu.Unlock()
}

func (c *Connector) lookupThreadRoom(ctx context.Context, threadID string) (threadRoom, bool) {
	if room, ok := c.threadRoom(threadID); ok {
		return room, true
	}
	c.hydrateThreadRooms(ctx)
	return c.threadRoom(threadID)
}

func (c *Connector) hydrateThreadRooms(ctx context.Context) {
	if c == nil || c.Bridge == nil {
		return
	}
	portals, err := c.Bridge.GetAllPortals(ctx)
	if err != nil {
		logFromContext(ctx).Err(err).Msg("Failed to hydrate Codex thread rooms")
		return
	}
	for _, portal := range portals {
		if c.deleteUnmaterializedProjectPortal(ctx, portal) {
			continue
		}
		portal = c.hydratePortalThreadRoomState(ctx, portal)
		c.rememberPortalThreadRoom(portal)
	}
}

func (c *Connector) syncBaseContactGhosts(ctx context.Context) {
	if c == nil || c.Bridge == nil {
		return
	}
	c.syncContactGhostsForThreads(ctx, nil)
}

func (c *Connector) syncRecentContactGhosts(ctx context.Context) {
	if c == nil || c.Bridge == nil {
		return
	}
	threads, _, err := (&Client{Main: c}).listThreadPage(ctx, "", contactGhostSyncLimit)
	if err != nil {
		logFromContext(ctx).Warn().Err(err).Msg("Failed to list Codex threads for contact ghost sync")
		return
	}
	c.syncContactGhostsForThreads(ctx, threads)
}

func (c *Connector) syncContactGhostsForThreads(ctx context.Context, threads []appserver.Thread) {
	if c == nil || c.Bridge == nil {
		return
	}
	contacts := []struct {
		id   networkid.UserID
		info *bridgev2.UserInfo
	}{
		{codexUserID, codexUserInfo("Codex", false)},
		{newProjectUserID, codexUserInfo("New Project", false)},
	}
	for _, thread := range sortedRecentDirectories(threads) {
		name := directoryName(thread.Cwd)
		contacts = append(contacts, struct {
			id   networkid.UserID
			info *bridgev2.UserInfo
		}{projectUserID(thread.Cwd), projectUserInfo(thread, thread.Cwd, name)})
	}
	for _, contact := range contacts {
		ghost, err := c.Bridge.GetGhostByID(ctx, contact.id)
		if err != nil {
			logFromContext(ctx).Warn().Err(err).Str("user_id", string(contact.id)).Msg("Failed to load Codex contact ghost")
			continue
		}
		ghost.UpdateInfo(ctx, contact.info)
	}
}

func (c *Connector) deleteUnmaterializedProjectPortal(ctx context.Context, portal *bridgev2.Portal) bool {
	if portal == nil || portal.MXID != "" {
		return false
	}
	if _, ok := parseProjectPortalID(portal.PortalKey.ID); !ok {
		return false
	}
	if err := portal.Delete(ctx); err != nil {
		logFromContext(ctx).Warn().Err(err).Stringer("portal_key", portal.PortalKey).Msg("Failed to delete unmaterialized Codex project portal")
		return false
	}
	logFromContext(ctx).Info().Stringer("portal_key", portal.PortalKey).Msg("Deleted unmaterialized Codex project portal")
	return true
}

func (c *Connector) hydratePortalThreadRoomState(ctx context.Context, portal *bridgev2.Portal) *bridgev2.Portal {
	if c == nil || c.Bridge == nil || portal == nil || portal.MXID == "" {
		return portal
	}
	meta := portalMetadata(portal.Metadata)
	hadThreadID := meta.ThreadID != ""
	login := c.Bridge.GetCachedUserLoginByID(portal.PortalKey.Receiver)
	if login == nil {
		return portal
	}
	cl, ok := login.Client.(*Client)
	if !ok || cl == nil {
		cl = &Client{Main: c, UserLogin: login, loggedIn: true}
	}
	state := cl.roomCodexThreadState(ctx, portal)
	threadID := meta.ThreadID
	if threadID == "" {
		threadID = firstStateString(state, "threadId", "sessionId")
	}
	if threadID == "" {
		return portal
	}
	cwd := meta.Cwd
	if cwd == "" {
		cwd = firstStateString(state, "cwd")
	}
	if meta.ThreadID == "" {
		meta = cl.hydratePortalThreadMetadata(ctx, portal, meta)
	} else if cwd != "" && cwd != meta.Cwd {
		cl.setPortalThreadMetadata(ctx, portal, threadID, cwd)
		meta = portalMetadata(portal.Metadata)
	}
	if meta.ThreadID != "" {
		threadID = meta.ThreadID
	}
	if meta.Cwd != "" {
		cwd = meta.Cwd
	}
	if actual, err := cl.readThread(ctx, threadID, false); err == nil {
		if actual.Cwd == "" {
			actual.Cwd = cwd
		}
		return cl.syncThreadPortal(ctx, portal, actual)
	} else if isThreadNotFoundError(err) {
		c.clearMissingPortalThread(ctx, cl, portal, meta)
		return portal
	} else {
		logFromContext(ctx).Warn().Err(err).Str("thread_id", meta.ThreadID).Msg("Failed to validate Codex thread during startup")
	}
	thread := appserver.Thread{
		ID:            threadID,
		SessionID:     firstStateString(state, "sessionId"),
		Cwd:           cwd,
		ModelProvider: firstStateString(state, "modelProvider"),
		Raw:           state,
	}
	if thread.SessionID == "" {
		thread.SessionID = thread.ID
	}
	if !hadThreadID || (cwd != "" && portal.PortalKey != projectPortalKey(cwd, login.ID)) {
		synced := cl.syncThreadPortal(ctx, portal, thread)
		if synced != nil {
			return synced
		}
	} else if c != nil {
		c.rememberThreadRoom(threadID, cl, portal.PortalKey, cwd, thread.ModelProvider, threadModelRef(thread), threadReasoningEffort(thread))
	}
	return portal
}

func (c *Connector) clearMissingPortalThread(ctx context.Context, cl *Client, portal *bridgev2.Portal, meta *PortalMetadata) {
	if c == nil || cl == nil || portal == nil || meta == nil || meta.ThreadID == "" {
		return
	}
	oldThreadID := meta.ThreadID
	cwd := meta.Cwd
	cl.clearMissingThread(ctx, portal, meta)
	if cwd == "" {
		if parsed, ok := parseProjectPortalID(portal.PortalKey.ID); ok {
			cwd = parsed
		}
	}
	state := map[string]any{
		"cwd":              cwd,
		"lastNotification": "thread/not_found",
		"missingThreadId":  oldThreadID,
	}
	portal.UpdateInfo(ctx, portalInfo(directoryName(cwd), cl.codexMembers(), cwd, "", state), cl.UserLogin, nil, time.Now())
	logFromContext(ctx).Warn().Str("thread_id", oldThreadID).Str("cwd", cwd).Msg("Cleared missing Codex thread during startup")
}

func (c *Connector) rememberPortalThreadRoom(portal *bridgev2.Portal) {
	if portal == nil || portal.MXID == "" {
		return
	}
	meta := portalMetadata(portal.Metadata)
	if meta.ThreadID == "" {
		return
	}
	login := c.Bridge.GetCachedUserLoginByID(portal.PortalKey.Receiver)
	if login == nil {
		return
	}
	c.threadMu.Lock()
	room := c.threadRooms[meta.ThreadID]
	room.portalKey = portal.PortalKey
	room.login = login
	if meta.Cwd != "" {
		room.cwd = meta.Cwd
	}
	c.threadRooms[meta.ThreadID] = room
	c.threadMu.Unlock()
}

func (c *Connector) handleThreadMetadataNotification(method, threadID string, params json.RawMessage) {
	if !isThreadMetadataNotification(method) {
		return
	}
	room, ok := c.lookupThreadRoom(context.Background(), threadID)
	if !ok {
		return
	}
	state := codexThreadState(method, threadID, room.cwd, params)
	if cwd, _ := state["cwd"].(string); cwd != "" && cwd != room.cwd {
		room.cwd = cwd
	}
	room = c.canonicalizeThreadRoom(context.Background(), threadID, room)
	if provider, _ := state["modelProvider"].(string); strings.TrimSpace(provider) != "" {
		room.modelProvider = strings.TrimSpace(provider)
	}
	if method == "model/rerouted" && room.modelProvider != "" {
		state["modelProvider"] = room.modelProvider
	}
	if model := codexModelStateRef(state, room.modelProvider); model != "" {
		room.model = model
	}
	if effort := firstStateString(state, "effort", "reasoning", "reasoningEffort", "reasoning_effort"); effort != "" {
		room.reasoningEffort = effort
	}
	if codexModelStateRef(state, room.modelProvider) == "" && room.model != "" {
		state["model"] = room.model
	}
	if firstStateString(state, "modelProvider", "provider") == "" && room.modelProvider != "" {
		state["modelProvider"] = room.modelProvider
	}
	if firstStateString(state, "effort", "reasoning", "reasoningEffort", "reasoning_effort") == "" && room.reasoningEffort != "" {
		state["reasoningEffort"] = room.reasoningEffort
	}
	c.threadMu.Lock()
	c.threadRooms[threadID] = room
	c.threadMu.Unlock()
	c.updateActiveRunRoom(threadID, room)
	if room.login == nil || room.login.Bridge == nil {
		return
	}
	info := codexThreadChatInfo(room.cwd, threadID, state)
	res := room.login.QueueRemoteEvent(&simplevent.ChatInfoChange{
		EventMeta: remoteEventMeta(bridgev2.RemoteEventChatInfoChange, room.portalKey, codexUserID, time.Now()),
		ChatInfoChange: &bridgev2.ChatInfoChange{
			ChatInfo: info,
		},
	})
	if !res.Success {
		logCodexQueueFailure(context.Background(), res, "Failed to queue Codex thread metadata update", map[string]any{
			"thread_id": threadID,
			"cwd":       room.cwd,
		})
	}
}

func (c *Connector) canonicalizeThreadRoom(ctx context.Context, threadID string, room threadRoom) threadRoom {
	if c == nil || c.Bridge == nil || room.login == nil || room.cwd == "" {
		return room
	}
	target := projectPortalKey(room.cwd, room.login.ID)
	if room.portalKey == target {
		return room
	}
	result, portal, err := c.Bridge.ReIDPortal(ctx, room.portalKey, target)
	if err != nil {
		logFromContext(ctx).Warn().Err(err).
			Str("thread_id", threadID).
			Stringer("source_portal_key", room.portalKey).
			Stringer("target_portal_key", target).
			Msg("Failed to canonicalize Codex thread room")
		return room
	}
	logFromContext(ctx).Info().
		Int("result", int(result)).
		Str("thread_id", threadID).
		Stringer("source_portal_key", room.portalKey).
		Stringer("target_portal_key", target).
		Msg("Canonicalized Codex thread room")
	room.portalKey = target
	if portal != nil {
		room.portalKey = portal.PortalKey
	}
	return room
}

func (c *Connector) updateActiveRunRoom(threadID string, room threadRoom) {
	if c == nil || threadID == "" || room.portalKey.ID == "" {
		return
	}
	c.activeMu.Lock()
	active := c.active[threadID]
	c.activeMu.Unlock()
	if active == nil {
		return
	}
	active.mu.Lock()
	active.portalKey = room.portalKey
	if room.login != nil {
		if active.client == nil {
			active.client = &Client{Main: c, UserLogin: room.login, loggedIn: true}
		} else {
			active.client.Main = c
			active.client.UserLogin = room.login
			active.client.loggedIn = true
		}
	}
	if room.model != "" {
		active.setModelLocked(room.model)
	}
	active.mu.Unlock()
}

func (c *Connector) handleThreadNoticeNotification(method, threadID string, params json.RawMessage) {
	text := threadNoticeText(method, params)
	if text == "" {
		return
	}
	c.queueThreadNotice(threadID, text)
}

func (c *Connector) queueThreadNotice(threadID, text string) {
	if c == nil || strings.TrimSpace(text) == "" {
		return
	}
	ctx := context.Background()
	room, ok := c.lookupThreadRoom(ctx, threadID)
	if !ok {
		return
	}
	portal, err := c.Bridge.GetExistingPortalByKey(ctx, room.portalKey)
	if err != nil || portal == nil {
		if err != nil {
			logFromContext(ctx).Err(err).Str("thread_id", threadID).Msg("Failed to load portal for Codex notice")
		}
		return
	}
	(&Client{Main: c, UserLogin: room.login}).queueCommandNotice(portal, threadID, text)
}

func remoteEventMeta(eventType bridgev2.RemoteEventType, portalKey networkid.PortalKey, sender networkid.UserID, timestamp time.Time) simplevent.EventMeta {
	return simplevent.EventMeta{
		Type:        eventType,
		PortalKey:   portalKey,
		Sender:      bridgev2.EventSender{Sender: sender},
		Timestamp:   timestamp,
		StreamOrder: timestamp.UnixNano(),
	}
}

func codexThreadStateUpdater(state map[string]any) bridgev2.ExtraUpdater[*bridgev2.Portal] {
	return func(ctx context.Context, portal *bridgev2.Portal) bool {
		if portal == nil || portal.MXID == "" {
			return false
		}
		_, err := portal.Internal().SendStateWithIntentOrBot(ctx, nil, event.Type{Type: codexThreadStateType, Class: event.StateEventType}, "", &event.Content{Raw: state}, time.Now())
		if err != nil {
			logFromContext(ctx).Err(err).Msg("Failed to sync Codex thread state")
			return false
		}
		return true
	}
}

func codexAIModelStateUpdater(state map[string]any) bridgev2.ExtraUpdater[*bridgev2.Portal] {
	content := codexAIModelStateContent(state)
	if content == nil {
		content = map[string]any{}
	}
	return func(ctx context.Context, portal *bridgev2.Portal) bool {
		if portal == nil || portal.MXID == "" {
			return false
		}
		_, err := portal.Internal().SendStateWithIntentOrBot(ctx, nil, event.Type{Type: beeperAIModelStateType, Class: event.StateEventType}, "", &event.Content{Raw: content}, time.Now())
		if err != nil {
			logFromContext(ctx).Err(err).Msg("Failed to sync Beeper AI model state")
			return false
		}
		return true
	}
}

func codexThreadMetadataUpdater(cwd, threadID string, state map[string]any) bridgev2.ExtraUpdater[*bridgev2.Portal] {
	return bridgev2.MergeExtraUpdaters(
		codexPortalMetadataUpdater(cwd, threadID),
		codexThreadStateUpdater(state),
		codexAIModelStateUpdater(state),
		codexCommandStateUpdater(),
	)
}

func codexThreadChatInfo(cwd, threadID string, state map[string]any) *bridgev2.ChatInfo {
	if stateCwd, _ := state["cwd"].(string); strings.TrimSpace(stateCwd) != "" {
		cwd = strings.TrimSpace(stateCwd)
	}
	info := &bridgev2.ChatInfo{
		ExcludeChangesFromTimeline: true,
		CanBackfill:                threadID != "",
		Avatar:                     codexAvatar(),
		ExtraUpdates:               codexThreadMetadataUpdater(cwd, threadID, state),
	}
	if name := chatNameFromThreadState(state, cwd); name != "" {
		info.Name = &name
	}
	return info
}

func chatNameFromThreadState(state map[string]any, cwd string) string {
	for _, key := range []string{"name", "threadName", "preview"} {
		if value, _ := state[key].(string); strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	if strings.TrimSpace(cwd) != "" {
		return directoryName(cwd)
	}
	return ""
}

func codexThreadState(method, threadID, cwd string, params json.RawMessage) map[string]any {
	state := rawPayload(params)
	state["threadId"] = threadID
	state["lastNotification"] = method
	if thread, _ := state["thread"].(map[string]any); thread != nil {
		copyThreadStateFields(state, thread)
	}
	normalizeThreadState(method, state)
	if existingCwd, _ := state["cwd"].(string); strings.TrimSpace(existingCwd) == "" && cwd != "" {
		state["cwd"] = cwd
	}
	switch method {
	case "thread/archived":
		state["archived"] = true
	case "thread/unarchived":
		state["archived"] = false
	case "thread/closed":
		state["closed"] = true
	}
	return state
}

func codexThreadInitialState(thread appserver.Thread) map[string]any {
	state := copyStateMap(thread.Raw)
	delete(state, "turns")
	state["threadId"] = thread.ID
	state["lastNotification"] = "thread/started"
	if thread.SessionID != "" {
		state["sessionId"] = thread.SessionID
	}
	if thread.Cwd != "" {
		state["cwd"] = thread.Cwd
	}
	if thread.Path != "" {
		state["path"] = thread.Path
	}
	if thread.Name != "" {
		state["name"] = thread.Name
	}
	if thread.Preview != "" {
		state["preview"] = thread.Preview
	}
	if thread.ModelProvider != "" {
		state["modelProvider"] = thread.ModelProvider
	}
	if thread.CreatedAt != 0 {
		state["createdAt"] = thread.CreatedAt
	}
	if thread.UpdatedAt != 0 {
		state["updatedAt"] = thread.UpdatedAt
	}
	normalizeThreadState("thread/started", state)
	return state
}

func copyThreadStateFields(dst, thread map[string]any) {
	delete(thread, "turns")
	for _, key := range []string{"sessionId", "forkedFromId", "parentThreadId", "cwd", "path", "name", "preview", "status", "model", "modelProvider", "serviceTier", "effort", "reasoningEffort", "summary", "createdAt", "updatedAt", "ephemeral", "cliVersion", "source", "threadSource", "agentNickname", "agentRole", "gitInfo"} {
		if value, ok := thread[key]; ok {
			dst[key] = value
		}
	}
	if id, _ := thread["id"].(string); id != "" {
		dst["threadId"] = id
	}
}

func normalizeThreadState(method string, state map[string]any) {
	if state == nil {
		return
	}
	if name, _ := state["threadName"].(string); strings.TrimSpace(name) != "" {
		state["name"] = strings.TrimSpace(name)
	}
	if status, _ := state["status"].(map[string]any); status != nil {
		if statusType, _ := status["type"].(string); statusType != "" {
			state["statusType"] = statusType
		}
		if flags, ok := status["activeFlags"]; ok {
			state["activeFlags"] = flags
		}
	}
	if goal, _ := state["goal"].(map[string]any); goal != nil {
		if status, _ := goal["status"].(string); status != "" {
			state["goalStatus"] = status
		}
		if objective, _ := goal["objective"].(string); objective != "" {
			state["goalObjective"] = objective
		}
		state["goalTokensUsed"] = goal["tokensUsed"]
		state["goalTokenBudget"] = goal["tokenBudget"]
		state["goalUpdatedAt"] = goal["updatedAt"]
	}
	if method == "thread/goal/cleared" {
		state["goal"] = nil
		state["goalStatus"] = "cleared"
		delete(state, "goalObjective")
		delete(state, "goalTokensUsed")
		delete(state, "goalTokenBudget")
		delete(state, "goalUpdatedAt")
	}
	if tokenUsage, _ := state["tokenUsage"].(map[string]any); tokenUsage != nil {
		state["tokenUsageTotal"] = tokenUsage["total"]
		state["tokenUsageLast"] = tokenUsage["last"]
		state["modelContextWindow"] = tokenUsage["modelContextWindow"]
	}
	if settings, _ := state["threadSettings"].(map[string]any); settings != nil {
		copyThreadSettingsFields(state, settings)
	}
	if method == "model/rerouted" {
		if toModel, _ := state["toModel"].(string); strings.TrimSpace(toModel) != "" {
			state["model"] = strings.TrimSpace(toModel)
		}
	}
}

func copyStateMap(src map[string]any) map[string]any {
	dst := make(map[string]any, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
}

func copyThreadSettingsFields(dst, settings map[string]any) {
	for _, key := range []string{"cwd", "approvalPolicy", "approvalsReviewer", "sandboxPolicy", "activePermissionProfile", "model", "modelProvider", "serviceTier", "effort", "summary", "collaborationMode", "personality"} {
		if value, ok := settings[key]; ok {
			dst[key] = value
			dst["settings."+key] = value
		}
	}
}

func codexAIModelStateContent(state map[string]any) map[string]any {
	model := codexModelStateRef(state, "")
	if model == "" {
		return nil
	}
	content := map[string]any{"model": model}
	if effort := firstStateString(state, "effort", "reasoning", "reasoningEffort", "reasoning_effort"); effort != "" {
		content["reasoning"] = effort
	}
	if name := firstStateString(state, "modelName"); name != "" && name != firstStateString(state, "model", "toModel") {
		content["name"] = name
	}
	return content
}

func codexModelStateRef(state map[string]any, fallbackProvider string) string {
	model := firstStateString(state, "model", "toModel")
	if model == "" {
		return ""
	}
	provider := firstStateString(state, "modelProvider", "provider")
	if provider == "" {
		provider = fallbackProvider
	}
	return codexModelRef(provider, model)
}

func codexModelRef(provider, model string) string {
	provider = strings.TrimSpace(provider)
	model = strings.TrimSpace(model)
	if provider == "" || strings.Contains(model, "/") {
		return model
	}
	return provider + "/" + model
}

func threadModelRef(thread appserver.Thread) string {
	if thread.Raw != nil {
		if model := codexModelStateRef(thread.Raw, thread.ModelProvider); model != "" {
			return model
		}
	}
	if provider := strings.TrimSpace(thread.ModelProvider); strings.Contains(provider, "/") {
		return provider
	}
	return ""
}

func threadReasoningEffort(thread appserver.Thread) string {
	if thread.Raw == nil {
		return ""
	}
	return firstStateString(thread.Raw, "effort", "reasoning", "reasoningEffort", "reasoning_effort")
}

func firstStateString(state map[string]any, keys ...string) string {
	for _, key := range keys {
		if value, _ := state[key].(string); strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func isThreadMetadataNotification(method string) bool {
	switch method {
	case "thread/started",
		"thread/status/changed",
		"thread/archived",
		"thread/unarchived",
		"thread/closed",
		"thread/name/updated",
		"thread/goal/updated",
		"thread/goal/cleared",
		"thread/settings/updated",
		"thread/tokenUsage/updated",
		"thread/compacted",
		"model/rerouted",
		"model/verification":
		return true
	default:
		return false
	}
}

func isThreadNoticeNotification(method string) bool {
	switch method {
	case "thread/compacted",
		"thread/status/changed",
		"thread/archived",
		"thread/unarchived",
		"thread/closed",
		"model/rerouted",
		"model/verification",
		"thread/realtime/error",
		"thread/realtime/closed",
		"warning",
		"guardianWarning",
		"configWarning",
		"deprecationNotice",
		"error":
		return true
	default:
		return false
	}
}

func threadNoticeText(method string, params json.RawMessage) string {
	if !isThreadNoticeNotification(method) {
		return ""
	}
	payload := rawPayload(params)
	switch method {
	case "thread/compacted":
		return codexCompactionNotice
	case "thread/status/changed":
		return threadStatusErrorNotice(payload)
	case "thread/archived":
		return "Codex archived this thread."
	case "thread/unarchived":
		return "Codex unarchived this thread."
	case "thread/closed":
		return "Codex closed this thread."
	case "model/rerouted":
		from, _ := payload["fromModel"].(string)
		to, _ := payload["toModel"].(string)
		reason, _ := payload["reason"].(string)
		if from != "" && to != "" {
			text := "Codex switched models from " + from + " to " + to + "."
			if reason != "" {
				text += "\n\nReason: " + reason
			}
			return text
		}
		return "Codex switched models."
	case "model/verification":
		return "Codex is verifying the selected model."
	case "thread/realtime/error":
		if message, _ := payload["message"].(string); strings.TrimSpace(message) != "" {
			return "Codex realtime error:\n\n" + strings.TrimSpace(message)
		}
		return "Codex realtime error."
	case "thread/realtime/closed":
		if reason, _ := payload["reason"].(string); strings.TrimSpace(reason) != "" {
			return "Codex realtime closed:\n\n" + strings.TrimSpace(reason)
		}
		return ""
	case "warning", "guardianWarning", "configWarning", "deprecationNotice":
		return warningNoticeText(method, payload)
	case "error":
		return errorNoticeText(payload)
	default:
		return ""
	}
}

func warningNoticeText(method string, payload map[string]any) string {
	for _, key := range []string{"message", "summary", "warning", "text"} {
		if value, _ := payload[key].(string); strings.TrimSpace(value) != "" {
			text := strings.TrimSpace(value)
			if details, _ := payload["details"].(string); strings.TrimSpace(details) != "" {
				text += "\n\n" + strings.TrimSpace(details)
			}
			return "Codex " + method + ":\n\n" + text
		}
	}
	return "Codex " + method + "."
}

func errorNoticeText(payload map[string]any) string {
	if errPayload, _ := payload["error"].(map[string]any); errPayload != nil {
		if message, _ := errPayload["message"].(string); strings.TrimSpace(message) != "" {
			return "Codex error:\n\n" + strings.TrimSpace(message)
		}
	}
	if message, _ := payload["message"].(string); strings.TrimSpace(message) != "" {
		return "Codex error:\n\n" + strings.TrimSpace(message)
	}
	return "Codex error."
}

func threadStatusErrorNotice(payload map[string]any) string {
	status, _ := payload["status"].(map[string]any)
	statusType, _ := status["type"].(string)
	if statusType == "" {
		statusType, _ = payload["statusType"].(string)
	}
	switch strings.ToLower(strings.TrimSpace(statusType)) {
	case "systemerror", "error", "failed":
	default:
		return ""
	}
	if message := statusErrorMessage(status); message != "" {
		return "Codex error:\n\n" + message
	}
	if message := statusErrorMessage(payload); message != "" {
		return "Codex error:\n\n" + message
	}
	if model, _ := payload["model"].(string); strings.TrimSpace(model) != "" {
		return "Codex entered system error state while using " + strings.TrimSpace(model) + "."
	}
	return "Codex entered system error state."
}

func statusErrorMessage(payload map[string]any) string {
	if payload == nil {
		return ""
	}
	if message, _ := payload["message"].(string); strings.TrimSpace(message) != "" {
		return strings.TrimSpace(message)
	}
	if errPayload, _ := payload["error"].(map[string]any); errPayload != nil {
		if message, _ := errPayload["message"].(string); strings.TrimSpace(message) != "" {
			return strings.TrimSpace(message)
		}
	}
	if errText, _ := payload["error"].(string); strings.TrimSpace(errText) != "" {
		return strings.TrimSpace(errText)
	}
	return ""
}

func accountName(account *appserver.Account) string {
	if account == nil {
		return "Codex"
	}
	if account.Email != "" {
		return account.Email
	}
	if account.Type != "" {
		return "Codex " + account.Type
	}
	return "Codex"
}

func directoryName(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return "New Project"
	}
	base := filepath.Base(path)
	if base == "." || base == "/" || base == "" {
		return path
	}
	return base
}

func sortedRecentDirectories(threads []appserver.Thread) []appserver.Thread {
	latest := map[string]appserver.Thread{}
	for _, thread := range threads {
		if thread.Cwd == "" {
			continue
		}
		if current, ok := latest[thread.Cwd]; !ok || thread.UpdatedAt > current.UpdatedAt {
			latest[thread.Cwd] = thread
		}
	}
	out := make([]appserver.Thread, 0, len(latest))
	for _, thread := range latest {
		out = append(out, thread)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].UpdatedAt > out[j].UpdatedAt })
	return out
}

func stringPtr(value string) *string {
	return &value
}

func boolPtr(value bool) *bool {
	return &value
}

func userInfo(name string, isBot bool, identifiers ...string) *bridgev2.UserInfo {
	return &bridgev2.UserInfo{Name: stringPtr(name), IsBot: boolPtr(isBot), Identifiers: uniqueIdentifiers(append([]string{name}, identifiers...)...)}
}

func loginUserInfo() *bridgev2.UserInfo {
	info := userInfo("You", false)
	info.Avatar = &bridgev2.Avatar{ID: networkid.AvatarID("login-user"), Remove: true}
	return info
}

func codexAvatar() *bridgev2.Avatar {
	return &bridgev2.Avatar{
		ID:  networkid.AvatarID(defaultCodexAvatarMXC),
		MXC: id.ContentURIString(defaultCodexAvatarMXC),
	}
}

func codexUserInfo(name string, isBot bool, identifiers ...string) *bridgev2.UserInfo {
	info := userInfo(name, isBot, identifiers...)
	info.Avatar = codexAvatar()
	return info
}

func uniqueIdentifiers(values ...string) []string {
	seen := map[string]bool{}
	var out []string
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" || seen[strings.ToLower(value)] {
			continue
		}
		seen[strings.ToLower(value)] = true
		out = append(out, value)
	}
	return out
}

func portalInfo(name string, members *bridgev2.ChatMemberList, cwd, threadID string, state map[string]any) *bridgev2.ChatInfo {
	roomType := database.RoomTypeDM
	extraUpdates := bridgev2.MergeExtraUpdaters(
		codexPortalMetadataUpdater(cwd, threadID),
		codexCommandStateUpdater(),
	)
	if state != nil {
		extraUpdates = codexThreadMetadataUpdater(cwd, threadID, state)
	}
	return &bridgev2.ChatInfo{
		Name:                       stringPtr(name),
		Avatar:                     codexAvatar(),
		Type:                       &roomType,
		Members:                    members,
		ExcludeChangesFromTimeline: true,
		CanBackfill:                threadID != "",
		ExtraUpdates:               extraUpdates,
	}
}

func applyStoredPortalInfo(info *bridgev2.ChatInfo, portal *bridgev2.Portal) {
	if info == nil || portal == nil {
		return
	}
	if portal.NameSet {
		info.Name = &portal.Name
	}
	if portal.TopicSet {
		info.Topic = &portal.Topic
	}
	if portal.Disappear.Type != "" {
		disappear := portal.Disappear
		info.Disappear = &disappear
	}
}

func (c *Connector) ResolveLogin(ctx context.Context, user *bridgev2.User, requested networkid.UserLoginID) (*bridgev2.UserLogin, error) {
	if requested == "" {
		requested = c.defaultLoginIDForUser(user)
	}
	if cached := c.Bridge.GetCachedUserLoginByID(requested); cached != nil && cached.UserMXID == user.MXID {
		return cached, nil
	}
	if !c.isLoginIDForUser(user.MXID, requested) {
		return nil, fmt.Errorf("unknown Codex login %s", requested)
	}
	account, err := c.readAccount(ctx)
	if err != nil {
		return nil, err
	}
	if account == nil {
		return nil, bridgev2.ErrNotLoggedIn
	}
	return c.ensureLoginID(ctx, user, requested, accountName(account))
}

func (c *Connector) loginIDForUser(_ id.UserID) networkid.UserLoginID {
	if c == nil || c.Bridge == nil {
		return defaultLoginID
	}
	if mx, ok := c.Bridge.Matrix.(*matrixconnector.Connector); ok && mx.Config != nil {
		botUsername := strings.TrimSpace(mx.Config.AppService.Bot.Username)
		if alias, ok := strings.CutSuffix(botUsername, "bot"); ok {
			return networkid.UserLoginID(alias)
		}
		template := strings.TrimSpace(mx.Config.AppService.UsernameTemplate)
		if prefix, _, ok := strings.Cut(template, "_{{"); ok {
			return networkid.UserLoginID(prefix)
		}
	}
	if c.Bridge.ID != "" {
		return networkid.UserLoginID(c.Bridge.ID)
	}
	return defaultLoginID
}

func (c *Connector) loginIDsForUser(userID id.UserID) []networkid.UserLoginID {
	return []networkid.UserLoginID{c.loginIDForUser(userID)}
}

func (c *Connector) isLoginIDForUser(userID id.UserID, loginID networkid.UserLoginID) bool {
	for _, allowed := range c.loginIDsForUser(userID) {
		if loginID == allowed {
			return true
		}
	}
	return false
}

func logFromContext(ctx context.Context) *zerolog.Logger {
	log := zerolog.Ctx(ctx)
	return log
}
