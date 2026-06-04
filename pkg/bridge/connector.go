package bridge

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
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
const bridgeStartupSyncTimeout = 2 * time.Minute
const contactGhostSyncLimit = 20
const bridgeInfoVersion = 2
const roomCapabilitiesVersion = 12
const roomFeaturesID = "com.beeper.codex.capabilities.2026_06_03.room_features_user_ai_model_and_edits"
const defaultCodexAvatarMXC = "mxc://beeper.com/51a668657dd9e0132cc823ad9402c6c2d0fc3321"

var codexRequestLogFields = []struct {
	key string
	log string
}{
	{"threadId", "thread_id"},
	{"turnId", "turn_id"},
	{"expectedTurnId", "expected_turn_id"},
	{"cwd", "cwd"},
	{"model", "model"},
	{"approvalPolicy", "approval_policy"},
	{"clientUserMessageId", "client_user_message_id"},
}

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

	matrixStartMu sync.Mutex
	matrixStarts  map[string]struct{}

	warmThreadMu sync.Mutex
	warmThreads  map[string]struct{}

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
	modelName       string
	reasoningEffort string
	reasoningMode   string
}

type threadRoomUpdate struct {
	threadID string
	room     threadRoom
}

type contactGhostUpdate struct {
	id   networkid.UserID
	info *bridgev2.UserInfo
}

func (room *threadRoom) applyState(state map[string]any) {
	if room == nil || len(state) == 0 {
		return
	}
	room.applyModelFields(state, []string{"modelProvider", "provider"}, []string{"modelName"}, true)
}

func (room threadRoom) fillState(state map[string]any) {
	if len(state) == 0 {
		return
	}
	setMissingStateStrings(state, room.missingStateFields()...)
}

func (room threadRoom) missingStateFields() []missingStateField {
	return []missingStateField{
		missingStateStringField("model", room.model, "model", "toModel"),
		missingStateStringField("modelProvider", room.modelProvider, "modelProvider", "provider"),
		missingStateStringField("reasoningEffort", room.reasoningEffort, "effort", "reasoning", "reasoningEffort", "reasoning_effort"),
		missingStateStringField("modelName", room.modelName, "modelName"),
		missingStateStringField("reasoning_mode", room.reasoningMode, "reasoning_mode", "reasoningMode"),
	}
}

func (room *threadRoom) applyModelState(state map[string]any) {
	if room == nil {
		return
	}
	if len(state) == 0 {
		room.clearModelFields()
		return
	}
	room.applyModelFields(state, []string{"provider", "modelProvider"}, []string{"modelName", "name"}, false)
}

func (room *threadRoom) applyModelFields(state map[string]any, providerKeys, nameKeys []string, useRoomProviderFallback bool) {
	modelProvider := firstString(state, providerKeys...)
	setStateString(&room.modelProvider, state, providerKeys...)
	if useRoomProviderFallback {
		modelProvider = room.modelProvider
	}
	setNonEmptyStringField(&room.model, codexModelStateRef(state, modelProvider))
	setStateString(&room.modelName, state, nameKeys...)
	setStateString(&room.reasoningEffort, state, "effort", "reasoning", "reasoningEffort", "reasoning_effort")
	setStateString(&room.reasoningMode, state, "reasoning_mode", "reasoningMode")
}

func (room *threadRoom) clearModelFields() {
	room.model = ""
	room.modelProvider = ""
	room.modelName = ""
	room.reasoningEffort = ""
	room.reasoningMode = ""
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
	c.matrixStarts = map[string]struct{}{}
	c.warmThreads = map[string]struct{}{}
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
	runBridgeStartupSync(ctx, c.seedConfiguredLogins)
	runBridgeStartupSync(ctx, c.hydrateThreadRooms)
	runBridgeStartupSync(ctx, func(syncCtx context.Context) {
		c.syncContactGhostsForThreads(syncCtx, nil)
	})
	go c.syncRecentContactGhosts(ctx)
	go c.dispatchAppServer()
	return nil
}

func runBridgeStartupSync(ctx context.Context, sync func(context.Context)) {
	syncCtx, cancel := context.WithTimeout(ctx, bridgeStartupSyncTimeout)
	defer cancel()
	sync(syncCtx)
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
	login.Client = newLoggedInClient(c, login)
	return nil
}

func newLoggedInClient(connector *Connector, login *bridgev2.UserLogin) *Client {
	return &Client{Main: connector, UserLogin: login, loggedIn: true}
}

func (c *Connector) appClient() (*appserver.Client, error) {
	c.appMu.Lock()
	defer c.appMu.Unlock()
	if c.app != nil {
		return c.app, nil
	}
	return nil, fmt.Errorf("Codex app-server is not running")
}

func (c *Connector) request(ctx context.Context, method string, params any, out any) error {
	app, err := c.appClient()
	if err != nil {
		return err
	}
	logRequest := func() *zerolog.Event {
		logger := zerolog.Ctx(ctx).With().Str("method", method).Logger()
		ev := logger.Debug()
		fields, _ := params.(map[string]any)
		for _, field := range codexRequestLogFields {
			if value := firstString(fields, field.key); value != "" {
				ev = ev.Str(field.log, value)
			}
		}
		return ev
	}
	logRequest().Msg("Sending Codex app-server request")
	err = app.Request(ctx, method, params, out)
	if err != nil {
		logRequest().Err(err).Msg("Codex app-server request failed")
	}
	return err
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
	threadID := notificationThreadID(msg.Params)
	var active *activeRun
	if threadID != "" {
		active = c.activeRun(threadID)
		if active == nil {
			active = c.startActiveRunFromNotification(threadID, msg.Params)
		}
	}
	if active == nil {
		_ = app.RespondError(ctx, msg.ID, -32000, "No active Codex turn for server request", serverRequestErrorMetadata(msg.Method))
		return
	}
	result, err := active.handleServerRequest(ctx, msg)
	if err != nil {
		_ = app.RespondError(ctx, msg.ID, -32001, err.Error(), serverRequestErrorMetadata(msg.Method))
		return
	}
	_ = app.Respond(ctx, msg.ID, result)
}

func serverRequestErrorMetadata(method string) map[string]any {
	return map[string]any{"method": method}
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
	active := c.activeRun(threadID)
	if active == nil && isActiveRunNotification(method) {
		active = c.startActiveRunFromNotification(threadID, params)
	}
	if active == nil {
		c.handleThreadNoticeNotification(method, threadID, params)
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
		if explicitlyFailed(payload) {
			message := firstNonEmptyString(firstString(payload, "error", "message"), "Codex login failed")
			c.broadcastBridgeState(status.StateLoggedOut, message)
			return
		}
		go c.refreshAccountState(context.Background())
	case "account/rateLimits/updated",
		"app/list/updated",
		"remoteControl/status/changed",
		"externalAgentConfig/import/completed",
		"skills/changed",
		"fuzzyFileSearch/sessionUpdated",
		"fuzzyFileSearch/sessionCompleted",
		"fs/changed":
		c.broadcastGlobalState(method, payload, status.StateConnected, "")
	case "mcpServer/oauthLogin/completed":
		c.updateGlobalState(method, payload)
		name := prefixedLabel("MCP server", firstString(payload, "name", "server", "serverName"))
		c.broadcastConditionalCompletionState(name, payload, "OAuth login failed", "OAuth login completed")
	case "windows/worldWritableWarning":
		c.broadcastGlobalState(method, payload, status.StateConnected, windowsWorldWritableWarningMessage(payload))
	case "windowsSandbox/setupCompleted":
		c.updateGlobalState(method, payload)
		mode := prefixedLabel("Windows sandbox", firstString(payload, "mode"))
		c.broadcastConditionalCompletionState(mode, payload, "setup failed", "setup completed")
	case "warning", "configWarning", "deprecationNotice", "guardianWarning":
		text := threadNoticeText(method, params)
		c.broadcastGlobalState(method, payload, status.StateConnected, text)
	case "mcpServer/startupStatus/updated":
		c.broadcastGlobalState(method, payload, status.StateConnected, mcpStartupStatusMessage(payload))
	case "error":
		text := errorNoticeText(payload)
		c.broadcastGlobalState(method, payload, status.StateUnknownError, text)
	default:
		zerolog.Ctx(context.Background()).Debug().Str("method", method).RawJSON("params", params).Msg("Ignoring global Codex notification")
	}
}

func windowsWorldWritableWarningMessage(payload map[string]any) string {
	paths := windowsWorldWritablePaths(payload)
	message := "Codex found world-writable Windows paths."
	if len(paths) != 0 {
		message = "Codex found world-writable Windows paths:\n\n" + strings.Join(paths, "\n")
		if extra, ok := numberFromAny(payload["extraCount"]); ok && extra > 0 {
			message += fmt.Sprintf("\n\nAnd %d more.", extra)
		}
	}
	if firstBool(payload, "failedScan") {
		message += "\n\nThe scan did not complete."
	}
	return message
}

func windowsWorldWritablePaths(payload map[string]any) []string {
	if paths := stringList(payload["samplePaths"]); len(paths) != 0 {
		return paths
	}
	return stringList(payload["paths"])
}

func mcpStartupStatusMessage(payload map[string]any) string {
	server := "A Codex MCP server"
	if name := firstString(payload, "name"); name != "" {
		server = "Codex MCP server " + name
	}
	switch lowerTrimmed(firstString(payload, "status")) {
	case "failed":
		return sentenceWithOptionalInlineDetail(server+" failed to start", firstString(payload, "error", "message"))
	case "cancelled", "canceled":
		return server + " startup was cancelled."
	default:
		return ""
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
	for key, value := range codexNotificationState(method, payload) {
		c.globalState[key] = value
	}
	c.globalState["updatedAt"] = time.Now().Format(time.RFC3339Nano)
	c.globalMu.Unlock()
}

func (c *Connector) broadcastGlobalState(method string, payload map[string]any, stateEvent status.BridgeStateEvent, message string) {
	c.updateGlobalState(method, payload)
	c.broadcastBridgeState(stateEvent, message)
}

func (c *Connector) globalBridgeInfo() map[string]any {
	if c == nil {
		return nil
	}
	c.globalMu.Lock()
	defer c.globalMu.Unlock()
	out := map[string]any{
		"bridge_name": c.bridgeAlias(),
	}
	setNonEmptyMapString(out, "hostname", codexHostname())
	maps.Copy(out, c.globalState)
	return out
}

func (c *Connector) broadcastBridgeState(stateEvent status.BridgeStateEvent, message string) {
	if c == nil || c.Bridge == nil {
		return
	}
	message = firstTrimmedNonEmpty(message)
	for _, login := range c.Bridge.GetAllCachedUserLogins() {
		if login == nil || login.BridgeState == nil {
			continue
		}
		login.BridgeState.Send(status.BridgeState{StateEvent: stateEvent, Message: message})
	}
}

func (c *Connector) broadcastCompletionState(subject, action string, payload map[string]any) {
	label := subject + " " + action
	c.broadcastBridgeState(status.StateConnected, sentenceWithOptionalInlineDetail(label, firstString(payload, "error", "message")))
}

func (c *Connector) broadcastConditionalCompletionState(subject string, payload map[string]any, failedAction, completedAction string) {
	if explicitlyFailed(payload) {
		c.broadcastCompletionState(subject, failedAction, payload)
		return
	}
	c.broadcastCompletionState(subject, completedAction, nil)
}

func explicitlyFailed(payload map[string]any) bool {
	success, ok := payload["success"].(bool)
	return ok && !success
}

func (c *Connector) refreshAccountState(ctx context.Context) {
	account, err := c.readAccount(ctx)
	if err != nil {
		zerolog.Ctx(ctx).Err(err).Msg("Failed to refresh Codex account after notification")
		c.broadcastBridgeState(status.StateUnknownError, err.Error())
		return
	}
	if account == nil {
		c.broadcastBridgeState(status.StateLoggedOut, "Codex is not logged in locally")
		return
	}
	displayName := accountName(account)
	c.updateGlobalState("account/read", c.accountGlobalState(account))
	if c.Bridge != nil {
		for _, login := range c.Bridge.GetAllCachedUserLogins() {
			if login == nil {
				continue
			}
			login.RemoteProfile = loginRemoteProfile(displayName, login.ID)
			login.RemoteName = login.RemoteProfile.Name
			if err := login.Save(ctx); err != nil {
				zerolog.Ctx(ctx).Err(err).Str("login_id", string(login.ID)).Msg("Failed to save refreshed Codex login")
			}
		}
	}
	c.broadcastBridgeState(status.StateConnected, "")
}

func (c *Connector) accountGlobalState(account *appserver.Account) map[string]any {
	state := map[string]any{
		"bridge_name": c.bridgeAlias(),
		"hostname":    codexHostname(),
	}
	setAccountStateFields(state, account)
	return state
}

func setAccountStateFields(state map[string]any, account *appserver.Account) {
	if account != nil {
		state["type"] = account.Type
		state["email"] = account.Email
	}
}

func notificationThreadID(params json.RawMessage) string {
	return nestedIDFromMap(rawPayload(params), 0, []string{"threadId", "conversationId"}, []string{"thread"}, []string{"turn", "item", "params", "request"})
}

func isActiveRunNotification(method string) bool {
	if isThreadMetadataNotification(method) || isThreadNoticeNotification(method) {
		return true
	}
	switch method {
	case "turn/started",
		"turn/completed",
		"turn/diff/updated",
		"turn/plan/updated",
		"hook/started",
		"hook/completed",
		"item/started",
		"item/completed",
		"item/agentMessage/delta",
		"item/reasoning/summaryTextDelta",
		"item/reasoning/textDelta",
		"item/reasoning/summaryPartAdded",
		"item/commandExecution/outputDelta",
		"item/fileChange/outputDelta",
		"item/commandExecution/terminalInteraction",
		"item/plan/delta",
		"item/mcpToolCall/progress",
		"item/fileChange/patchUpdated",
		"item/autoApprovalReview/started",
		"item/autoApprovalReview/completed",
		"command/exec/outputDelta",
		"process/outputDelta",
		"process/exited",
		"rawResponseItem/completed",
		"serverRequest/resolved",
		"thread/realtime/started",
		"thread/realtime/itemAdded",
		"thread/realtime/transcript/delta",
		"thread/realtime/transcript/done",
		"thread/realtime/outputAudio/delta",
		"thread/realtime/sdp":
		return true
	default:
		return false
	}
}

func nestedIDFromMap(payload map[string]any, depth int, directKeys, objectKeys, nestedKeys []string) string {
	if depth > 3 || payload == nil {
		return ""
	}
	if value := firstString(payload, directKeys...); value != "" {
		return value
	}
	for _, key := range objectKeys {
		object, _ := payload[key].(map[string]any)
		if object == nil {
			continue
		}
		if value := firstString(object, "id"); value != "" {
			return value
		}
	}
	for _, key := range nestedKeys {
		nested, _ := payload[key].(map[string]any)
		if nested == nil {
			continue
		}
		if value := nestedIDFromMap(nested, depth+1, directKeys, objectKeys, nestedKeys); value != "" {
			return value
		}
	}
	return ""
}

func (c *Connector) startActiveRunFromNotification(threadID string, params json.RawMessage) *activeRun {
	turnID := nestedIDFromMap(rawPayload(params), 0, []string{"turnId", "expectedTurnId"}, []string{"turn"}, []string{"item", "params", "request"})
	if threadID == "" || turnID == "" {
		return nil
	}
	if active := c.activeRun(threadID); active != nil {
		return active
	}
	client, portalKey, hasPending := c.activeRunStartupTarget(threadID)
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
	c.rememberThreadRoom(threadID, run.client, run.portalKey, "", nil)
	if !hasPending {
		run.startAsync(context.Background())
		return run
	}
	if err := run.start(context.Background()); err != nil {
		run.handleStartFailure(context.Background(), err, "notification")
		return nil
	}
	return run
}

func (c *Connector) activeRunStartupTarget(threadID string) (*Client, networkid.PortalKey, bool) {
	pending, ok := c.pendingTurnStart(threadID)
	if ok {
		return pending.client, pending.portalKey, true
	}
	room, ok := c.lookupThreadRoom(context.Background(), threadID)
	if !ok {
		return nil, networkid.PortalKey{}, false
	}
	return newLoggedInClient(c, room.login), room.portalKey, false
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
		c.activeMu.Unlock()
		return
	}
	c.active[threadID] = run
	c.activeMu.Unlock()
	c.rememberThreadRoom(threadID, run.client, run.portalKey, "", nil)
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
	c.forgetWarmThread(threadID)
	c.activeMu.Lock()
	run := c.active[threadID]
	delete(c.active, threadID)
	c.activeMu.Unlock()
	c.forgetProcessesForThread(threadID, run)
	c.threadMu.Lock()
	delete(c.threadRooms, threadID)
	c.threadMu.Unlock()
}

func (c *Connector) stopThreadBridging(ctx context.Context, threadID string) {
	if c == nil || threadID == "" {
		return
	}
	if active := c.activeRun(threadID); active != nil {
		active.mu.Lock()
		active.stopPublisherLocked(ctx)
		active.deletePersistedLocked(ctx)
		active.mu.Unlock()
	}
	c.forgetThread(threadID)
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
	processID := firstString(rawPayload(params), "processId", "processHandle")
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

func (c *Connector) rememberThreadRoom(threadID string, cl *Client, portalKey networkid.PortalKey, cwd string, state map[string]any) {
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
	setNonEmptyStringField(&room.cwd, cwd)
	room.applyState(state)
	c.threadRooms[threadID] = room
	c.threadMu.Unlock()
}

func (c *Connector) threadRoom(threadID string) (threadRoom, bool) {
	c.threadMu.Lock()
	defer c.threadMu.Unlock()
	room, ok := c.threadRooms[threadID]
	return room, ok && room.login != nil && room.portalKey.ID != ""
}

func (c *Connector) cachedThreadsForLogin(login *bridgev2.UserLogin) []appserver.Thread {
	if c == nil {
		return nil
	}
	c.threadMu.Lock()
	defer c.threadMu.Unlock()
	threads := make([]appserver.Thread, 0, len(c.threadRooms))
	for threadID, room := range c.threadRooms {
		if room.cwd == "" || (login != nil && (room.login == nil || room.login.ID != login.ID)) {
			continue
		}
		threads = append(threads, appserver.Thread{
			ID:            threadID,
			SessionID:     threadID,
			Cwd:           room.cwd,
			ModelProvider: room.modelProvider,
			Raw:           threadRoomRawState(room),
		})
	}
	return threads
}

func threadRoomRawState(room threadRoom) map[string]any {
	raw := map[string]any{}
	setNonEmptyMapStrings(raw,
		mapStringField{"model", room.model},
		mapStringField{"modelName", room.modelName},
		mapStringField{"reasoningEffort", room.reasoningEffort},
		mapStringField{"reasoning_mode", room.reasoningMode},
	)
	return raw
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
		state := threadRoomModelState(room)
		if len(state) > 0 {
			return state
		}
	}
	return nil
}

func threadRoomModelState(room threadRoom) map[string]any {
	state := map[string]any{}
	setNonEmptyMapStringAliases(state, room.model, "model")
	setNonEmptyMapStringAliases(state, room.modelName, "name", "modelName")
	setNonEmptyMapStringAliases(state, room.modelProvider, "provider", "modelProvider")
	setNonEmptyMapStringAliases(state, room.reasoningEffort, "effort", "reasoning", "reasoningEffort")
	setNonEmptyMapStringAliases(state, room.reasoningMode, "reasoning_mode", "reasoningMode")
	return state
}

func (c *Connector) setModelStateForPortalKey(portalKey networkid.PortalKey, state map[string]any) {
	if c == nil || portalKey.ID == "" {
		return
	}
	for _, update := range c.applyModelStateForPortalKey(portalKey, state) {
		c.updateActiveRunRoom(update.threadID, update.room)
		if active := c.activeRun(update.threadID); active != nil {
			active.writeCodexRoomState(beeperAIModelStateType, copyStateMap(state))
		}
	}
}

func (c *Connector) applyModelStateForPortalKey(portalKey networkid.PortalKey, state map[string]any) []threadRoomUpdate {
	var activeUpdates []threadRoomUpdate
	c.threadMu.Lock()
	defer c.threadMu.Unlock()
	for threadID, room := range c.threadRooms {
		if room.portalKey != portalKey {
			continue
		}
		room.applyModelState(state)
		c.threadRooms[threadID] = room
		activeUpdates = append(activeUpdates, threadRoomUpdate{threadID: threadID, room: room})
	}
	return activeUpdates
}

func (c *Connector) claimMatrixStart(key string) bool {
	if c == nil || key == "" {
		return false
	}
	c.matrixStartMu.Lock()
	defer c.matrixStartMu.Unlock()
	if c.matrixStarts == nil {
		c.matrixStarts = map[string]struct{}{}
	}
	if _, ok := c.matrixStarts[key]; ok {
		return false
	}
	c.matrixStarts[key] = struct{}{}
	return true
}

func (c *Connector) finishMatrixStart(key string) {
	if c == nil || key == "" {
		return
	}
	c.matrixStartMu.Lock()
	delete(c.matrixStarts, key)
	c.matrixStartMu.Unlock()
}

func (c *Connector) rememberWarmThread(threadID string) {
	if c == nil || threadID == "" {
		return
	}
	c.warmThreadMu.Lock()
	if c.warmThreads == nil {
		c.warmThreads = map[string]struct{}{}
	}
	c.warmThreads[threadID] = struct{}{}
	c.warmThreadMu.Unlock()
}

func (c *Connector) forgetWarmThread(threadID string) {
	if c == nil || threadID == "" {
		return
	}
	c.warmThreadMu.Lock()
	delete(c.warmThreads, threadID)
	c.warmThreadMu.Unlock()
}

func (c *Connector) isWarmThread(threadID string) bool {
	if c == nil || threadID == "" {
		return false
	}
	c.warmThreadMu.Lock()
	defer c.warmThreadMu.Unlock()
	_, ok := c.warmThreads[threadID]
	return ok
}

func (c *Connector) lookupThreadRoom(ctx context.Context, threadID string) (threadRoom, bool) {
	if room, ok := c.threadRoom(threadID); ok {
		return room, true
	}
	c.hydrateThreadRooms(ctx)
	return c.threadRoom(threadID)
}

func (c *Connector) hydrateThreadRooms(ctx context.Context) {
	c.reconcilePortals(ctx, "")
}

func (c *Connector) reconcileLoginPortals(ctx context.Context, loginID networkid.UserLoginID) {
	if loginID == "" {
		return
	}
	c.reconcilePortals(ctx, loginID)
}

func (c *Connector) reconcilePortals(ctx context.Context, receiver networkid.UserLoginID) {
	if c == nil || c.Bridge == nil {
		return
	}
	portals, err := c.Bridge.GetAllPortals(ctx)
	if err != nil {
		zerolog.Ctx(ctx).Err(err).Msg("Failed to hydrate Codex thread rooms")
		return
	}
	for _, portal := range portals {
		if portal == nil || (receiver != "" && portal.PortalKey.Receiver != receiver) {
			continue
		}
		if portal = c.reconcileStartupPortal(ctx, portal); portal != nil {
			c.rememberPortalThreadRoom(portal)
		}
	}
}

func (c *Connector) reconcileStartupPortal(ctx context.Context, portal *bridgev2.Portal) *bridgev2.Portal {
	if portal == nil {
		return nil
	}
	if _, ok := parseProjectPortalID(portal.PortalKey.ID); portal.MXID == "" && ok {
		err := portal.Delete(ctx)
		if err == nil {
			zerolog.Ctx(ctx).Info().Stringer("portal_key", portal.PortalKey).Msg("Deleted unmaterialized Codex project portal")
			return nil
		}
		zerolog.Ctx(ctx).Warn().Err(err).Stringer("portal_key", portal.PortalKey).Msg("Failed to delete unmaterialized Codex project portal")
	}
	if c == nil || c.Bridge == nil {
		return portal
	}
	login := c.Bridge.GetCachedUserLoginByID(portal.PortalKey.Receiver)
	if login == nil {
		return portal
	}
	cl, ok := login.Client.(*Client)
	if !ok || cl == nil {
		cl = newLoggedInClient(c, login)
	}
	if portal.MXID != "" {
		updateStartupPortalInfo(ctx, portal, login, cl)
	}
	return c.hydratePortalThreadRoomState(ctx, portal, cl, login)
}

func updateStartupPortalInfo(ctx context.Context, portal *bridgev2.Portal, login *bridgev2.UserLogin, cl *Client) {
	info := &bridgev2.ChatInfo{Members: cl.codexMembers()}
	if strings.HasPrefix(string(portal.ID), newPortalIDPrefix) {
		updatePortalInfo(ctx, portal, login, cl.newProjectChatInfo())
		return
	}
	portal.UpdateInfo(ctx, info, login, nil, time.Now())
}

func (c *Connector) syncRecentContactGhosts(ctx context.Context) {
	if c == nil || c.Bridge == nil {
		return
	}
	var resp appserver.ThreadListResponse
	err := c.request(ctx, "thread/list", threadListParams("", contactGhostSyncLimit), &resp)
	threads := resp.Data
	if err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).Msg("Failed to list Codex threads for contact ghost sync")
		threads = c.cachedThreadsForLogin(nil)
	}
	c.syncContactGhostsForThreads(ctx, threads)
}

func (c *Connector) syncContactGhostsForThreads(ctx context.Context, threads []appserver.Thread) {
	if c == nil || c.Bridge == nil {
		return
	}
	for _, contact := range contactGhostUpdates(threads) {
		ghost, err := c.Bridge.GetGhostByID(ctx, contact.id)
		if err != nil {
			zerolog.Ctx(ctx).Warn().Err(err).Str("user_id", string(contact.id)).Msg("Failed to load Codex contact ghost")
			continue
		}
		ghost.UpdateInfo(ctx, contact.info)
	}
}

func contactGhostUpdates(threads []appserver.Thread) []contactGhostUpdate {
	contacts := []contactGhostUpdate{
		{codexUserID, codexUserInfo("Codex", false)},
	}
	for _, thread := range sortedRecentDirectories(threads) {
		contacts = append(contacts, contactGhostUpdate{projectUserID(thread.Cwd), projectThreadUserInfo(thread)})
	}
	return contacts
}

func (c *Connector) hydratePortalThreadRoomState(ctx context.Context, portal *bridgev2.Portal, cl *Client, login *bridgev2.UserLogin) *bridgev2.Portal {
	if c == nil || c.Bridge == nil || portal == nil || portal.MXID == "" {
		return portal
	}
	meta := portalMetadata(portal.Metadata)
	hadThreadID := meta.ThreadID != ""
	if cl == nil || login == nil {
		return portal
	}
	state := cl.roomCodexThreadState(ctx, portal)
	threadID := firstNonEmptyString(meta.ThreadID, firstString(state, "threadId", "sessionId"))
	if threadID == "" {
		return portal
	}
	cwd := firstNonEmptyString(meta.Cwd, firstString(state, "cwd"))
	switch {
	case meta.ThreadID == "":
		meta = cl.hydratePortalThreadMetadata(ctx, portal, meta)
	case cwd != "" && cwd != meta.Cwd:
		cl.syncProjectPortalMetadata(ctx, portal, cwd, threadID)
		meta = portalMetadata(portal.Metadata)
	}
	threadID = firstNonEmptyString(meta.ThreadID, threadID)
	cwd = firstNonEmptyString(meta.Cwd, cwd)
	isSubagent := meta.Kind == portalKindSubagent
	parentThreadID := firstNonEmptyString(meta.ParentThreadID, firstString(state, "parentThreadId", "parent_thread_id"))
	actual, err := cl.readThread(ctx, threadID, false)
	if err == nil {
		if actual.Cwd == "" {
			actual.Cwd = cwd
		}
		if isDetachedThread(actual) {
			c.detachThreadPortal(ctx, threadID, threadRoom{portalKey: portal.PortalKey, login: login, cwd: actual.Cwd}, "startup/reconcile")
			return nil
		}
		if isSubagent {
			return cl.syncSubagentPortal(ctx, portal, parentThreadID, actual)
		}
		return cl.syncThreadPortal(ctx, portal, actual)
	}
	if isThreadNotFoundError(err) {
		if isSubagent {
			c.detachThreadPortal(ctx, threadID, threadRoom{portalKey: portal.PortalKey, login: login, cwd: cwd}, "startup/reconcile")
			return nil
		}
		c.clearMissingPortalThread(ctx, cl, portal, meta)
		return portal
	}
	zerolog.Ctx(ctx).Warn().Err(err).Str("thread_id", meta.ThreadID).Msg("Failed to validate Codex thread during startup")
	thread := startupFallbackThread(threadID, cwd, state)
	if isSubagent {
		cl.syncSubagentPortal(ctx, portal, parentThreadID, thread)
		return portal
	}
	if !hadThreadID || (cwd != "" && portal.PortalKey != projectPortalKey(cwd, login.ID)) {
		synced := cl.syncThreadPortal(ctx, portal, thread)
		if synced == nil {
			return portal
		}
		return synced
	}
	c.rememberThreadRoom(threadID, cl, portal.PortalKey, cwd, state)
	return portal
}

func startupFallbackThread(threadID, cwd string, state map[string]any) appserver.Thread {
	thread := appserver.Thread{
		ID:            threadID,
		SessionID:     firstString(state, "sessionId"),
		Cwd:           cwd,
		ModelProvider: firstString(state, "modelProvider", "provider"),
		Raw:           state,
	}
	if thread.SessionID == "" {
		thread.SessionID = thread.ID
	}
	return thread
}

func (c *Connector) clearMissingPortalThread(ctx context.Context, cl *Client, portal *bridgev2.Portal, meta *PortalMetadata) {
	if c == nil || cl == nil || portal == nil || meta == nil || meta.ThreadID == "" {
		return
	}
	oldThreadID := meta.ThreadID
	cwd := missingThreadCWD(portal, meta.Cwd)
	cl.clearMissingThread(ctx, portal, meta)
	state := missingThreadState(cwd, oldThreadID)
	portal.UpdateInfo(ctx, portalInfo(directoryName(cwd), cl.codexMembers(), cwd, "", state), cl.UserLogin, nil, time.Now())
	zerolog.Ctx(ctx).Warn().Str("thread_id", oldThreadID).Str("cwd", cwd).Msg("Cleared missing Codex thread during startup")
}

func missingThreadCWD(portal *bridgev2.Portal, cwd string) string {
	if cwd != "" || portal == nil {
		return cwd
	}
	parsed, _ := parseProjectPortalID(portal.PortalKey.ID)
	return parsed
}

func missingThreadState(cwd, threadID string) map[string]any {
	return map[string]any{
		"cwd":              cwd,
		"lastNotification": "thread/not_found",
		"missingThreadId":  threadID,
	}
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
	setNonEmptyStringField(&room.cwd, meta.Cwd)
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
	if cwd := firstString(state, "cwd"); cwd != "" && cwd != room.cwd {
		room.cwd = cwd
	}
	room = c.canonicalizeThreadRoom(context.Background(), threadID, room)
	room.fillState(state)
	room.applyState(state)
	room.fillState(state)
	c.threadMu.Lock()
	c.threadRooms[threadID] = room
	c.threadMu.Unlock()
	c.updateActiveRunRoom(threadID, room)
	if active := c.activeRun(threadID); active != nil {
		active.writeCodexThreadRoomState(copyStateMap(state))
	}
	switch method {
	case "thread/archived", "thread/closed":
		c.detachThreadPortal(context.Background(), threadID, room, method)
		return
	}
	if room.login == nil || room.login.Bridge == nil {
		return
	}
	info := codexThreadChatInfo(room.cwd, threadID, state)
	if c.Bridge != nil {
		portal, err := c.Bridge.GetExistingPortalByKey(context.Background(), room.portalKey)
		if err != nil {
			zerolog.Ctx(context.Background()).Warn().Err(err).Str("thread_id", threadID).Stringer("portal_key", room.portalKey).Msg("Failed to load portal before Codex thread metadata update")
		} else {
			applyStoredPortalInfo(info, portal)
		}
	}
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

func (c *Connector) detachThreadPortal(ctx context.Context, threadID string, room threadRoom, reason string) {
	if c == nil || threadID == "" {
		return
	}
	c.stopThreadBridging(ctx, threadID)
	if room.login == nil || room.portalKey.ID == "" {
		return
	}
	res := room.login.QueueRemoteEvent(&simplevent.ChatDelete{
		EventMeta: remoteEventMeta(bridgev2.RemoteEventChatDelete, room.portalKey, codexUserID, time.Now()),
		OnlyForMe: true,
	})
	if !res.Success {
		logCodexQueueFailure(ctx, res, "Failed to queue Codex thread detach", map[string]any{
			"thread_id": threadID,
			"reason":    reason,
			"cwd":       room.cwd,
		})
		return
	}
	zerolog.Ctx(ctx).Info().
		Str("thread_id", threadID).
		Str("reason", reason).
		Stringer("portal_key", room.portalKey).
		Msg("Detached Codex thread portal")
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
		zerolog.Ctx(ctx).Warn().Err(err).
			Str("thread_id", threadID).
			Stringer("source_portal_key", room.portalKey).
			Stringer("target_portal_key", target).
			Msg("Failed to canonicalize Codex thread room")
		return room
	}
	zerolog.Ctx(ctx).Info().
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
	active.applyRoomLocked(c, room)
	active.mu.Unlock()
}

func (r *activeRun) applyRoomLocked(connector *Connector, room threadRoom) {
	r.portalKey = room.portalKey
	if room.login != nil {
		if r.client == nil {
			r.client = &Client{}
		}
		r.client.Main = connector
		r.client.UserLogin = room.login
		r.client.loggedIn = true
	}
	if room.model != "" {
		r.setModelLocked(room.model)
	}
}

func (c *Connector) handleThreadNoticeNotification(method, threadID string, params json.RawMessage) {
	if text := threadNoticeText(method, params); text != "" {
		c.queueThreadNotice(threadID, text)
	}
}

func (c *Connector) queueThreadNotice(threadID, text string) {
	if c == nil || firstTrimmedNonEmpty(text) == "" {
		return
	}
	ctx := context.Background()
	room, ok := c.lookupThreadRoom(ctx, threadID)
	if !ok {
		return
	}
	portal, err := c.Bridge.GetExistingPortalByKey(ctx, room.portalKey)
	if err != nil {
		zerolog.Ctx(ctx).Err(err).Str("thread_id", threadID).Msg("Failed to load portal for Codex notice")
		return
	}
	if portal == nil {
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

type codexRoomState struct {
	EventType string
	Content   map[string]any
	LogName   string
}

func codexThreadRoomStates(state map[string]any) []codexRoomState {
	if state == nil {
		state = map[string]any{}
	}
	modelState := codexAIModelStateContent(state)
	if modelState == nil {
		modelState = map[string]any{}
	}
	return []codexRoomState{
		{EventType: codexThreadStateType, Content: state, LogName: "Codex thread state"},
		{EventType: beeperAIModelStateType, Content: modelState, LogName: "Beeper AI model state"},
	}
}

func codexRoomStateUpdater(state codexRoomState) bridgev2.ExtraUpdater[*bridgev2.Portal] {
	return func(ctx context.Context, portal *bridgev2.Portal) bool {
		if portal == nil || portal.MXID == "" {
			return false
		}
		_, err := portal.Internal().SendStateWithIntentOrBot(ctx, nil, roomStateEventType(state.EventType), "", &event.Content{Raw: state.Content}, time.Now())
		if err != nil {
			zerolog.Ctx(ctx).Err(err).Msg("Failed to sync " + state.LogName)
			return false
		}
		return true
	}
}

func codexThreadMetadataUpdater(cwd, threadID string, state map[string]any) bridgev2.ExtraUpdater[*bridgev2.Portal] {
	updaters := []bridgev2.ExtraUpdater[*bridgev2.Portal]{
		func(_ context.Context, portal *bridgev2.Portal) bool {
			if portal == nil || (cwd == "" && threadID == "") {
				return false
			}
			meta := portalMetadata(portal.Metadata)
			changed := meta.applyProject(cwd, threadID)
			if changed {
				portal.Metadata = meta
			}
			return changed
		},
	}
	updaters = appendCodexRoomStateUpdaters(updaters, state)
	updaters = append(updaters, codexCommandStateUpdater())
	return bridgev2.MergeExtraUpdaters(updaters...)
}

func appendCodexRoomStateUpdaters(updaters []bridgev2.ExtraUpdater[*bridgev2.Portal], state map[string]any) []bridgev2.ExtraUpdater[*bridgev2.Portal] {
	for _, roomState := range codexThreadRoomStates(state) {
		updaters = append(updaters, codexRoomStateUpdater(roomState))
	}
	return updaters
}

func codexThreadChatInfo(cwd, threadID string, state map[string]any) *bridgev2.ChatInfo {
	if stateCwd := firstString(state, "cwd"); stateCwd != "" {
		cwd = stateCwd
	}
	info := &bridgev2.ChatInfo{
		ExcludeChangesFromTimeline: true,
		CanBackfill:                threadID != "",
		Avatar:                     codexAvatar(),
		ExtraUpdates:               codexThreadMetadataUpdater(cwd, threadID, state),
	}
	if name := codexThreadDisplayName(cwd, state); name != "" {
		info.Name = &name
	}
	return info
}

func codexThreadDisplayName(cwd string, state map[string]any) string {
	if name := firstString(state, "name", "threadName", "preview"); name != "" {
		return name
	}
	if cwd != "" {
		return directoryName(cwd)
	}
	return ""
}

func codexThreadState(method, threadID, cwd string, params json.RawMessage) map[string]any {
	state := rawPayload(params)
	state["threadId"] = threadID
	state["lastNotification"] = method
	applyThreadPayloadState(state)
	normalizeThreadState(method, state)
	setMissingStateString(state, "cwd", cwd, "cwd")
	applyThreadLifecycleState(method, state)
	return state
}

func applyThreadPayloadState(state map[string]any) {
	thread, _ := state["thread"].(map[string]any)
	if thread == nil {
		return
	}
	delete(thread, "turns")
	copyMapFields(state, thread, "", threadPayloadStateFields()...)
	setNonEmptyMapString(state, "threadId", firstString(thread, "id"))
}

func threadPayloadStateFields() []string {
	return []string{
		"sessionId", "forkedFromId", "parentThreadId", "cwd", "path", "name", "preview", "status",
		"model", "modelName", "modelProvider", "serviceTier", "effort", "reasoningEffort",
		"reasoning_mode", "reasoningMode", "summary", "createdAt", "updatedAt", "ephemeral",
		"cliVersion", "source", "threadSource", "agentNickname", "agentRole", "gitInfo",
	}
}

func applyThreadLifecycleState(method string, state map[string]any) {
	switch method {
	case "thread/archived":
		state["archived"] = true
	case "thread/unarchived":
		state["archived"] = false
	case "thread/closed":
		state["closed"] = true
	}
}

func codexThreadInitialState(thread appserver.Thread) map[string]any {
	thread = hydrateThreadFromSessionFile(thread)
	state := copyStateMap(thread.Raw)
	delete(state, "turns")
	setInitialThreadStateFields(state, thread)
	normalizeThreadState("thread/started", state)
	return state
}

func setInitialThreadStateFields(state map[string]any, thread appserver.Thread) {
	state["threadId"] = thread.ID
	state["lastNotification"] = "thread/started"
	setNonEmptyMapStrings(state,
		mapStringField{"sessionId", thread.SessionID},
		mapStringField{"cwd", thread.Cwd},
		mapStringField{"path", thread.Path},
		mapStringField{"name", thread.Name},
		mapStringField{"preview", thread.Preview},
		mapStringField{"modelProvider", thread.ModelProvider},
	)
	setNonZeroMapInt64(state, "createdAt", thread.CreatedAt)
	setNonZeroMapInt64(state, "updatedAt", thread.UpdatedAt)
}

func hydrateThreadFromSessionFile(thread appserver.Thread) appserver.Thread {
	if thread.Path == "" {
		return thread
	}
	file, err := os.Open(thread.Path)
	if err != nil {
		return thread
	}
	defer file.Close()
	raw := copyStateMap(thread.Raw)
	reader := bufio.NewReader(file)
	for {
		line, err := reader.ReadString('\n')
		mergeThreadContextPayload(raw, line)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return thread
		}
	}
	if len(raw) == 0 {
		return thread
	}
	thread.Raw = raw
	if thread.Cwd == "" {
		thread.Cwd = firstString(raw, "cwd")
	}
	if thread.ModelProvider == "" {
		setNonEmptyStringField(&thread.ModelProvider, modelProviderFromModelRef(firstString(raw, "model")))
	}
	return thread
}

func mergeThreadContextPayload(raw map[string]any, line string) {
	if !strings.Contains(line, `"type":"turn_context"`) {
		return
	}
	var record struct {
		Type    string         `json:"type"`
		Payload map[string]any `json:"payload"`
	}
	if json.Unmarshal([]byte(line), &record) != nil || record.Type != "turn_context" {
		return
	}
	copyMapFields(raw, record.Payload, "",
		"cwd", "model", "modelName", "effort", "reasoning_mode", "reasoningMode",
	)
	if value, ok := record.Payload["reasoning_effort"]; ok {
		raw["reasoningEffort"] = value
	}
}

func normalizeThreadState(method string, state map[string]any) {
	if state == nil {
		return
	}
	setNonEmptyMapString(state, "name", firstString(state, "threadName"))
	normalizeThreadStatusState(state)
	normalizeThreadGoalState(method, state)
	normalizeThreadTokenUsageState(state)
	normalizeThreadSettingsState(state)
	if method == "model/rerouted" {
		setNonEmptyMapString(state, "model", firstString(state, "toModel"))
	}
}

func normalizeThreadStatusState(state map[string]any) {
	if status, _ := state["status"].(map[string]any); status != nil {
		setNonEmptyMapString(state, "statusType", firstString(status, "type"))
		if flags, ok := status["activeFlags"]; ok {
			state["activeFlags"] = flags
		}
	}
}

func normalizeThreadGoalState(method string, state map[string]any) {
	if goal, _ := state["goal"].(map[string]any); goal != nil {
		normalizeThreadGoalFields(state, goal)
	}
	if method == "thread/goal/cleared" {
		clearThreadGoalState(state)
	}
}

func normalizeThreadGoalFields(state, goal map[string]any) {
	setNonEmptyMapString(state, "goalStatus", firstString(goal, "status"))
	setNonEmptyMapString(state, "goalObjective", firstString(goal, "objective"))
	state["goalTokensUsed"] = goal["tokensUsed"]
	state["goalTokenBudget"] = goal["tokenBudget"]
	state["goalUpdatedAt"] = goal["updatedAt"]
}

func clearThreadGoalState(state map[string]any) {
	state["goal"] = nil
	state["goalStatus"] = "cleared"
	deleteMapFields(state, "goalObjective", "goalTokensUsed", "goalTokenBudget", "goalUpdatedAt")
}

func normalizeThreadTokenUsageState(state map[string]any) {
	if tokenUsage, _ := state["tokenUsage"].(map[string]any); tokenUsage != nil {
		normalizeThreadTokenUsageFields(state, tokenUsage)
	}
}

func normalizeThreadTokenUsageFields(state, tokenUsage map[string]any) {
	state["tokenUsageTotal"] = tokenUsage["total"]
	state["tokenUsageLast"] = tokenUsage["last"]
	state["modelContextWindow"] = tokenUsage["modelContextWindow"]
}

func normalizeThreadSettingsState(state map[string]any) {
	if settings, _ := state["threadSettings"].(map[string]any); settings != nil {
		copyMapFields(state, settings, "settings.", threadSettingsStateFields()...)
	}
}

func threadSettingsStateFields() []string {
	return []string{
		"cwd", "approvalPolicy", "approvalsReviewer", "sandboxPolicy", "activePermissionProfile",
		"model", "modelName", "modelProvider", "serviceTier", "effort", "reasoning_mode",
		"reasoningMode", "summary", "collaborationMode", "personality",
	}
}

func copyStateMap(src map[string]any) map[string]any {
	if src == nil {
		return map[string]any{}
	}
	return maps.Clone(src)
}

func deleteMapFields(values map[string]any, keys ...string) {
	for _, key := range keys {
		delete(values, key)
	}
}

func copyMapFields(dst, src map[string]any, mirrorPrefix string, keys ...string) {
	for _, key := range keys {
		value, ok := src[key]
		if !ok {
			continue
		}
		setMapFieldWithMirror(dst, key, value, mirrorPrefix)
	}
}

func setMapFieldWithMirror(dst map[string]any, key string, value any, mirrorPrefix string) {
	dst[key] = value
	if mirrorPrefix != "" {
		dst[mirrorPrefix+key] = value
	}
}

func setNonEmptyMapString(dst map[string]any, key, value string) {
	setNonEmptyMapStringAliases(dst, value, key)
}

func setNonEmptyMapStringAliases(dst map[string]any, value string, keys ...string) {
	if value == "" {
		return
	}
	for _, key := range keys {
		dst[key] = value
	}
}

type mapStringField struct {
	key   string
	value string
}

func setNonEmptyMapStrings(dst map[string]any, fields ...mapStringField) {
	for _, field := range fields {
		setNonEmptyMapString(dst, field.key, field.value)
	}
}

func setStateString(dst *string, state map[string]any, keys ...string) {
	setNonEmptyStringField(dst, firstString(state, keys...))
}

func setNonZeroMapInt64(dst map[string]any, key string, value int64) {
	if value != 0 {
		dst[key] = value
	}
}

func setMissingStateString(state map[string]any, outputKey, value string, stateKeys ...string) {
	if !missingStateString(state, value, stateKeys...) {
		return
	}
	state[outputKey] = value
}

func missingStateString(state map[string]any, value string, stateKeys ...string) bool {
	return value != "" && firstString(state, stateKeys...) == ""
}

type missingStateField struct {
	outputKey string
	value     string
	stateKeys []string
}

func missingStateStringField(outputKey, value string, stateKeys ...string) missingStateField {
	return missingStateField{outputKey: outputKey, value: value, stateKeys: stateKeys}
}

func setMissingStateStrings(state map[string]any, fields ...missingStateField) {
	for _, field := range fields {
		setMissingStateString(state, field.outputKey, field.value, field.stateKeys...)
	}
}

func setMissingLazyStateStrings(state map[string]any, ensure func() map[string]any, fields ...missingStateField) {
	for _, field := range fields {
		if missingStateString(state, field.value, field.stateKeys...) {
			ensure()[field.outputKey] = field.value
		}
	}
}

func codexAIModelStateContent(state map[string]any) map[string]any {
	model := codexModelStateRef(state, "")
	if model == "" {
		return nil
	}
	content := map[string]any{"model": model}
	applyAIModelStateOptionalFields(content, state)
	return content
}

func applyAIModelStateOptionalFields(content, state map[string]any) {
	setNonEmptyMapString(content, "reasoning", roomReasoningEffort(state))
	setNonEmptyMapString(content, "reasoning_mode", roomReasoningMode(state))
	setAIModelDisplayName(content, state)
}

func setAIModelDisplayName(content, state map[string]any) {
	modelName := firstString(state, "model", "toModel")
	if name := firstString(state, "modelName"); name != "" && name != modelName {
		content["name"] = name
	}
}

func enrichThreadStateWithModelState(state, modelState map[string]any) map[string]any {
	if len(modelState) == 0 {
		return state
	}
	var enriched map[string]any
	ensureEnriched := func() map[string]any {
		if enriched != nil {
			return enriched
		}
		enriched = copyStateMap(state)
		return enriched
	}
	setMissingLazyStateStrings(state, ensureEnriched,
		missingStateStringField("model", firstString(modelState, "model"), "model", "toModel"),
		missingStateStringField("modelProvider", firstString(modelState, "modelProvider", "provider"), "modelProvider", "provider"),
		missingStateStringField("reasoningEffort", roomReasoningEffort(modelState), "effort", "reasoning", "reasoningEffort", "reasoning_effort"),
		missingStateStringField("modelName", firstString(modelState, "modelName", "name"), "modelName", "name"),
		missingStateStringField("reasoning_mode", roomReasoningMode(modelState), "reasoning_mode", "reasoningMode"),
	)
	if enriched == nil {
		return state
	}
	return enriched
}

func codexModelStateRef(state map[string]any, fallbackProvider string) string {
	model := firstString(state, "model", "toModel")
	if model == "" {
		return ""
	}
	provider := firstNonEmptyString(firstString(state, "modelProvider", "provider"), firstTrimmedNonEmpty(fallbackProvider))
	if provider != "" && !strings.Contains(model, "/") {
		return provider + "/" + model
	}
	return model
}

func modelProviderRef(provider string) string {
	provider = firstTrimmedNonEmpty(provider)
	if !strings.Contains(provider, "/") {
		return ""
	}
	return provider
}

func modelProviderFromModelRef(model string) string {
	provider, _, ok := strings.Cut(model, "/")
	if !ok {
		return ""
	}
	return provider
}

func firstString(values map[string]any, keys ...string) string {
	for _, key := range keys {
		value, _ := values[key].(string)
		if value := firstTrimmedNonEmpty(value); value != "" {
			return value
		}
	}
	return ""
}

func firstTrimmedNonEmpty(values ...string) string {
	for _, value := range values {
		if value := strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func prefixedLabel(prefix, value string) string {
	if value != "" {
		return prefix + " " + value
	}
	return prefix
}

func colonLabel(label, value string) string {
	return label + ": " + value
}

func lowerTrimmed(value string) string {
	return strings.ToLower(firstTrimmedNonEmpty(value))
}

func lowerContains(value, query string) bool {
	return strings.Contains(strings.ToLower(value), query)
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
		return threadStatusNoticeText(payload)
	case "thread/archived", "thread/unarchived", "thread/closed":
		return threadLifecycleNoticeText(method)
	case "model/rerouted":
		return modelReroutedNoticeText(payload)
	case "model/verification":
		return modelVerificationNoticeText(payload)
	case "thread/realtime/error", "thread/realtime/closed":
		return realtimeNoticeText(method, payload)
	case "warning", "guardianWarning", "configWarning", "deprecationNotice":
		return warningNoticeText(method, payload)
	case "error":
		return errorNoticeText(payload)
	default:
		return ""
	}
}

func realtimeNoticeText(method string, payload map[string]any) string {
	switch method {
	case "thread/realtime/error":
		return noticeWithDetail("Codex realtime error", firstString(payload, "message"))
	case "thread/realtime/closed":
		return noticeWithDetail("Codex realtime closed", firstString(payload, "reason"))
	default:
		return ""
	}
}

func warningNoticeText(method string, payload map[string]any) string {
	return noticeWithDetail("Codex "+method, warningNoticeDetail(payload))
}

func threadLifecycleNoticeText(method string) string {
	switch method {
	case "thread/archived":
		return "Codex archived this thread."
	case "thread/unarchived":
		return "Codex unarchived this thread."
	case "thread/closed":
		return "Codex closed this thread."
	default:
		return ""
	}
}

func threadStatusNoticeText(payload map[string]any) string {
	status, _ := payload["status"].(map[string]any)
	statusType := firstNonEmptyString(firstString(status, "type"), firstString(payload, "statusType"))
	if !isCodexErrorStatus(statusType) {
		return ""
	}
	if message := firstNonEmptyString(statusErrorMessage(status), statusErrorMessage(payload)); message != "" {
		return noticeWithDetail("Codex error", message)
	}
	if model := firstString(payload, "model"); model != "" {
		return "Codex entered system error state while using " + model + "."
	}
	return "Codex entered system error state."
}

func isCodexErrorStatus(statusType string) bool {
	switch lowerTrimmed(statusType) {
	case "systemerror", "error", "failed":
		return true
	default:
		return false
	}
}

func modelVerificationNoticeText(payload map[string]any) string {
	verifications, _ := payload["verifications"].([]any)
	for _, verification := range verifications {
		switch verificationName(verification) {
		case "trustedAccessForCyber", "trusted_access_for_cyber":
			return "Codex is running extra safety checks for possible cybersecurity risk. Responses may take longer.\n\nTrusted Access for Cyber: https://chatgpt.com/cyber"
		}
	}
	return "Codex is verifying the selected model."
}

func warningNoticeDetail(payload map[string]any) string {
	text := firstString(payload, "message", "summary", "warning", "text")
	if text == "" {
		return ""
	}
	return textWithOptionalDetail(text, firstString(payload, "details"))
}

func verificationName(verification any) string {
	switch typed := verification.(type) {
	case string:
		return firstTrimmedNonEmpty(typed)
	case map[string]any:
		return firstString(typed, "type", "verification", "name")
	default:
		return ""
	}
}

func modelReroutedNoticeText(payload map[string]any) string {
	from := firstString(payload, "fromModel")
	to := firstString(payload, "toModel")
	if from == "" || to == "" {
		return "Codex switched models."
	}
	text := "Codex switched models from " + from + " to " + to + "."
	return textWithOptionalDetail(text, modelRerouteNoticeDetail(payload))
}

func modelRerouteNoticeDetail(payload map[string]any) string {
	if reason := firstString(payload, "reason"); reason != "" {
		return colonLabel("Reason", reason)
	}
	return ""
}

func errorNoticeText(payload map[string]any) string {
	return noticeWithDetail("Codex error", statusErrorMessage(payload))
}

func noticeWithDetail(title, detail string) string {
	if detail != "" {
		return title + ":\n\n" + detail
	}
	return title + "."
}

func textWithOptionalDetail(text, detail string) string {
	if detail != "" {
		return text + "\n\n" + detail
	}
	return text
}

func sentenceWithOptionalInlineDetail(text, detail string) string {
	if detail != "" {
		return text + ": " + detail
	}
	return text + "."
}

func statusErrorMessage(payload map[string]any) string {
	errPayload, _ := payload["error"].(map[string]any)
	return firstNonEmptyString(firstString(payload, "message", "error"), firstString(errPayload, "message"))
}

func accountName(account *appserver.Account) string {
	if account == nil {
		return "Codex"
	}
	return accountDisplayName(account)
}

func accountDisplayName(account *appserver.Account) string {
	return firstNonEmptyString(account.Email, prefixedLabel("Codex", account.Type))
}

func loginRemoteProfile(displayName string, loginID networkid.UserLoginID) status.RemoteProfile {
	displayName = firstTrimmedNonEmpty(displayName, "Codex")
	profile := status.RemoteProfile{
		Name:     hostedDisplayName(displayName, codexHostname()),
		Username: string(loginID),
		Avatar:   id.ContentURIString(defaultCodexAvatarMXC),
	}
	applyRemoteProfileEmail(&profile, displayName)
	return profile
}

func applyRemoteProfileEmail(profile *status.RemoteProfile, displayName string) {
	if looksLikeEmail(displayName) {
		profile.Email = displayName
	}
}

func hostedDisplayName(displayName, host string) string {
	if shouldAppendHost(displayName, host) {
		return displayName + " on " + host
	}
	return displayName
}

func shouldAppendHost(displayName, host string) bool {
	return host != "" && !strings.Contains(displayName, " on "+host)
}

func looksLikeEmail(value string) bool {
	return strings.Contains(value, "@") && !strings.Contains(value, " ")
}

func codexHostname() string {
	host, err := os.Hostname()
	if err != nil {
		return ""
	}
	return firstTrimmedNonEmpty(host)
}

func (c *Connector) bridgeAlias() string {
	return string(c.loginIDForUser())
}

func directoryName(path string) string {
	path = firstTrimmedNonEmpty(path, "New Project")
	base := filepath.Base(path)
	switch base {
	case ".", "/", "":
		return path
	}
	return base
}

func sortedRecentDirectories(threads []appserver.Thread) []appserver.Thread {
	latest := latestThreadsByDirectory(threads)
	out := make([]appserver.Thread, 0, len(latest))
	for _, thread := range latest {
		out = append(out, thread)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].UpdatedAt > out[j].UpdatedAt })
	return out
}

func latestThreadsByDirectory(threads []appserver.Thread) map[string]appserver.Thread {
	latest := map[string]appserver.Thread{}
	for _, thread := range threads {
		if !isRecentDirectoryThread(thread) {
			continue
		}
		if current, ok := latest[thread.Cwd]; !ok || thread.UpdatedAt > current.UpdatedAt {
			latest[thread.Cwd] = thread
		}
	}
	return latest
}

func isRecentDirectoryThread(thread appserver.Thread) bool {
	return thread.Cwd != "" && !isDetachedThread(thread)
}

func userInfo(name string, isBot bool, identifiers ...string) *bridgev2.UserInfo {
	return &bridgev2.UserInfo{Name: &name, IsBot: &isBot, Identifiers: userIdentifiers(name, identifiers...)}
}

func userIdentifiers(name string, identifiers ...string) []string {
	return uniqueIdentifiers(append([]string{name}, identifiers...)...)
}

func uniqueIdentifiers(values ...string) []string {
	seen := map[string]bool{}
	var out []string
	for _, value := range values {
		appendUniqueIdentifier(&out, seen, value)
	}
	return out
}

func appendUniqueIdentifier(out *[]string, seen map[string]bool, value string) {
	value = firstTrimmedNonEmpty(value)
	if value == "" {
		return
	}
	key := strings.ToLower(value)
	if seen[key] {
		return
	}
	seen[key] = true
	*out = append(*out, value)
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

func projectThreadUserInfo(thread appserver.Thread) *bridgev2.UserInfo {
	return codexUserInfo(thread.Cwd, false, thread.Cwd, thread.ID, thread.Name)
}

func portalInfo(name string, members *bridgev2.ChatMemberList, cwd, threadID string, state map[string]any) *bridgev2.ChatInfo {
	info := codexChatInfo(name, members)
	info.CanBackfill = threadID != ""
	info.ExtraUpdates = codexThreadMetadataUpdater(cwd, threadID, state)
	return info
}

func subagentThreadState(parentThreadID string, ref subagentRef, cwd string, base map[string]any) map[string]any {
	state := copyStateMap(base)
	state["threadId"] = ref.ThreadID
	state["parentThreadId"] = parentThreadID
	state["kind"] = portalKindSubagent
	state["readOnly"] = true
	setNonEmptyMapString(state, "cwd", cwd)
	setNonEmptyMapString(state, "status", ref.Status)
	return state
}

func subagentPortalInfo(members *bridgev2.ChatMemberList, parentThreadID string, ref subagentRef, cwd string, state map[string]any) *bridgev2.ChatInfo {
	name := "Subagent " + ref.ThreadID
	if state == nil {
		state = subagentThreadState(parentThreadID, ref, cwd, nil)
	}
	updaters := []bridgev2.ExtraUpdater[*bridgev2.Portal]{subagentMetadataUpdater(parentThreadID, ref.ThreadID, cwd)}
	updaters = appendCodexRoomStateUpdaters(updaters, state)
	info := codexChatInfo(name, members)
	info.CanBackfill = true
	info.ExtraUpdates = bridgev2.MergeExtraUpdaters(updaters...)
	return info
}

func subagentMetadataUpdater(parentThreadID, threadID, cwd string) bridgev2.ExtraUpdater[*bridgev2.Portal] {
	return func(ctx context.Context, portal *bridgev2.Portal) bool {
		if portal == nil || threadID == "" {
			return false
		}
		meta := portalMetadata(portal.Metadata)
		changed := meta.applySubagent(parentThreadID, threadID, cwd)
		if changed {
			portal.Metadata = meta
		}
		return changed
	}
}

func codexChatInfo(name string, members *bridgev2.ChatMemberList) *bridgev2.ChatInfo {
	roomType := database.RoomTypeDM
	return &bridgev2.ChatInfo{
		Name:                       &name,
		Avatar:                     codexAvatar(),
		Type:                       &roomType,
		Members:                    members,
		ExcludeChangesFromTimeline: true,
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
}

func updatePortalInfo(ctx context.Context, portal *bridgev2.Portal, login *bridgev2.UserLogin, info *bridgev2.ChatInfo) {
	applyStoredPortalInfo(info, portal)
	portal.UpdateInfo(ctx, info, login, nil, time.Now())
}

func (c *Connector) ResolveLogin(ctx context.Context, user *bridgev2.User, requested networkid.UserLoginID) (*bridgev2.UserLogin, error) {
	if requested == "" {
		requested = defaultLoginID
		if user != nil {
			requested = c.loginIDForUser()
		}
	}
	if cached := c.Bridge.GetCachedUserLoginByID(requested); cached != nil && cached.UserMXID == user.MXID {
		return cached, nil
	}
	if requested != c.loginIDForUser() {
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

func (c *Connector) loginIDForUser() networkid.UserLoginID {
	if c == nil || c.Bridge == nil {
		return defaultLoginID
	}
	if mx, ok := c.Bridge.Matrix.(*matrixconnector.Connector); ok && mx.Config != nil {
		botUsername := firstTrimmedNonEmpty(mx.Config.AppService.Bot.Username)
		if alias, ok := strings.CutSuffix(botUsername, "bot"); ok {
			return networkid.UserLoginID(alias)
		}
		template := firstTrimmedNonEmpty(mx.Config.AppService.UsernameTemplate)
		if prefix, _, ok := strings.Cut(template, "_{{"); ok {
			return networkid.UserLoginID(prefix)
		}
	}
	if c.Bridge.ID != "" {
		return networkid.UserLoginID(c.Bridge.ID)
	}
	return defaultLoginID
}
