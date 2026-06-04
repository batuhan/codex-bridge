package bridge

import (
	"context"
	"fmt"
	"strings"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/id"

	"github.com/beeper/codex-bridge/pkg/appserver"
)

const (
	loginFlowCodex    = "codex"
	loginStepComplete = "com.beeper.codex.login.complete"
)

func loginUserID(loginID networkid.UserLoginID) networkid.UserID {
	return networkid.UserID("login:" + string(loginID))
}

func (c *Connector) GetLoginFlows() []bridgev2.LoginFlow {
	return []bridgev2.LoginFlow{{
		Name:        "Codex",
		Description: "Use the local Codex app login",
		ID:          loginFlowCodex,
	}}
}

func (c *Connector) CreateLogin(ctx context.Context, user *bridgev2.User, flowID string) (bridgev2.LoginProcess, error) {
	if flowID != loginFlowCodex {
		return nil, fmt.Errorf("invalid login flow ID")
	}
	return &CodexLogin{Main: c, User: user}, nil
}

type CodexLogin struct {
	Main *Connector
	User *bridgev2.User
}

var _ bridgev2.LoginProcess = (*CodexLogin)(nil)

func (l *CodexLogin) Start(ctx context.Context) (*bridgev2.LoginStep, error) {
	account, err := l.Main.readAccount(ctx)
	if err != nil {
		return nil, err
	}
	if account == nil {
		return nil, fmt.Errorf("Codex is not logged in locally; run `codex login` first")
	}
	login, err := l.Main.ensureLogin(ctx, l.User, accountName(account))
	if err != nil {
		return nil, err
	}
	return &bridgev2.LoginStep{
		Type:         bridgev2.LoginStepTypeComplete,
		StepID:       loginStepComplete,
		Instructions: "Codex login ready",
		CompleteParams: &bridgev2.LoginCompleteParams{
			UserLoginID: login.ID,
			UserLogin:   login,
		},
	}, nil
}

func (l *CodexLogin) Cancel() {}

func (c *Connector) readAccount(ctx context.Context) (*appserver.Account, error) {
	var resp appserver.AccountReadResponse
	if err := c.request(ctx, "account/read", map[string]any{}, &resp); err != nil {
		return nil, err
	}
	return resp.Account, nil
}

func (c *Connector) seedConfiguredLogins(ctx context.Context) {
	if c == nil || c.Bridge == nil || c.Bridge.Config == nil {
		return
	}
	account, err := c.readAccount(ctx)
	if err != nil || account == nil {
		return
	}
	for rawUserID, permissions := range c.Bridge.Config.Permissions {
		if permissions == nil || !permissions.Login {
			continue
		}
		userID, ok := configuredLoginUserID(rawUserID)
		if !ok {
			continue
		}
		user, err := c.Bridge.GetUserByMXID(ctx, userID)
		if err != nil {
			continue
		}
		_, _ = c.ensureLogin(ctx, user, accountName(account))
	}
}

func configuredLoginUserID(raw string) (id.UserID, bool) {
	if !isConfiguredLoginUserID(raw) {
		return "", false
	}
	userID := id.UserID(raw)
	_, _, err := userID.Parse()
	return userID, err == nil
}

func isConfiguredLoginUserID(raw string) bool {
	return strings.HasPrefix(raw, "@")
}

func (c *Connector) ensureLogin(ctx context.Context, user *bridgev2.User, displayName string) (*bridgev2.UserLogin, error) {
	if c == nil || c.Bridge == nil || user == nil {
		return nil, fmt.Errorf("Codex login requires a bridge and user")
	}
	return c.ensureLoginID(ctx, user, c.loginIDForUser(), displayName)
}

func (c *Connector) ensureLoginID(ctx context.Context, user *bridgev2.User, loginID networkid.UserLoginID, displayName string) (*bridgev2.UserLogin, error) {
	if cached := c.Bridge.GetCachedUserLoginByID(loginID); cached != nil {
		return c.refreshCachedLogin(ctx, user, cached, loginID, displayName)
	}
	login, err := user.NewLogin(ctx, codexUserLoginRecord(loginID, displayName), &bridgev2.NewLoginParams{})
	if err != nil {
		return nil, err
	}
	return c.finishLogin(ctx, login, loginID), nil
}

func (c *Connector) refreshCachedLogin(ctx context.Context, user *bridgev2.User, login *bridgev2.UserLogin, loginID networkid.UserLoginID, displayName string) (*bridgev2.UserLogin, error) {
	if login.UserMXID != user.MXID {
		return nil, fmt.Errorf("Codex login %s belongs to %s", loginID, login.UserMXID)
	}
	updateLoginRemoteProfile(login, displayName, loginID)
	if err := login.Save(ctx); err != nil {
		return nil, err
	}
	return c.finishLogin(ctx, login, loginID), nil
}

func codexUserLoginRecord(loginID networkid.UserLoginID, displayName string) *database.UserLogin {
	remoteProfile := loginRemoteProfile(displayName, loginID)
	return &database.UserLogin{
		ID:            loginID,
		RemoteName:    remoteProfile.Name,
		RemoteProfile: remoteProfile,
	}
}

func updateLoginRemoteProfile(login *bridgev2.UserLogin, displayName string, loginID networkid.UserLoginID) {
	remoteProfile := loginRemoteProfile(displayName, loginID)
	login.RemoteName = remoteProfile.Name
	login.RemoteProfile = remoteProfile
}

func (c *Connector) finishLogin(ctx context.Context, login *bridgev2.UserLogin, loginID networkid.UserLoginID) *bridgev2.UserLogin {
	if login.Client != nil {
		login.Client.Connect(ctx)
	}
	c.syncLoginGhost(ctx, loginID)
	return login
}

func (c *Connector) syncLoginGhost(ctx context.Context, loginID networkid.UserLoginID) {
	if c == nil || c.Bridge == nil || loginID == "" {
		return
	}
	userID := loginUserID(loginID)
	ghost, err := c.Bridge.GetGhostByID(ctx, userID)
	if err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).Str("user_id", string(userID)).Msg("Failed to load Codex login ghost")
		return
	}
	ghost.UpdateInfo(ctx, loginUserInfo())
}
