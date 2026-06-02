package bridge

import (
	"context"
	"fmt"
	"strings"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/status"
	"maunium.net/go/mautrix/id"

	"github.com/beeper/codex-bridge/pkg/appserver"
)

const (
	loginFlowCodex    = "codex"
	loginStepCodex    = "com.beeper.codex.login"
	loginStepComplete = "com.beeper.codex.login.complete"
)

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
		if permissions == nil || !permissions.Login || !strings.HasPrefix(rawUserID, "@") {
			continue
		}
		userID := id.UserID(rawUserID)
		if _, _, err = userID.Parse(); err != nil {
			continue
		}
		user, err := c.Bridge.GetUserByMXID(ctx, userID)
		if err != nil {
			continue
		}
		_, _ = c.ensureLogin(ctx, user, accountName(account))
	}
}

func (c *Connector) ensureLogin(ctx context.Context, user *bridgev2.User, displayName string) (*bridgev2.UserLogin, error) {
	if c == nil || c.Bridge == nil || user == nil {
		return nil, fmt.Errorf("Codex login requires a bridge and user")
	}
	var primary *bridgev2.UserLogin
	for _, loginID := range c.loginIDsForUser(user.MXID) {
		login, err := c.ensureLoginID(ctx, user, loginID, displayName)
		if err != nil {
			return nil, err
		}
		if primary == nil {
			primary = login
		}
	}
	return primary, nil
}

func (c *Connector) ensureLoginID(ctx context.Context, user *bridgev2.User, loginID networkid.UserLoginID, displayName string) (*bridgev2.UserLogin, error) {
	if cached := c.Bridge.GetCachedUserLoginByID(loginID); cached != nil {
		if cached.UserMXID != user.MXID {
			return nil, fmt.Errorf("Codex login %s belongs to %s", loginID, cached.UserMXID)
		}
		cached.RemoteName = displayName
		cached.RemoteProfile.Name = displayName
		if err := cached.Save(ctx); err != nil {
			return nil, err
		}
		if cached.Client != nil {
			cached.Client.Connect(ctx)
		}
		return cached, nil
	}
	login, err := user.NewLogin(ctx, &database.UserLogin{
		ID:         loginID,
		RemoteName: displayName,
		RemoteProfile: status.RemoteProfile{
			Name: displayName,
		},
	}, &bridgev2.NewLoginParams{})
	if err != nil {
		return nil, err
	}
	if login.Client != nil {
		login.Client.Connect(ctx)
	}
	return login, nil
}

func (c *Connector) defaultLoginIDForUser(user *bridgev2.User) networkid.UserLoginID {
	if user == nil {
		return defaultLoginID
	}
	return c.loginIDForUser(user.MXID)
}
