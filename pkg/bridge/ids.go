package bridge

import (
	"crypto/rand"
	"encoding/base64"
	"strings"

	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/id"
)

const (
	networkID        = "codex"
	beeperBridgeType = "codex"
	defaultLoginID   = networkid.UserLoginID("codex")
	codexUserID      = networkid.UserID("codex")
	newProjectUserID = networkid.UserID("new-project")
)

func threadPortalKey(threadID string, loginID networkid.UserLoginID) networkid.PortalKey {
	return networkid.PortalKey{ID: networkid.PortalID("thread:" + sanitizeID(threadID)), Receiver: loginID}
}

func projectPortalKey(cwd string, loginID networkid.UserLoginID) networkid.PortalKey {
	return networkid.PortalKey{ID: networkid.PortalID("project:" + encode(strings.TrimSpace(cwd))), Receiver: loginID}
}

func newProjectPortalKey(loginID networkid.UserLoginID) networkid.PortalKey {
	var data [16]byte
	if _, err := rand.Read(data[:]); err != nil {
		return networkid.PortalKey{ID: networkid.PortalID("new:" + sanitizeID(err.Error())), Receiver: loginID}
	}
	return networkid.PortalKey{ID: networkid.PortalID("new:" + base64.RawURLEncoding.EncodeToString(data[:])), Receiver: loginID}
}

func isNewProjectPortalID(portalID networkid.PortalID) bool {
	return strings.HasPrefix(string(portalID), "new:")
}

func projectUserID(cwd string) networkid.UserID {
	return networkid.UserID("project:" + encode(strings.TrimSpace(cwd)))
}

func parseProjectUserID(userID networkid.UserID) (string, bool) {
	encoded, ok := strings.CutPrefix(string(userID), "project:")
	if !ok || encoded == "" {
		return "", false
	}
	raw, err := base64.RawURLEncoding.DecodeString(encoded)
	return string(raw), err == nil
}

func parseProjectPortalID(portalID networkid.PortalID) (string, bool) {
	encoded, ok := strings.CutPrefix(string(portalID), "project:")
	if !ok || encoded == "" {
		return "", false
	}
	raw, err := base64.RawURLEncoding.DecodeString(encoded)
	return string(raw), err == nil
}

func partID(name string) networkid.PartID {
	return networkid.PartID(sanitizeID(name))
}

func matrixSafeID(value string) string {
	return id.EncodeUserLocalpart(value)
}

func encode(value string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(value))
}

func sanitizeID(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "unknown"
	}
	return strings.NewReplacer(" ", "_", "/", "_", "\\", "_", ":", "_", "\n", "_", "\r", "_", "\t", "_").Replace(value)
}
