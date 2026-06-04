package bridge

import (
	"crypto/rand"
	"encoding/base64"
	"strings"

	"maunium.net/go/mautrix/bridgev2/networkid"
)

const (
	networkID        = "codex"
	beeperBridgeType = "codex"
	defaultLoginID   = networkid.UserLoginID("codex")
	codexUserID      = networkid.UserID("codex")
	newProjectUserID = networkid.UserID("new-project")

	projectIDPrefix        = "project:"
	subagentPortalIDPrefix = "subagent:"
	newPortalIDPrefix      = "new:"
	defaultNewPortalID     = networkid.PortalID(newPortalIDPrefix + "starter")

	codexApprovalPolicyOnRequest = "on-request"
)

var idSanitizer = strings.NewReplacer(" ", "_", "/", "_", "\\", "_", ":", "_", "\n", "_", "\r", "_", "\t", "_")

func subagentPortalKey(threadID string, loginID networkid.UserLoginID) networkid.PortalKey {
	return portalKey(subagentPortalIDPrefix+sanitizeID(threadID), loginID)
}

func projectPortalKey(cwd string, loginID networkid.UserLoginID) networkid.PortalKey {
	return portalKey(projectID(cwd), loginID)
}

func newProjectPortalKey(loginID networkid.UserLoginID) networkid.PortalKey {
	return portalKey(newProjectPortalID(), loginID)
}

func defaultNewProjectPortalKey(loginID networkid.UserLoginID) networkid.PortalKey {
	return portalKey(string(defaultNewPortalID), loginID)
}

func newProjectPortalID() string {
	var data [16]byte
	if _, err := rand.Read(data[:]); err != nil {
		return newPortalIDPrefix + sanitizeID(err.Error())
	}
	return prefixedRawURLEncoded(newPortalIDPrefix, data[:])
}

func portalKey(id string, loginID networkid.UserLoginID) networkid.PortalKey {
	return networkid.PortalKey{ID: networkid.PortalID(id), Receiver: loginID}
}

func projectUserID(cwd string) networkid.UserID {
	return networkid.UserID(projectID(cwd))
}

func projectID(cwd string) string {
	return prefixedRawURLEncoded(projectIDPrefix, []byte(firstTrimmedNonEmpty(cwd)))
}

func parseProjectUserID(userID networkid.UserID) (string, bool) {
	return parseProjectID(string(userID))
}

func parseProjectPortalID(portalID networkid.PortalID) (string, bool) {
	return parseProjectID(string(portalID))
}

func parseProjectID(value string) (string, bool) {
	encoded, ok := projectIDPayload(value)
	if !ok {
		return "", false
	}
	raw, err := rawURLDecoded(encoded)
	if err != nil {
		return "", false
	}
	return string(raw), true
}

func projectIDPayload(value string) (string, bool) {
	encoded, ok := strings.CutPrefix(value, projectIDPrefix)
	return encoded, ok && encoded != ""
}

func partID(name string) networkid.PartID {
	return networkid.PartID(sanitizeID(name))
}

func sanitizeID(value string) string {
	value = firstTrimmedNonEmpty(value, "unknown")
	return idSanitizer.Replace(value)
}

func rawURLEncoded(data []byte) string {
	return base64.RawURLEncoding.EncodeToString(data)
}

func rawURLDecoded(value string) ([]byte, error) {
	return base64.RawURLEncoding.DecodeString(value)
}

func prefixedRawURLEncoded(prefix string, data []byte) string {
	return prefix + rawURLEncoded(data)
}
