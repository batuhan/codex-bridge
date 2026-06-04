package bridge

import (
	"context"
	"errors"
	"net"
	"strings"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"
)

var (
	codexTimeoutErrorHints = []string{
		"timeout",
		"timed out",
		"context deadline exceeded",
	}
	codexBridgeUnavailableErrorHints = []string{
		"codex app-server is not running",
		"failed to start codex",
		"missing codex stream bridge context",
	}
	codexPermissionErrorHints = []string{
		"permission denied",
		"not permitted",
		"approval denied",
		"unauthorized",
		"forbidden",
	}
	codexUnsupportedErrorHints = []string{
		"unsupported",
		"not supported",
		"invalid project directory",
		"not a directory",
	}
)

func matrixMessageStatusForCodexError(err error) bridgev2.MessageStatus {
	status := bridgev2.WrapErrorInStatus(err)
	if status.InternalError == nil {
		status.InternalError = err
	}
	if keep, fillMessage := wrappedMatrixStatusHandling(&status); keep {
		if fillMessage {
			status.Message = firstNonEmptyString(status.Message, err.Error())
		}
		return status
	}

	setRetriableMatrixFailure(&status)
	applyCodexMatrixStatusOverride(&status, err)
	return status
}

func applyCodexMatrixStatusOverride(status *bridgev2.MessageStatus, err error) {
	lower := strings.ToLower(err.Error())
	switch {
	case isCodexCanceledError(err):
		setCertainMatrixFailure(status, event.MessageStatusBridgeUnavailable, "Codex request was cancelled")
	case isCodexTimeoutError(err, lower):
		setTimeoutMatrixFailure(status)
	case isCodexBridgeUnavailableError(lower):
		setCertainMatrixFailure(status, event.MessageStatusBridgeUnavailable, "Codex bridge is not ready")
	case isCodexPermissionError(lower):
		setCertainMatrixFailure(status, event.MessageStatusNoPermission, err.Error())
	case isCodexUnsupportedError(lower):
		setCertainMatrixFailure(status, event.MessageStatusUnsupported, err.Error())
	}
}

func isCodexCanceledError(err error) bool {
	return errors.Is(err, context.Canceled)
}

func setTimeoutMatrixFailure(status *bridgev2.MessageStatus) {
	status.ErrorReason = event.MessageStatusNetworkError
	status.Message = "Codex request timed out"
}

func wrappedMatrixStatusHandling(status *bridgev2.MessageStatus) (keep, fillMessage bool) {
	if status == nil || status.Status == "" {
		return false, false
	}
	if status.IsCertain && !status.SendNotice {
		return true, true
	}
	return status.ErrorReason != event.MessageStatusGenericError, false
}

func isCodexTimeoutError(err error, lower string) bool {
	var netErr net.Error
	return errors.Is(err, context.DeadlineExceeded) ||
		errors.As(err, &netErr) && netErr.Timeout() ||
		containsAny(lower, codexTimeoutErrorHints...)
}

func isCodexBridgeUnavailableError(lower string) bool {
	return containsAny(lower, codexBridgeUnavailableErrorHints...)
}

func isCodexPermissionError(lower string) bool {
	return containsAny(lower, codexPermissionErrorHints...)
}

func isCodexUnsupportedError(lower string) bool {
	return containsAny(lower, codexUnsupportedErrorHints...)
}

func setCertainMatrixFailure(status *bridgev2.MessageStatus, reason event.MessageStatusReason, message string) {
	status.Status = event.MessageStatusFail
	status.ErrorReason = reason
	status.Message = message
	status.IsCertain = true
}

func setRetriableMatrixFailure(status *bridgev2.MessageStatus) {
	status.Status = event.MessageStatusRetriable
	status.ErrorReason = event.MessageStatusGenericError
	status.Message = "Codex failed to respond"
	status.SendNotice = true
}

func containsAny(value string, needles ...string) bool {
	for _, needle := range needles {
		if strings.Contains(value, needle) {
			return true
		}
	}
	return false
}

func (cl *Client) logMatrixMessageError(msg *bridgev2.MatrixMessage, err error, message string) {
	if cl == nil || cl.Main == nil || cl.Main.Bridge == nil {
		return
	}
	log := cl.Main.Bridge.Log.Error().
		Err(err).
		Str("action", "codex_matrix_message")
	if msg != nil && msg.Portal != nil {
		log = log.
			Str("portal_id", string(msg.Portal.ID)).
			Str("portal_receiver", string(msg.Portal.Receiver)).
			Str("portal_mxid", string(msg.Portal.MXID))
	}
	if msg != nil && msg.Event != nil {
		log = log.
			Str("event_id", string(msg.Event.ID)).
			Str("event_type", msg.Event.Type.Type).
			Str("sender", string(msg.Event.Sender))
	}
	if cl.UserLogin != nil {
		log = log.Str("login_id", string(cl.UserLogin.ID))
	}
	log.Msg(message)
}
