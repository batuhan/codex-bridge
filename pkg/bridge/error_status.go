package bridge

import (
	"context"
	"errors"
	"net"
	"strings"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"
)

func matrixMessageStatusForCodexError(err error) bridgev2.MessageStatus {
	status := bridgev2.WrapErrorInStatus(err)
	if status.InternalError == nil {
		status.InternalError = err
	}
	if status.Status != "" && status.ErrorReason != event.MessageStatusGenericError {
		return status
	}

	status.Status = event.MessageStatusRetriable
	status.ErrorReason = event.MessageStatusGenericError
	status.Message = "Codex failed to respond"
	status.SendNotice = true

	lower := strings.ToLower(err.Error())
	switch {
	case errors.Is(err, context.Canceled):
		status.Status = event.MessageStatusFail
		status.ErrorReason = event.MessageStatusBridgeUnavailable
		status.Message = "Codex request was cancelled"
		status.IsCertain = true
	case errors.Is(err, context.DeadlineExceeded), isCodexTimeout(err, lower):
		status.ErrorReason = event.MessageStatusNetworkError
		status.Message = "Codex request timed out"
	case strings.Contains(lower, "codex app-server is not running") ||
		strings.Contains(lower, "failed to start codex") ||
		strings.Contains(lower, "missing codex stream bridge context"):
		status.Status = event.MessageStatusFail
		status.ErrorReason = event.MessageStatusBridgeUnavailable
		status.Message = "Codex bridge is not ready"
		status.IsCertain = true
	case strings.Contains(lower, "permission denied") ||
		strings.Contains(lower, "not permitted") ||
		strings.Contains(lower, "approval denied") ||
		strings.Contains(lower, "unauthorized") ||
		strings.Contains(lower, "forbidden"):
		status.Status = event.MessageStatusFail
		status.ErrorReason = event.MessageStatusNoPermission
		status.Message = err.Error()
		status.IsCertain = true
	case strings.Contains(lower, "unsupported") ||
		strings.Contains(lower, "not supported") ||
		strings.Contains(lower, "invalid project directory") ||
		strings.Contains(lower, "not a directory"):
		status.Status = event.MessageStatusFail
		status.ErrorReason = event.MessageStatusUnsupported
		status.Message = err.Error()
		status.IsCertain = true
	}
	return status
}

func isCodexTimeout(err error, lower string) bool {
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout() ||
		strings.Contains(lower, "timeout") ||
		strings.Contains(lower, "timed out") ||
		strings.Contains(lower, "context deadline exceeded")
}

func (cl *Client) logMatrixMessageError(msg *bridgev2.MatrixMessage, err error, message string) {
	if cl == nil || cl.Main == nil || cl.Main.Bridge == nil {
		return
	}
	log := cl.Main.Bridge.Log.Error().
		Err(err).
		Str("action", "codex_matrix_message")
	if msg != nil {
		if msg.Portal != nil {
			log = log.
				Str("portal_id", string(msg.Portal.ID)).
				Str("portal_receiver", string(msg.Portal.Receiver)).
				Str("portal_mxid", string(msg.Portal.MXID))
		}
		if msg.Event != nil {
			log = log.
				Str("event_id", string(msg.Event.ID)).
				Str("event_type", msg.Event.Type.Type).
				Str("sender", string(msg.Event.Sender))
		}
	}
	if cl.UserLogin != nil {
		log = log.Str("login_id", string(cl.UserLogin.ID))
	}
	log.Msg(message)
}
