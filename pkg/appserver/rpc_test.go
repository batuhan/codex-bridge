package appserver

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"
)

func TestParseVersion(t *testing.T) {
	if got := ParseVersion("codex-cli 0.133.0"); got != "0.133.0" {
		t.Fatalf("unexpected version %q", got)
	}
}

func TestCompareVersion(t *testing.T) {
	if CompareVersion("0.133.0", "0.132.9") <= 0 {
		t.Fatal("expected 0.133.0 to be newer")
	}
	if CompareVersion("0.133.0", "0.133.0") != 0 {
		t.Fatal("expected equal versions")
	}
}

func TestRequestReturnsRPCError(t *testing.T) {
	stdinReader, stdinWriter := io.Pipe()
	stdoutReader, stdoutWriter := io.Pipe()
	client := &Client{
		stdin:    stdinWriter,
		stdout:   stdoutReader,
		waiting:  map[string]chan Message{},
		incoming: make(chan Message, 1),
		done:     make(chan struct{}),
	}
	go client.readLoop()
	defer client.Close()

	go func() {
		scanner := bufio.NewScanner(stdinReader)
		if !scanner.Scan() {
			return
		}
		var req Message
		if err := json.Unmarshal(scanner.Bytes(), &req); err != nil {
			return
		}
		_ = json.NewEncoder(stdoutWriter).Encode(Message{
			ID: req.ID,
			Error: &Error{
				Code:    -32042,
				Message: "bad request",
				Data:    json.RawMessage(`{"detail":"kept"}`),
			},
		})
	}()

	err := client.Request(context.Background(), "thing/do", nil, nil)
	var rpcErr *RPCError
	if !errors.As(err, &rpcErr) {
		t.Fatalf("expected RPCError, got %T: %v", err, err)
	}
	if rpcErr.Method != "thing/do" || rpcErr.Code != -32042 || rpcErr.Msg != "bad request" {
		t.Fatalf("unexpected RPC error: %#v", rpcErr)
	}
	if string(rpcErr.Data) != `{"detail":"kept"}` {
		t.Fatalf("unexpected RPC error data: %s", rpcErr.Data)
	}
}

func TestInitializeDeclaresNoAttestation(t *testing.T) {
	stdinReader, stdinWriter := io.Pipe()
	stdoutReader, stdoutWriter := io.Pipe()
	client := &Client{
		stdin:    stdinWriter,
		stdout:   stdoutReader,
		waiting:  map[string]chan Message{},
		incoming: make(chan Message, 1),
		done:     make(chan struct{}),
	}
	go client.readLoop()
	defer client.Close()

	errCh := make(chan error, 1)
	go func() {
		scanner := bufio.NewScanner(stdinReader)
		if !scanner.Scan() {
			errCh <- scanner.Err()
			return
		}
		var req Message
		if err := json.Unmarshal(scanner.Bytes(), &req); err != nil {
			errCh <- err
			return
		}
		var params map[string]any
		if err := json.Unmarshal(req.Params, &params); err != nil {
			errCh <- err
			return
		}
		caps, _ := params["capabilities"].(map[string]any)
		if caps["requestAttestation"] != false {
			errCh <- errors.New("requestAttestation was not false")
			return
		}
		_ = json.NewEncoder(stdoutWriter).Encode(Message{ID: req.ID, Result: json.RawMessage(`{}`)})
		if !scanner.Scan() {
			errCh <- scanner.Err()
			return
		}
		var notify Message
		if err := json.Unmarshal(scanner.Bytes(), &notify); err != nil {
			errCh <- err
			return
		}
		if notify.Method != "initialized" {
			errCh <- errors.New("initialized notification was not sent")
			return
		}
		errCh <- nil
	}()

	if err := Initialize(context.Background(), client); err != nil {
		t.Fatal(err)
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
}
