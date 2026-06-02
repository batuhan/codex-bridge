package appserver

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Message struct {
	ID     any             `json:"id,omitempty"`
	Method string          `json:"method,omitempty"`
	Params json.RawMessage `json:"params,omitempty"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  *Error          `json:"error,omitempty"`
}

type Error struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

type RPCError struct {
	Method string
	Code   int
	Msg    string
	Data   json.RawMessage
}

func (e *RPCError) Error() string {
	if e.Method == "" {
		return e.Msg
	}
	return e.Method + ": " + e.Msg
}

type Client struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout io.ReadCloser

	nextID  atomic.Int64
	writeMu sync.Mutex
	waitMu  sync.Mutex
	waiting map[string]chan Message

	incoming chan Message
	done     chan struct{}
}

func Start(ctx context.Context, command string, env map[string]string) (*Client, error) {
	command = strings.TrimSpace(command)
	if command == "" {
		command = "codex"
	}
	parts := strings.Fields(command)
	if len(parts) == 0 {
		return nil, fmt.Errorf("empty Codex command")
	}
	cmd := exec.CommandContext(ctx, parts[0], append(parts[1:], "app-server", "--listen", "stdio://")...)
	cmd.Env = os.Environ()
	for key, value := range env {
		cmd.Env = append(cmd.Env, key+"="+value)
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	cmd.Stderr = os.Stderr
	if err = cmd.Start(); err != nil {
		return nil, err
	}
	client := &Client{
		cmd:      cmd,
		stdin:    stdin,
		stdout:   stdout,
		waiting:  map[string]chan Message{},
		incoming: make(chan Message, 1024),
		done:     make(chan struct{}),
	}
	go client.readLoop()
	return client, nil
}

func (c *Client) Incoming() <-chan Message {
	return c.incoming
}

func (c *Client) Close() {
	if c == nil {
		return
	}
	_ = c.stdin.Close()
	if c.cmd != nil && c.cmd.Process != nil {
		_ = c.cmd.Process.Kill()
		_, _ = c.cmd.Process.Wait()
	}
}

func (c *Client) Request(ctx context.Context, method string, params any, out any) error {
	id := strconv.FormatInt(c.nextID.Add(1), 10)
	ch := make(chan Message, 1)
	c.waitMu.Lock()
	c.waiting[id] = ch
	c.waitMu.Unlock()
	defer func() {
		c.waitMu.Lock()
		delete(c.waiting, id)
		c.waitMu.Unlock()
	}()
	if err := c.write(ctx, Message{ID: id, Method: method}, params); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return fmt.Errorf("Codex app-server exited")
	case msg := <-ch:
		if msg.Error != nil {
			return &RPCError{
				Method: method,
				Code:   msg.Error.Code,
				Msg:    msg.Error.Message,
				Data:   msg.Error.Data,
			}
		}
		if out == nil {
			return nil
		}
		return json.Unmarshal(msg.Result, out)
	}
}

func (c *Client) Notify(ctx context.Context, method string, params any) error {
	return c.write(ctx, Message{Method: method}, params)
}

func (c *Client) Respond(ctx context.Context, id any, result any) error {
	return c.write(ctx, Message{ID: id}, result)
}

func (c *Client) RespondError(ctx context.Context, id any, code int, message string, data ...any) error {
	errValue := &Error{Code: code, Message: message}
	if len(data) > 0 && data[0] != nil {
		raw, err := json.Marshal(data[0])
		if err != nil {
			return err
		}
		errValue.Data = raw
	}
	return c.writeRaw(ctx, Message{ID: id, Error: errValue})
}

func (c *Client) write(ctx context.Context, msg Message, payload any) error {
	if payload != nil {
		raw, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		if msg.Method == "" {
			msg.Result = raw
		} else {
			msg.Params = raw
		}
	}
	return c.writeRaw(ctx, msg)
}

func (c *Client) writeRaw(ctx context.Context, msg Message) error {
	raw, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	done := make(chan error, 1)
	go func() {
		c.writeMu.Lock()
		defer c.writeMu.Unlock()
		_, err := c.stdin.Write(raw)
		done <- err
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-done:
		return err
	}
}

func (c *Client) readLoop() {
	defer close(c.done)
	defer close(c.incoming)
	scanner := bufio.NewScanner(c.stdout)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		var msg Message
		if err := json.Unmarshal(scanner.Bytes(), &msg); err != nil {
			continue
		}
		if msg.ID != nil && msg.Method == "" {
			id := fmt.Sprint(msg.ID)
			c.waitMu.Lock()
			ch := c.waiting[id]
			c.waitMu.Unlock()
			if ch != nil {
				ch <- msg
			}
			continue
		}
		select {
		case c.incoming <- msg:
		default:
		}
	}
}

func Initialize(ctx context.Context, c *Client) error {
	if err := c.Request(ctx, "initialize", map[string]any{
		"clientInfo": map[string]any{
			"name":    "codex-bridge",
			"title":   "Codex Bridge",
			"version": "0.1.0",
		},
		"capabilities": map[string]any{
			"experimentalApi":           true,
			"requestAttestation":        false,
			"optOutNotificationMethods": nil,
		},
	}, nil); err != nil {
		return err
	}
	return c.Notify(ctx, "initialized", nil)
}

func CheckBinary(ctx context.Context, command, minVersion string) (string, error) {
	command = strings.TrimSpace(command)
	if command == "" {
		command = "codex"
	}
	parts := strings.Fields(command)
	path, err := exec.LookPath(parts[0])
	if err != nil {
		return "", fmt.Errorf("Codex binary %q not found", parts[0])
	}
	checkCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	out, err := exec.CommandContext(checkCtx, path, "--version").CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("failed to run %s --version: %w", path, err)
	}
	version := ParseVersion(string(out))
	if version == "" {
		return "", fmt.Errorf("failed to parse Codex version from %q", strings.TrimSpace(string(out)))
	}
	if CompareVersion(version, minVersion) < 0 {
		return "", fmt.Errorf("Codex version %s is too old; need at least %s", version, minVersion)
	}
	return path, nil
}

func ParseVersion(output string) string {
	for _, field := range strings.Fields(output) {
		field = strings.TrimPrefix(field, "v")
		if strings.Count(field, ".") >= 1 && CompareVersion(field, "0.0.0") >= 0 {
			return field
		}
	}
	return ""
}

func CompareVersion(left, right string) int {
	a, b := versionParts(left), versionParts(right)
	for i := 0; i < 3; i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	return 0
}

func versionParts(version string) [3]int {
	var out [3]int
	for i, part := range strings.Split(version, ".") {
		if i >= 3 {
			break
		}
		n, err := strconv.Atoi(strings.TrimRightFunc(part, func(r rune) bool { return r < '0' || r > '9' }))
		if err == nil {
			out[i] = n
		}
	}
	return out
}
