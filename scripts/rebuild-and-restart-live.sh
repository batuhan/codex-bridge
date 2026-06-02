#!/usr/bin/env bash
set -euo pipefail

ROOT=${CODEX_BRIDGE_ROOT:-/Users/batuhan/projects/codex-bridge}
BIN=${CODEX_BRIDGE_BIN:-$ROOT/bin/codex-bridge}

PROD_SESSION=${CODEX_BRIDGE_PROD_SESSION:-sh-codex}
PROD_CONFIG=${CODEX_BRIDGE_PROD_CONFIG:-$ROOT/config.yaml}

QA_SESSION=${CODEX_BRIDGE_QA_SESSION:-sh-codex-qa}
QA_CONFIG=${CODEX_BRIDGE_QA_CONFIG:-/tmp/codex-bridge-qa-config.yaml}

cd "$ROOT"

test -f "$PROD_CONFIG"
test -f "$QA_CONFIG"

go build -o "$BIN" ./cmd/codex

restart() {
	local session=$1
	local config=$2
	local command="cd '$ROOT' && '$BIN' -c '$config'"

	if tmux has-session -t "$session" 2>/dev/null; then
		tmux send-keys -t "$session" C-c
	else
		tmux new-session -d -s "$session"
	fi
	tmux send-keys -t "$session" "$command" Enter
}

restart "$PROD_SESSION" "$PROD_CONFIG"
restart "$QA_SESSION" "$QA_CONFIG"

sleep 2
pgrep -fl "codex-bridge.*($PROD_CONFIG|$QA_CONFIG)" || true
