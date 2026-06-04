#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN_DIR="${CODEX_BRIDGE_BIN_DIR:-$ROOT/bin}"
BRIDGE_BIN="${CODEX_BRIDGE_BIN:-$BIN_DIR/codex-bridge}"
if [[ -n "${CODEX_BRIDGE_NAME:-}" ]]; then
	BRIDGE_NAME="$CODEX_BRIDGE_NAME"
	BRIDGE_NAME_EXPLICIT=1
else
	BRIDGE_NAME="sh-codex"
	BRIDGE_NAME_EXPLICIT=0
fi
CONFIG_FILE="${CODEX_BRIDGE_CONFIG_FILE:-config.yaml}"
if [[ "$CONFIG_FILE" = /* ]]; then
	CONFIG_PATH="$CONFIG_FILE"
else
	CONFIG_PATH="$ROOT/$CONFIG_FILE"
fi

BBCTL_BIN="${BBCTL_BIN:-}"

CODEX_CLI="${CODEX_CLI:-}"

log() {
	printf '\033[1;36m==>\033[0m %s\n' "$*"
}

mask_text() {
	local value="$1"
	if [[ ${#value} -le 2 ]]; then
		printf '****'
	else
		printf '%s****' "${value:0:2}"
	fi
}

mask_mxid() {
	local mxid="$1"
	if [[ "$mxid" =~ ^@([^:]+):(.*)$ ]]; then
		printf '@%s:%s' "$(mask_text "${BASH_REMATCH[1]}")" "${BASH_REMATCH[2]}"
	else
		mask_text "$mxid"
	fi
}

redact_bbctl_output() {
	sed -E \
		-e 's/(User ID: @)[^:[:space:]]+(:[^[:space:]]+)/\1****\2/g' \
		-e 's/(Name: ).+/\1****/g' \
		-e 's/(Email: )[^@[:space:]]+@([^[:space:]]+)/\1****@\2/g' \
		-e 's/(Support room ID: ).+/\1****/g' \
		-e 's/(Registered at: ).+/\1****/g' \
		-e 's/(Cluster ID: ).+/\1****/g' \
		-e 's#(Hungryserv URL: https://matrix\.[^/]+/_hungryserv/)[^[:space:]]+#\1****#g' \
		-e 's/(remote: [A-Z_]+ \()[^/()]+( \/ [^)]+\))/\1****\2/g'
}

find_codex_cli() {
	if [[ -n "$CODEX_CLI" ]]; then
		return
	fi
	local candidate
	while IFS= read -r candidate; do
		if [[ "$candidate" != "$ROOT/codex" ]]; then
			CODEX_CLI="$candidate"
			return
		fi
	done < <(type -ap codex || true)
	if [[ -x "/opt/homebrew/bin/codex" ]]; then
		CODEX_CLI="/opt/homebrew/bin/codex"
	elif [[ -x "/usr/local/bin/codex" ]]; then
		CODEX_CLI="/usr/local/bin/codex"
	else
		CODEX_CLI="codex"
	fi
}

bbctl() {
	if [[ -n "$BBCTL_BIN" ]]; then
		"$BBCTL_BIN" "$@"
	else
		(cd "$ROOT" && go tool bbctl "$@")
	fi
}

ensure_bbctl() {
	if [[ -n "$BBCTL_BIN" ]]; then
		if [[ ! -x "$BBCTL_BIN" ]]; then
			printf 'BBCTL_BIN is not executable: %s\n' "$BBCTL_BIN" >&2
			exit 1
		fi
		return
	fi
	(cd "$ROOT" && go tool -n bbctl >/dev/null)
}

ensure_beeper_login() {
	local whoami
	whoami="$(bbctl --color never whoami 2>&1 || true)"
	if grep -q '^User ID:' <<<"$whoami"; then
		local user_id
		user_id="$(awk -F': ' '/^User ID:/ { print $2; exit }' <<<"$whoami")"
		log "Beeper login found as $(mask_mxid "$user_id")"
		return
	fi
	log "Beeper login required"
	if [[ -n "$whoami" ]]; then
		printf '%s\n' "$whoami" | redact_bbctl_output
	fi
	bbctl --color never login
}

bridge_name_taken() {
	local name="$1"
	local whoami
	whoami="$(bbctl --color never whoami 2>&1 || true)"
	awk -v target="$name" '$1 == target { found = 1 } END { exit !found }' <<<"$whoami"
}

choose_bridge_name() {
	if [[ "$BRIDGE_NAME_EXPLICIT" == 1 ]]; then
		return
	fi

	local base="$BRIDGE_NAME"
	local candidate="$base"
	local suffix=2
	while bridge_name_taken "$candidate"; do
		candidate="$base-$suffix"
		suffix=$((suffix + 1))
	done

	if [[ "$candidate" != "$BRIDGE_NAME" ]]; then
		log "Bridge name $BRIDGE_NAME is already registered; using $candidate"
		BRIDGE_NAME="$candidate"
	fi
}

configured_bridge_name() {
	awk '
		/^[[:space:]]*bot:[[:space:]]*$/ {
			in_bot = 1
			next
		}
		in_bot && /^[[:space:]]*username:[[:space:]]*/ {
			name = $0
			sub(/^[[:space:]]*username:[[:space:]]*/, "", name)
			gsub(/["'\''"]/, "", name)
			sub(/bot$/, "", name)
			print name
			exit
		}
		in_bot && /^[^[:space:]]/ {
			in_bot = 0
		}
	' "$CONFIG_PATH"
}

ensure_codex_login() {
	find_codex_cli
	local codex_status
	codex_status="$("$CODEX_CLI" login status 2>&1 || true)"
	if grep -qi '^Logged in' <<<"$codex_status"; then
		log "Codex login found: $codex_status"
		return
	fi
	log "Codex login required"
	"$CODEX_CLI" login
}

build_bridge() {
	log "Building codex bridge"
	mkdir -p "$BIN_DIR"
	(cd "$ROOT" && go build -o "$BRIDGE_BIN" ./cmd/codex)
}

ensure_config() {
	if [[ -s "$CONFIG_PATH" ]]; then
		local configured
		configured="$(configured_bridge_name || true)"
		if [[ -n "$configured" ]]; then
			BRIDGE_NAME="$configured"
		fi
		log "Using existing $CONFIG_FILE"
		return
	fi

	choose_bridge_name
	log "Generating $CONFIG_FILE with bbctl"
	bbctl --color never config \
		--type bridgev2 \
		--output "$CONFIG_PATH" \
		"$BRIDGE_NAME"

	if [[ ! -s "$CONFIG_PATH" ]]; then
		printf 'bbctl did not create a usable config at %s\n' "$CONFIG_PATH" >&2
		exit 1
	fi
}

run_bridge() {
	log "Running $BRIDGE_NAME"
	cd "$ROOT"
	exec "$BRIDGE_BIN" -c "$CONFIG_PATH"
}

main() {
	ensure_bbctl
	ensure_beeper_login
	ensure_codex_login
	ensure_config
	build_bridge
	run_bridge
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
	main "$@"
fi
