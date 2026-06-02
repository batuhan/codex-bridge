package main

import (
	"maunium.net/go/mautrix/bridgev2/matrix/mxmain"

	"github.com/beeper/codex-bridge/pkg/bridge"
)

var (
	Tag       = "dev"
	Commit    = "unknown"
	BuildTime = "unknown"
)

var codex = &bridge.Connector{}

var mainBridge = mxmain.BridgeMain{
	Name:        "codex",
	URL:         "https://github.com/beeper/codex-bridge",
	Description: "A Beeper Codex bridge.",
	Version:     "0.1.0",
	Connector:   codex,
}

func main() {
	mainBridge.InitVersion(Tag, Commit, BuildTime)
	mainBridge.Run()
}
