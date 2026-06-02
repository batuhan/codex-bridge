package bridge

import (
	_ "embed"

	up "go.mau.fi/util/configupgrade"
	"gopkg.in/yaml.v3"
)

//go:embed example-config.yaml
var ExampleConfig string

type Config struct {
	CodexCommand string `yaml:"codex_command"`
	MinVersion   string `yaml:"min_version"`
	DefaultModel string `yaml:"default_model"`
}

type umConfig Config

func (c *Config) UnmarshalYAML(node *yaml.Node) error {
	c.ApplyDefaults()
	if err := node.Decode((*umConfig)(c)); err != nil {
		return err
	}
	c.ApplyDefaults()
	return nil
}

func (c *Config) ApplyDefaults() {
	if c.CodexCommand == "" {
		c.CodexCommand = "codex"
	}
	if c.MinVersion == "" {
		c.MinVersion = "0.133.0"
	}
}

func upgradeConfig(helper up.Helper) {
	helper.Copy(up.Str, "codex_command")
	helper.Copy(up.Str, "min_version")
	helper.Copy(up.Str, "default_model")
}

func (c *Connector) GetConfig() (string, any, up.Upgrader) {
	c.Config.ApplyDefaults()
	return ExampleConfig, &c.Config, up.SimpleUpgrader(upgradeConfig)
}
