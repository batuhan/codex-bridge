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

const (
	defaultCodexCommand = "codex"
	defaultMinVersion   = "0.133.0"
)

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
	c.applyCommandDefaults()
}

func (c *Config) applyCommandDefaults() {
	c.CodexCommand = firstNonEmptyString(c.CodexCommand, defaultCodexCommand)
	c.MinVersion = firstNonEmptyString(c.MinVersion, defaultMinVersion)
}

func (c *Connector) GetConfig() (string, any, up.Upgrader) {
	c.Config.ApplyDefaults()
	return ExampleConfig, &c.Config, configUpgrader()
}

func configUpgrader() up.Upgrader {
	return up.SimpleUpgrader(func(helper up.Helper) {
		helper.Copy(up.Str, "codex_command")
		helper.Copy(up.Str, "min_version")
		helper.Copy(up.Str, "default_model")
	})
}
