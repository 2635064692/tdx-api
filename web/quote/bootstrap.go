package quote

import (
	"os"
	"strings"
)

type BootstrapConfig struct {
	Enabled bool
	DSN     string
}

func LoadBootstrapConfig(getenv func(string) string) BootstrapConfig {
	if getenv == nil {
		getenv = os.Getenv
	}
	return BootstrapConfig{
		Enabled: parseBootstrapEnabled(getenv("QUOTE_STORAGE_ENABLED")),
		DSN:     strings.TrimSpace(getenv("QUOTE_STORAGE_MYSQL_DSN")),
	}
}

func parseBootstrapEnabled(raw string) bool {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return true
	}
}
