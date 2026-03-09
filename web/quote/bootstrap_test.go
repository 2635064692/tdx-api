package quote

import "testing"

func TestLoadBootstrapConfigDefaultsToEnabled(t *testing.T) {
	cfg := LoadBootstrapConfig(func(string) string { return "" })
	if !cfg.Enabled {
		t.Fatalf("expected bootstrap to be enabled by default")
	}
	if cfg.DSN != "" {
		t.Fatalf("expected empty dsn by default, got %q", cfg.DSN)
	}
}

func TestLoadBootstrapConfigCanDisableWithoutDSN(t *testing.T) {
	cfg := LoadBootstrapConfig(func(key string) string {
		if key == "QUOTE_STORAGE_ENABLED" {
			return "off"
		}
		return ""
	})
	if cfg.Enabled {
		t.Fatalf("expected bootstrap disabled when env switch is off")
	}
	if cfg.DSN != "" {
		t.Fatalf("expected empty dsn while disabled, got %q", cfg.DSN)
	}
}

func TestLoadBootstrapConfigReadsDSNWhenEnabled(t *testing.T) {
	cfg := LoadBootstrapConfig(func(key string) string {
		switch key {
		case "QUOTE_STORAGE_ENABLED":
			return "true"
		case "QUOTE_STORAGE_MYSQL_DSN":
			return "root:pwd@tcp(127.0.0.1:3306)/vnpy"
		default:
			return ""
		}
	})
	if !cfg.Enabled {
		t.Fatalf("expected bootstrap enabled")
	}
	if cfg.DSN != "root:pwd@tcp(127.0.0.1:3306)/vnpy" {
		t.Fatalf("unexpected dsn: %q", cfg.DSN)
	}
}
