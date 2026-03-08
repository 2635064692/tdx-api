package quote

import (
	"os"
	"strings"
	"testing"
)

func TestMigrationDoesNotOverwriteExistingSysConfig(t *testing.T) {
	content, err := os.ReadFile("../../migrations/001_quote_storage_schema.sql")
	if err != nil {
		t.Fatalf("read migration failed: %v", err)
	}
	sql := string(content)
	if !strings.Contains(sql, "INSERT IGNORE INTO sys_config") {
		t.Fatalf("expected migration to use INSERT IGNORE for sys_config defaults")
	}
	if strings.Contains(sql, "config_value = VALUES(config_value)") {
		t.Fatalf("migration should not overwrite existing sys_config values")
	}
}
