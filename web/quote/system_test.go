package quote

import (
	"context"
	"testing"
	"time"
)

func TestSystemStartStopWithoutDB(t *testing.T) {
	system := NewQuoteStorageSystem(nil, SystemDependencies{})
	if err := system.Start(); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	status := system.Status()
	if !status.Running {
		t.Fatalf("expected running status")
	}
	if status.Ready {
		t.Fatalf("expected not ready without db")
	}
	if err := system.Stop(); err != nil {
		t.Fatalf("Stop returned error: %v", err)
	}
}

func TestSplitStatements(t *testing.T) {
	statements := splitStatements("CREATE TABLE a (id INT);\n\nINSERT INTO a VALUES (1);")
	if len(statements) != 2 {
		t.Fatalf("unexpected statements: %+v", statements)
	}
}

func TestDrainReturnsOnEmptyQueue(t *testing.T) {
	system := NewQuoteStorageSystem(nil, SystemDependencies{})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := system.Drain(ctx); err != nil {
		t.Fatalf("Drain returned error: %v", err)
	}
}
