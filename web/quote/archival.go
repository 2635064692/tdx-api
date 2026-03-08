package quote

import (
	"context"
	"time"

	"xorm.io/xorm"
)

type ArchivalTask struct {
	db *xorm.Engine
}

func NewArchivalTask(db *xorm.Engine) *ArchivalTask                        { return &ArchivalTask{db: db} }
func (a *ArchivalTask) Run(ctx context.Context, tradeDate time.Time) error { return nil }
