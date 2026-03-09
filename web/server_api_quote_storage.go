package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	tdx "github.com/injoyai/tdx"
	"github.com/injoyai/tdx/protocol"
	webquote "web/quote"
)

const (
	quoteStorageTaskCollector = "quote-storage-collector"
	quoteStorageTaskArchival  = "quote-storage-archival"
)

var quoteStorageSystem *webquote.QuoteStorageSystem

func initQuoteStorageSystem() {
	deps := webquote.SystemDependencies{
		Collector: webquote.CollectorDependencies{
			NormalizeCodes:  normalizeStockCodes,
			FetchRealQuotes: fetchQuotesBatched,
			FetchMockQuote: func(code string) (*protocol.Quote, error) {
				startMockQuotePusher()
				if mockPusher == nil {
					return nil, fmt.Errorf("mock quote pusher is unavailable")
				}
				return mockPusher.generateQuote(code), nil
			},
			Now: time.Now,
		},
		PollInterval:  time.Minute,
		MigrationFile: "../migrations/001_quote_storage_schema.sql",
	}
	quoteStorageSystem = webquote.NewQuoteStorageSystem(nil, deps)
	bootstrap := webquote.LoadBootstrapConfig(os.Getenv)
	if !bootstrap.Enabled {
		log.Printf("[quote-storage] bootstrap disabled by QUOTE_STORAGE_ENABLED, storage subsystem stays standby")
		return
	}
	if bootstrap.DSN == "" {
		log.Printf("[quote-storage] mysql dsn not configured, storage subsystem stays standby")
		quoteStorageSystem.ReportError(fmt.Errorf("QUOTE_STORAGE_MYSQL_DSN is not configured"))
		return
	}
	engine, err := webquote.OpenMySQLEngine(bootstrap.DSN)
	if err != nil {
		log.Printf("[quote-storage] mysql init failed: %v", err)
		quoteStorageSystem.ReportError(err)
		return
	}
	quoteStorageSystem = webquote.NewQuoteStorageSystem(engine, deps)
	if status := quoteStorageSystem.Status(); status.LastError != "" {
		log.Printf("[quote-storage] startup warning: %s", status.LastError)
	}
	if err := quoteStorageSystem.Start(); err != nil {
		log.Printf("[quote-storage] startup failed: %v", err)
		quoteStorageSystem.ReportError(err)
		return
	}
	scheduleQuoteStorageArchival()
}

func scheduleQuoteStorageArchival() {
	if manager == nil || quoteStorageSystem == nil || !quoteStorageSystem.Status().Ready {
		return
	}
	manager.AddWorkdayTask("0 0 16 * * *", func(_ *tdx.Manage) {
		taskManager.Run(quoteStorageTaskArchival, func(ctx context.Context) error {
			return quoteStorageSystem.RunArchival(ctx, time.Now())
		})
	})
}

func handleQuoteStorageStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		errorResponse(w, "只支持GET请求")
		return
	}
	if quoteStorageSystem == nil {
		errorResponse(w, "行情存储系统未初始化")
		return
	}
	successResponse(w, quoteStorageSystem.Status())
}

func handleQuoteStorageStart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		errorResponse(w, "只支持POST请求")
		return
	}
	if !quoteStorageReady(w) {
		return
	}
	if err := quoteStorageSystem.Start(); err != nil {
		errorResponse(w, "启动行情存储系统失败: "+err.Error())
		return
	}
	successResponse(w, quoteStorageSystem.Status())
}

func handleQuoteStorageStop(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		errorResponse(w, "只支持POST请求")
		return
	}
	if quoteStorageSystem == nil {
		errorResponse(w, "行情存储系统未初始化")
		return
	}
	if err := quoteStorageSystem.Stop(); err != nil {
		errorResponse(w, "停止行情存储系统失败: "+err.Error())
		return
	}
	successResponse(w, quoteStorageSystem.Status())
}

func handleQuoteStorageStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		errorResponse(w, "只支持GET请求")
		return
	}
	if quoteStorageSystem == nil {
		errorResponse(w, "行情存储系统未初始化")
		return
	}
	status := quoteStorageSystem.Status()
	successResponse(w, map[string]interface{}{
		"ready":           status.Ready,
		"running":         status.Running,
		"queue_size":      status.QueueSize,
		"queue_capacity":  status.QueueCapacity,
		"config":          status.Config,
		"writer":          status.Writer,
		"collector_codes": status.CollectorCodes,
	})
}

func handleQuoteStorageArchival(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		errorResponse(w, "只支持POST请求")
		return
	}
	if !quoteStorageReady(w) {
		return
	}
	tradeDate, err := quoteStorageRequestDate(r)
	if err != nil {
		errorResponse(w, err.Error())
		return
	}
	taskID := taskManager.Run(quoteStorageTaskArchival, func(ctx context.Context) error {
		return quoteStorageSystem.RunArchival(ctx, tradeDate)
	})
	successResponse(w, map[string]string{"task_id": taskID})
}

func handleQuoteStorageTasks(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		errorResponse(w, "只支持GET请求")
		return
	}
	if quoteStorageSystem == nil {
		errorResponse(w, "行情存储系统未初始化")
		return
	}
	tasks := make([]*Task, 0)
	for _, task := range taskManager.List() {
		if strings.HasPrefix(task.Type, quoteStorageTaskArchival) {
			tasks = append(tasks, task)
		}
	}
	status := quoteStorageSystem.Status()
	successResponse(w, map[string]interface{}{
		"collector": map[string]interface{}{
			"type":   quoteStorageTaskCollector,
			"status": quoteStorageCollectorStatus(status.Running),
			"codes":  status.CollectorCodes,
		},
		"archival": tasks,
	})
}

func quoteStorageReady(w http.ResponseWriter) bool {
	if quoteStorageSystem == nil {
		errorResponse(w, "行情存储系统未初始化")
		return false
	}
	if !quoteStorageSystem.Status().Ready {
		errorResponse(w, "行情存储系统未就绪，请检查 QUOTE_STORAGE_MYSQL_DSN 与数据库连接")
		return false
	}
	return true
}

func quoteStorageRequestDate(r *http.Request) (time.Time, error) {
	raw := strings.TrimSpace(r.URL.Query().Get("date"))
	if raw == "" {
		return time.Now(), nil
	}
	for _, layout := range []string{"2006-01-02", "20060102"} {
		if value, err := time.Parse(layout, raw); err == nil {
			return value, nil
		}
	}
	return time.Time{}, fmt.Errorf("date 参数格式错误，应为 YYYY-MM-DD 或 YYYYMMDD")
}

func quoteStorageCollectorStatus(running bool) string {
	if running {
		return string(TaskStatusRunning)
	}
	return string(TaskStatusSuccess)
}
