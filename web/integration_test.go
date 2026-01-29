// +build integration

package main

import (
	"testing"

	tdx "github.com/injoyai/tdx"
)

// TestIntegration_GetHistoryMinuteTradeDay 集成测试，调用真实 TDX 服务器
// 运行: go test -tags=integration -v -run TestIntegration_GetHistoryMinuteTradeDay
func TestIntegration_GetHistoryMinuteTradeDay(t *testing.T) {
	// 连接真实 TDX 服务器
	client, err := tdx.DialDefault()
	if err != nil {
		t.Fatalf("连接 TDX 服务器失败: %v", err)
	}
	defer client.Close()

	// 调用真实方法 - 在这里设置断点
	resp, err := client.GetHistoryMinuteTradeDay("20250120", "000001")
	if err != nil {
		t.Fatalf("获取历史分时成交失败: %v", err)
	}

	t.Logf("获取到 %d 条成交记录", resp.Count)
	if len(resp.List) > 0 {
		t.Logf("第一条: 时间=%v, 价格=%d, 成交量=%d",
			resp.List[0].Time, resp.List[0].Price, resp.List[0].Volume)
	}
}

// TestIntegration_GetMinuteTrade 测试今日分时成交
func TestIntegration_GetMinuteTrade(t *testing.T) {
	client, err := tdx.DialDefault()
	if err != nil {
		t.Fatalf("连接 TDX 服务器失败: %v", err)
	}
	defer client.Close()

	resp, err := client.GetMinuteTrade("000001", 0, 100)
	if err != nil {
		t.Fatalf("获取分时成交失败: %v", err)
	}

	t.Logf("获取到 %d 条成交记录", resp.Count)
}
