package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/injoyai/tdx"
)

type result struct {
	Host      string
	Region    string
	DialMs    int64
	QuoteOK   bool
	QuoteErr  string
	KlineOK   bool
	KlineErr  string
	TradeOK   bool
	TradeErr  string
	MinuteOK  bool
	MinuteErr string
}

func check(host, region string) result {
	r := result{Host: host, Region: region}

	start := time.Now()
	c, err := tdx.Dial(host, tdx.WithDebug(false))
	r.DialMs = time.Since(start).Milliseconds()
	if err != nil {
		r.QuoteErr = err.Error()
		return r
	}
	defer c.Close()

	time.Sleep(500 * time.Millisecond)
	c.Wait.SetTimeout(8 * time.Second)

	// 1. 测试行情数据 - 使用纯股票代码（6位数字会自动加前缀）
	quotes, err := c.GetQuote("000001", "600519")
	if err != nil {
		r.QuoteErr = err.Error()
	} else if len(quotes) == 2 {
		r.QuoteOK = true
	} else {
		r.QuoteErr = fmt.Sprintf("expected 2 quotes, got %d", len(quotes))
	}

	// 2. 测试K线数据
	kline, err := c.GetKlineDay("000001", 0, 10)
	if err != nil {
		r.KlineErr = err.Error()
	} else if kline.Count > 0 && len(kline.List) > 0 {
		r.KlineOK = true
	} else {
		r.KlineErr = "no kline data"
	}

	// 3. 测试分时成交
	trade, err := c.GetTrade("000001", 0, 100)
	if err != nil {
		r.TradeErr = err.Error()
	} else if trade.Count > 0 {
		r.TradeOK = true
	} else {
		r.TradeErr = "no trade data"
	}

	// 4. 测试分时数据
	minute, err := c.GetMinute("000001")
	if err != nil {
		r.MinuteErr = err.Error()
	} else if minute.Count > 0 {
		r.MinuteOK = true
	} else {
		r.MinuteErr = "no minute data"
	}

	return r
}

func main() {
	// 只测试之前验证可用的 20 个 host
	availableHosts := []struct {
		addr   string
		region string
	}{
		{"119.97.185.59", "武汉"},
		{"124.71.187.122", "上海"},
		{"123.60.70.228", "上海"},
		{"124.71.187.72", "上海"},
		{"118.25.98.114", "上海"},
		{"124.70.199.56", "上海"},
		{"124.70.133.119", "上海"},
		{"111.230.186.52", "广州"},
		{"122.51.232.182", "上海"},
		{"124.71.9.153", "广州"},
		{"122.51.120.217", "上海"},
		{"123.60.73.44", "上海"},
		{"121.36.225.169", "上海"},
		{"123.60.84.66", "上海"},
		{"111.229.247.189", "上海"},
		{"110.41.147.114", "广州"},
		{"116.205.183.150", "广州"},
		{"116.205.171.132", "广州"},
		{"110.41.2.72", "广州"},
		{"116.205.163.254", "广州"},
	}

	results := make([]result, len(availableHosts))
	var wg sync.WaitGroup
	wg.Add(len(availableHosts))

	for i, h := range availableHosts {
		go func(i int, addr, region string) {
			defer wg.Done()
			results[i] = check(addr, region)
		}(i, h.addr, h.region)
	}
	wg.Wait()

	fmt.Printf("\n%-18s %-4s %6s  %-5s %-5s %-5s %-5s  %s\n",
		"HOST", "区域", "延迟", "行情", "K线", "成交", "分时", "备注")
	fmt.Println("--------------------------------------------------------------------------------")

	fullOK := 0
	for _, r := range results {
		status := "OK"
		mark := func(ok bool) string {
			if ok {
				return "✓"
			}
			return "✗"
		}

		if !r.QuoteOK || !r.KlineOK || !r.TradeOK || !r.MinuteOK {
			errs := []string{}
			if r.QuoteErr != "" {
				errs = append(errs, "行情:"+r.QuoteErr)
			}
			if r.KlineErr != "" {
				errs = append(errs, "K线:"+r.KlineErr)
			}
			if r.TradeErr != "" {
				errs = append(errs, "成交:"+r.TradeErr)
			}
			if r.MinuteErr != "" {
				errs = append(errs, "分时:"+r.MinuteErr)
			}
			if len(errs) > 0 {
				status = errs[0]
			}
		} else {
			fullOK++
		}

		fmt.Printf("%-18s %-4s %4dms  %-5s %-5s %-5s %-5s  %s\n",
			r.Host, r.Region, r.DialMs,
			mark(r.QuoteOK), mark(r.KlineOK), mark(r.TradeOK), mark(r.MinuteOK),
			status)
	}
	fmt.Printf("\n全功能可用: %d/%d\n", fullOK, len(availableHosts))
}
