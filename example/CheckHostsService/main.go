package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/injoyai/tdx"
	"github.com/injoyai/tdx/protocol"
)

type result struct {
	Host     string
	Region   string
	DialMs   int64
	CountSH  int
	CountSZ  int
	Err      string
}

func check(host, region string) result {
	r := result{Host: host, Region: region}

	start := time.Now()
	c, err := tdx.Dial(host, tdx.WithDebug(false))
	r.DialMs = time.Since(start).Milliseconds()
	if err != nil {
		r.Err = err.Error()
		return r
	}
	defer c.Close()

	// 等待异步连接握手完成
	time.Sleep(500 * time.Millisecond)
	c.Wait.SetTimeout(8 * time.Second)

	sh, err := c.GetCount(protocol.ExchangeSH)
	if err != nil {
		r.Err = fmt.Sprintf("GetCount(SH): %v", err)
		return r
	}
	r.CountSH = int(sh.Count)

	sz, err := c.GetCount(protocol.ExchangeSZ)
	if err != nil {
		r.Err = fmt.Sprintf("GetCount(SZ): %v", err)
		return r
	}
	r.CountSZ = int(sz.Count)

	return r
}

func main() {
	type hostEntry struct {
		addr   string
		region string
	}

	var hosts []hostEntry
	for _, h := range tdx.SHHosts {
		hosts = append(hosts, hostEntry{h, "上海"})
	}
	for _, h := range tdx.BJHosts {
		hosts = append(hosts, hostEntry{h, "北京"})
	}
	for _, h := range tdx.GZHosts {
		hosts = append(hosts, hostEntry{h, "广州"})
	}
	for _, h := range tdx.WHHosts {
		hosts = append(hosts, hostEntry{h, "武汉"})
	}

	results := make([]result, len(hosts))
	var wg sync.WaitGroup
	wg.Add(len(hosts))

	for i, h := range hosts {
		go func(i int, h hostEntry) {
			defer wg.Done()
			results[i] = check(h.addr, h.region)
		}(i, h)
	}
	wg.Wait()

	fmt.Printf("\n%-18s %-4s %8s %8s %8s  %s\n", "HOST", "区域", "延迟ms", "沪股数", "深股数", "状态")
	fmt.Println("-------------------------------------------------------------------------------")

	ok, fail := 0, 0
	for _, r := range results {
		status := "OK"
		if r.Err != "" {
			status = r.Err
			fail++
		} else {
			ok++
		}
		fmt.Printf("%-18s %-4s %6dms %7d %7d  %s\n",
			r.Host, r.Region, r.DialMs, r.CountSH, r.CountSZ, status)
	}
	fmt.Printf("\n合计: %d 可用, %d 不可用, 共 %d\n", ok, fail, ok+fail)
}
