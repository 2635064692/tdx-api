package main

import (
	"fmt"
	"time"

	"github.com/injoyai/tdx"
	"github.com/injoyai/tdx/protocol"
)

func main() {
	host := "124.71.187.122"
	fmt.Printf("测试 %s:7709\n\n", host)

	c, err := tdx.Dial(host, tdx.WithDebug(true))
	if err != nil {
		fmt.Printf("Dial 失败: %v\n", err)
		return
	}
	defer c.Close()
	fmt.Println("Dial 成功")

	// 等待连接完全建立
	time.Sleep(time.Second)
	c.Wait.SetTimeout(10 * time.Second)

	// GetCount
	sh, err := c.GetCount(protocol.ExchangeSH)
	if err != nil {
		fmt.Printf("GetCount(SH) 失败: %v\n", err)
	} else {
		fmt.Printf("GetCount(SH) = %d\n", sh.Count)
	}

	sz, err := c.GetCount(protocol.ExchangeSZ)
	if err != nil {
		fmt.Printf("GetCount(SZ) 失败: %v\n", err)
	} else {
		fmt.Printf("GetCount(SZ) = %d\n", sz.Count)
	}

	// GetQuote (不依赖 DefaultCodes，用带前缀的代码)
	quotes, err := c.GetQuote("sh000001")
	if err != nil {
		fmt.Printf("GetQuote(sh000001) 失败: %v\n", err)
	} else if len(quotes) > 0 {
		q := quotes[0]
		fmt.Printf("GetQuote(sh000001) = %s last=%d open=%d high=%d low=%d\n",
			q.Code, q.K.Last, q.K.Open, q.K.High, q.K.Low)
	}
}
