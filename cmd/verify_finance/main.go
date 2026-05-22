package main

import (
	"encoding/hex"
	"fmt"
	"os"
	"time"

	"github.com/injoyai/tdx"
	"github.com/injoyai/tdx/protocol"
)

// 验证基本面协议号
// 策略：每个协议号使用独立连接，避免超时关闭影响后续测试
// 通过观察日志输出 "通讯类型未解析:0xXXXX" 判断协议号有效

func main() {
	fmt.Println("===== TDX 基本面协议号验证 =====")
	fmt.Println()

	tests := []struct {
		name    string
		typeVal uint16
	}{
		{"0x000f 除权除息(XDXR)", 0x000f},
		{"0x0010 财务数据(FINANCE)", 0x0010},
		{"0x02cf 公司信息文件目录", 0x02cf},
		{"0x02d0 公司信息内容(F10)", 0x02d0},
	}

	data := []byte{0x00, 0x00, '0', '0', '0', '0', '0', '1'}

	for i, t := range tests {
		fmt.Printf("[%d] %s\n", i+1, t.name)

		// 每次独立连接
		c, err := tdx.DialDefault(tdx.WithDebug(false))
		if err != nil {
			fmt.Printf("  连接失败: %v\n\n", err)
			continue
		}

		c.SetTimeout(5 * time.Second)

		f := &protocol.Frame{
			Control: 0x01,
			Type:    t.typeVal,
			Data:    data,
		}

		result, err := c.SendFrame(f)
		c.Close()

		if err != nil {
			errMsg := err.Error()
			if contains(errMsg, "timeout") {
				// 超时 = handlerDealMessage default 分支收到数据但未调用 Wait.Done
				// 结合日志 "通讯类型未解析:0xXXXX" 确认服务器有返回
				fmt.Println("  超时(本地无解码器) → 日志应显示'通讯类型未解析' → 协议号有效")
			} else if contains(errMsg, "closed") {
				fmt.Printf("  连接已关闭: %v\n", errMsg)
				fmt.Println("  ※ 同样因为超时触发关闭，查看上方日志是否有'通讯类型未解析'")
			} else {
				fmt.Printf("  其他错误: %v\n", err)
			}
		} else {
			if result == nil {
				fmt.Println("  返回 nil")
			} else {
				fmt.Printf("  成功: type=%T\n", result)
				dumpHex(result)
			}
		}
		fmt.Println()
	}

	// 对照组
	fmt.Println("[对照] 行情接口 - 已知有效")
	c, err := tdx.DialDefault(tdx.WithDebug(false))
	if err != nil {
		fmt.Printf("  连接失败: %v\n", err)
		os.Exit(1)
	}
	quotes, err := c.GetQuote("sz000001")
	c.Close()
	if err != nil {
		fmt.Printf("  失败: %v\n", err)
	} else if len(quotes) > 0 {
		q := quotes[0]
		fmt.Printf("  成功: %s%s 收盘价=%.2f\n", q.Exchange, q.Code, q.K.Close.Float64())
		fmt.Println("  >>> 对照通过 <<<")
	}
}

func dumpHex(v interface{}) {
	switch d := v.(type) {
	case []byte:
		n := len(d)
		if n > 64 {
			n = 64
		}
		fmt.Printf("  hex: %s\n", hex.EncodeToString(d[:n]))
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(sub) == 0 ||
		(len(s) > 0 && len(sub) > 0 && findSub(s, sub)))
}

func findSub(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
