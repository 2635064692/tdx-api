package main

import (
	"encoding/hex"
	"fmt"
	"os"
	"time"

	"github.com/injoyai/conv"
	"github.com/injoyai/tdx"
	"github.com/injoyai/tdx/protocol"
)

// 验证基本面协议号：0x0010(财务), 0x000f(除权除息), 0x02cf(公司目录), 0x02d0(公司内容)
// 使用 tdx.Client 连接，利用 handlerDealMessage 的 default 分支日志输出判断协议号是否可用

func main() {
	fmt.Println("===== TDX 基本面协议号验证 =====")
	fmt.Println()

	c, err := tdx.DialDefault(tdx.WithDebug(false))
	if err != nil {
		fmt.Printf("连接失败: %v\n", err)
		os.Exit(1)
	}
	defer c.Close()
	fmt.Println("连接成功")
	fmt.Println()

	// 延长超时以便等待响应
	c.SetTimeout(5 * time.Second)

	// 测试协议号
	tests := []struct {
		name    string
		typeVal uint16
		data    []byte
	}{
		{
			name:    "0x000f 除权除息(XDXR)",
			typeVal: 0x000f,
			data:    []byte{0x00, 0x00, '0', '0', '0', '0', '0', '1'},
		},
		{
			name:    "0x0010 财务数据(FINANCE)",
			typeVal: 0x0010,
			data:    []byte{0x00, 0x00, '0', '0', '0', '0', '0', '1'},
		},
		{
			name:    "0x02cf 公司信息文件目录",
			typeVal: 0x02cf,
			data:    []byte{0x00, 0x00, '0', '0', '0', '0', '0', '1'},
		},
		{
			name:    "0x02d0 公司信息内容(F10)",
			typeVal: 0x02d0,
			data:    []byte{0x00, 0x00, '0', '0', '0', '0', '0', '1'},
		},
	}

	for i, t := range tests {
		fmt.Printf("[%d] 测试 %s\n", i+1, t.name)

		f := &protocol.Frame{
			Control: 0x01,
			Type:    t.typeVal,
			Data:    t.data,
		}

		// SendFrame 内部会走 handlerDealMessage
		// default 分支会打印 "通讯类型未解析:0xXXXX" 的日志
		// 如果超时 → 服务器无响应（协议号不可用或数据格式不对）
		// 如果未超时返回 nil → 连接/心跳类型（不太可能）
		// 如果返回 error "通讯类型未解析" → 服务器确实返回了数据，协议号有效！
		result, err := c.SendFrame(f)
		if err != nil {
			errMsg := err.Error()
			if errMsg == "timeout" || errMsg == "wait timeout" {
				fmt.Println("  超时 - 服务器无响应，协议号可能不可用或请求数据格式不正确")
			} else {
				// 有错误但不是超时，说明服务器有返回，只是本地无法解析
				fmt.Printf("  响应错误: %v\n", err)
				fmt.Println("  >>> 服务器有响应返回，协议号有效（本地暂无解码器）<<<")
			}
		} else {
			if result == nil {
				fmt.Println("  返回 nil")
			} else {
				// 如果能走到这里，说明 handlerDealMessage 成功解析了
				fmt.Printf("  成功解析: type=%T\n", result)
				dumpData(result)
			}
		}
		fmt.Println()
	}

	// 对照组：已知有效的行情
	fmt.Println("[对照] TypeQuote 行情接口 - 已知有效")
	_, err = c.GetQuote("sz000001")
	if err != nil {
		fmt.Printf("  行情失败: %v\n", err)
	} else {
		fmt.Println("  >>> 行情接口正常，对照通过 <<<")
	}
}

func dumpData(v interface{}) {
	switch data := v.(type) {
	case []byte:
		if len(data) > 128 {
			data = data[:128]
		}
		fmt.Printf("  数据(hex): %s\n", hex.EncodeToString(data))
	default:
		// 尝试获取原始字节
		bs := conv.Bytes(v)
		if len(bs) > 0 {
			if len(bs) > 128 {
				bs = bs[:128]
			}
			fmt.Printf("  数据: %s\n", hex.EncodeToString(bs))
		}
	}
}
