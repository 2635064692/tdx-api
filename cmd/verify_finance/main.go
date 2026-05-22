package main

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/injoyai/tdx/protocol"
)

// 直接用 TCP 连接验证基本面协议号
// 复用 protocol.ReadFrom + protocol.Decode 进行帧解析

var hosts = []string{
	"119.147.212.81:7709",
	"112.74.214.43:7727",
	"221.231.141.60:7709",
	"218.75.126.9:7709",
	"115.238.56.198:7709",
	"124.160.88.183:7709",
}

func main() {
	fmt.Println("===== TDX 基本面协议号验证 =====")
	fmt.Println()

	// 1. 建立 TCP 连接
	var conn net.Conn
	var err error
	for _, addr := range hosts {
		fmt.Printf("尝试连接 %s ...", addr)
		conn, err = net.DialTimeout("tcp", addr, 5*time.Second)
		if err == nil {
			fmt.Println(" 成功")
			break
		}
		fmt.Printf(" 失败: %v\n", err)
	}
	if conn == nil {
		fmt.Println("所有服务器连接失败")
		os.Exit(1)
	}
	defer conn.Close()
	fmt.Printf("已连接: %s\n\n", conn.RemoteAddr())

	reader := bufio.NewReader(conn)
	conn.SetDeadline(time.Now().Add(30 * time.Second))

	// 2. 发送连接帧
	fmt.Println("--- 建立连接 ---")
	connectFrame := &protocol.Frame{
		MsgID:   1,
		Control: 0x01,
		Type:    protocol.TypeConnect,
		Data:    []byte{0x01},
	}
	if _, err := conn.Write(connectFrame.Bytes()); err != nil {
		fmt.Printf("发送连接帧失败: %v\n", err)
		os.Exit(1)
	}

	raw, err := protocol.ReadFrom(reader)
	if err != nil {
		fmt.Printf("读取连接响应失败: %v\n", err)
		os.Exit(1)
	}
	resp, err := protocol.Decode(raw)
	if err != nil {
		fmt.Printf("解析连接响应失败: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("连接成功: Control=0x%02X, Type=0x%04X, DataLen=%d\n\n", resp.Control, resp.Type, len(resp.Data))

	// 3. 测试各基本面协议号
	code := "000001"
	market := byte(0x00) // 深圳

	tests := []struct {
		name    string
		typeVal uint16
		data    []byte
	}{
		{
			name:    "0x000f 除权除息(XDXR)",
			typeVal: 0x000f,
			data:    []byte{market, 0x00, '0', '0', '0', '0', '0', '1'},
		},
		{
			name:    "0x0010 财务数据(FINANCE)",
			typeVal: 0x0010,
			data:    []byte{market, 0x00, '0', '0', '0', '0', '0', '1'},
		},
		{
			name:    "0x02cf 公司信息文件目录",
			typeVal: 0x02cf,
			data:    []byte{market, 0x00, '0', '0', '0', '0', '0', '1'},
		},
		{
			name:    "0x02d0 公司信息内容(F10)",
			typeVal: 0x02d0,
			data:    []byte{market, 0x00, '0', '0', '0', '0', '0', '1'},
		},
	}

	msgID := uint32(3)
	for i, t := range tests {
		fmt.Printf("[%d] 测试 %s (code=%s)\n", i+1, t.name, code)

		f := &protocol.Frame{
			MsgID:   msgID,
			Control: 0x01,
			Type:    t.typeVal,
			Data:    t.data,
		}
		msgID++

		if _, err := conn.Write(f.Bytes()); err != nil {
			fmt.Printf("  发送失败: %v\n\n", err)
			continue
		}
		fmt.Printf("  已发送, Type=0x%04X\n", t.typeVal)

		// 设置单次读取超时
		conn.SetDeadline(time.Now().Add(5 * time.Second))
		raw, err := protocol.ReadFrom(reader)
		conn.SetDeadline(time.Now().Add(30 * time.Second))

		if err != nil {
			fmt.Printf("  读取响应失败: %v\n", err)
			fmt.Println("  >>> 协议号可能不可用或数据格式不正确 <<<")
			fmt.Println()
			continue
		}

		resp, err := protocol.Decode(raw)
		if err != nil {
			fmt.Printf("  解码失败: %v\n", err)
			show := raw
			if len(show) > 64 {
				show = show[:64]
			}
			fmt.Printf("  原始数据: %s\n", hex.EncodeToString(show))
			fmt.Println("  >>> 服务器有响应，协议号有效但数据格式未知 <<<")
			fmt.Println()
			continue
		}

		fmt.Printf("  响应: Control=0x%02X, Type=0x%04X, ZipLen=%d, UnzipLen=%d, DataLen=%d\n",
			resp.Control, resp.Type, resp.ZipLength, resp.Length, len(resp.Data))

		dumpLen := len(resp.Data)
		if dumpLen > 128 {
			dumpLen = 128
		}
		if dumpLen > 0 {
			fmt.Printf("  数据前%d字节:\n%s\n", dumpLen, hex.Dump(resp.Data[:dumpLen]))

			text := string(protocol.UTF8ToGBK(resp.Data))
			clean := filterPrintable(text)
			if len(clean) > 10 {
				if len(clean) > 200 {
					clean = clean[:200] + "..."
				}
				fmt.Printf("  文本内容: %s\n", clean)
			}
		}

		if resp.Control&0x10 != 0 {
			fmt.Println("  >>> 协议号有效，服务器返回了数据 <<<")
		} else {
			fmt.Println("  >>> 服务器响应但可能标记为错误 <<<")
		}
		fmt.Println()
	}

	// 对照组
	fmt.Println("[对照] TypeQuote=0x053E 行情 - 已知有效")
	quoteFrame := &protocol.Frame{
		MsgID:   msgID,
		Control: 0x01,
		Type:    protocol.TypeQuote,
		Data: []byte{
			0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x01, 0x00,
			0x00, '0', '0', '0', '0', '0', '1',
		},
	}
	if _, err := conn.Write(quoteFrame.Bytes()); err != nil {
		fmt.Printf("  发送失败: %v\n", err)
	} else {
		raw, err := protocol.ReadFrom(reader)
		if err != nil {
			fmt.Printf("  读取失败: %v\n", err)
		} else {
			resp, _ := protocol.Decode(raw)
			if resp != nil {
				fmt.Printf("  响应: Control=0x%02X, Type=0x%04X, DataLen=%d\n",
					resp.Control, resp.Type, len(resp.Data))
				fmt.Println("  >>> 行情接口正常，对照通过 <<<")
			}
		}
	}
}

func filterPrintable(s string) string {
	result := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		b := s[i]
		if b >= 0x20 && b < 0x7F {
			result = append(result, b)
		} else if b >= 0xC0 {
			// UTF-8 多字节字符开头
			result = append(result, b)
		}
	}
	return string(result)
}
