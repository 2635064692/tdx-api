package main

import (
	"bufio"
	"bytes"
	"compress/zlib"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	"github.com/injoyai/tdx/protocol"
)

// 直接用 TCP 连接验证基本面协议号，绕过 Client 的高层封装
// 避免 handlerDealMessage 的 default 分支导致超时

var hosts = []string{
	"119.147.212.81:7709",
	"112.74.214.43:7727",
	"221.231.141.60:7709",
}

func main() {
	fmt.Println("===== TDX 基本面协议号验证 =====")
	fmt.Println()

	// 1. 建立TCP连接
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
	fmt.Println()

	reader := bufio.NewReader(conn)

	// 2. 发送连接帧(建立连接)
	fmt.Println("--- 建立连接 ---")
	connectFrame := &protocol.Frame{
		Control: 0x01,
		Type:    protocol.TypeConnect,
		Data:    []byte{0x01},
	}
	if err := writeFrame(conn, connectFrame); err != nil {
		fmt.Printf("发送连接帧失败: %v\n", err)
		os.Exit(1)
	}
	resp, err := readResponse(reader)
	if err != nil {
		fmt.Printf("读取连接响应失败: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("连接响应: Control=0x%02X, Type=0x%04X, DataLen=%d\n", resp.Control, resp.Type, len(resp.Data))
	fmt.Println()

	// 3. 测试各基本面协议号
	code := "000001" // 平安银行
	market := byte(0x00) // 深圳

	tests := []struct {
		name    string
		typeVal uint16
		data    []byte
	}{
		{
			name:    "0x000f 除权除息(XDXR)",
			typeVal: 0x000f,
			// 格式: market(1) + padding(1) + code(6)
			data: []byte{market, 0x00, '0', '0', '0', '0', '0', '1'},
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

	conn.SetDeadline(time.Now().Add(10 * time.Second))
	defer conn.SetDeadline(time.Time{})

	for i, t := range tests {
		fmt.Printf("[%d] 测试 %s (code=%s)\n", i+1, t.name, code)

		f := &protocol.Frame{
			Control: 0x01,
			Type:    t.typeVal,
			Data:    t.data,
		}

		if err := writeFrame(conn, f); err != nil {
			fmt.Printf("  发送失败: %v\n\n", err)
			continue
		}
		fmt.Printf("  已发送帧, Type=0x%04X, DataLen=%d\n", t.typeVal, len(t.data))

		resp, err := readResponse(reader)
		if err != nil {
			fmt.Printf("  读取响应失败: %v\n", err)
			fmt.Println("  >>> 协议号可能不可用或数据格式不正确 <<<")
			fmt.Println()
			continue
		}

		fmt.Printf("  响应: Control=0x%02X, Type=0x%04X, ZipLen=%d, UnzipLen=%d, DataLen=%d\n",
			resp.Control, resp.Type, resp.ZipLength, resp.Length, len(resp.Data))

		// 打印前128字节的hex dump
		dumpLen := len(resp.Data)
		if dumpLen > 128 {
			dumpLen = 128
		}
		if dumpLen > 0 {
			fmt.Printf("  数据前%d字节:\n%s\n", dumpLen, hex.Dump(resp.Data[:dumpLen]))

			// 尝试以文本形式显示（GBK解码）
			if text := tryGBK(resp.Data); text != "" {
				fmt.Printf("  文本内容(前200字): %s\n", truncate(text, 200))
			}
		}

		// 判断结果
		if resp.Control&0x10 != 0 {
			fmt.Println("  >>> 协议号有效，服务器返回了数据 <<<")
		} else {
			fmt.Println("  >>> 服务器返回了响应但可能标记为错误 <<<")
		}
		fmt.Println()
	}

	// 对比: 也测试一个已知有效的协议号(行情)作为对照
	fmt.Println("[对照] 测试 0x053E 行情(QUOTE) - 已知有效")
	quoteFrame := &protocol.Frame{
		Control: 0x01,
		Type:    protocol.TypeQuote,
		Data: []byte{
			0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 固定头
			0x01, 0x00, // 数量=1 (小端)
			0x00,             // 深圳
			'0', '0', '0', '0', '0', '1', // 000001
		},
	}
	if err := writeFrame(conn, quoteFrame); err != nil {
		fmt.Printf("  发送失败: %v\n", err)
	} else {
		resp, err := readResponse(reader)
		if err != nil {
			fmt.Printf("  读取失败: %v\n", err)
		} else {
			fmt.Printf("  响应: Control=0x%02X, Type=0x%04X, DataLen=%d\n",
				resp.Control, resp.Type, len(resp.Data))
			fmt.Println("  >>> 行情接口正常，对照通过 <<<")
		}
	}
}

func writeFrame(conn net.Conn, f *protocol.Frame) error {
	// 直接用 protocol.Frame.Bytes() 生成帧
	// 但需要手动设置 MsgID（这里简单用递增）
	// 由于 Frame.Bytes() 需要 MsgID，我们手动构造
	length := uint16(len(f.Data) + 2)
	data := make([]byte, 12+len(f.Data))
	data[0] = 0x0C // Prefix
	// MsgID = 递增（简单用固定值）
	copy(data[1:], []byte{0x01, 0x00, 0x00, 0x00}) // MsgID=1
	data[5] = byte(f.Control)
	copy(data[6:], protocol.Bytes(length))
	copy(data[8:], protocol.Bytes(length))
	copy(data[10:], protocol.Bytes(f.Type))
	copy(data[12:], f.Data)

	_, err := conn.Write(data)
	return err
}

type rawResponse struct {
	Control   uint8
	Type      uint16
	ZipLength uint16
	Length    uint16
	Data      []byte
}

func readResponse(reader *bufio.Reader) (*rawResponse, error) {
	// 读取帧头 b1cb7400
	prefix := make([]byte, 4)
	if _, err := io.ReadFull(reader, prefix); err != nil {
		return nil, fmt.Errorf("读取帧头失败: %w", err)
	}
	if protocol.Uint32(prefix) != 0xB1CB7400 {
		return nil, fmt.Errorf("帧头不匹配: %X", prefix)
	}

	// 读取12字节
	buf := make([]byte, 12)
	if _, err := io.ReadFull(reader, buf); err != nil {
		return nil, fmt.Errorf("读取头部失败: %w", err)
	}

	resp := &rawResponse{
		Control:   buf[0],
		Type:      protocol.Uint16(buf[2:4]),
		ZipLength: protocol.Uint16(buf[4:6]),
		Length:    protocol.Uint16(buf[6:8]),
	}

	// 读取数据
	data := make([]byte, resp.ZipLength)
	if _, err := io.ReadFull(reader, data); err != nil {
		return nil, fmt.Errorf("读取数据失败: %w", err)
	}

	// 解压
	if resp.ZipLength != resp.Length {
		r, err := zlib.NewReader(bytes.NewReader(data))
		if err != nil {
			resp.Data = data // 解压失败就返回原始数据
			return resp, nil
		}
		defer r.Close()
		resp.Data, _ = io.ReadAll(r)
	} else {
		resp.Data = data
	}

	return resp, nil
}

func tryGBK(data []byte) string {
	// 简单检测是否包含可读文本
	result := protocol.UTF8ToGBK(data)
	// 过滤不可打印字符
	clean := make([]byte, 0, len(result))
	for _, b := range result {
		if b >= 0x20 && b < 0x7F || b >= 0x80 { // 可打印ASCII或中文
			clean = append(clean, b)
		}
	}
	return string(clean)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
