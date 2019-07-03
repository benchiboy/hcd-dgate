// rcs_contract_mgr project main.go
package main

import (
	"bufio"
	"bytes"
	"encoding/binary"

	//	"encoding/hex"
	"encoding/json"
	"fmt"

	//	"hash/crc32"

	//	"io"
	"net"
)

//整形转换成字节
func IntToBytes(n int) []byte {
	x := int32(n)
	bytesBuffer := bytes.NewBuffer([]byte{})
	binary.Write(bytesBuffer, binary.BigEndian, x)
	return bytesBuffer.Bytes()
}

//字节转换成整形
func BytesToInt(b []byte) int32 {
	bytesBuffer := bytes.NewBuffer(b)
	var x int32
	binary.Read(bytesBuffer, binary.BigEndian, &x)

	return int32(x)
}

func ProcPacket(buf []byte) {
	fmt.Println(string(buf))
	var online Online
	json.Unmarshal(buf, &online)
	fmt.Println(online)
}

func tcpPipe(conn *net.TCPConn) {
	ipStr := conn.RemoteAddr().String()
	defer func() {
		fmt.Println("disconnected :" + ipStr)
		conn.Close()
	}()
	reader := bufio.NewReader(conn)
	packBuf := make([]byte, 1024)
	var nSum int32
	for {
		readBuf := make([]byte, 1024)
		var nLen int
		nLen, err := reader.Read(readBuf)
		if err != nil {
			fmt.Println(err)
			return
		}
		nSum = nSum + int32(nLen)
		if nSum < 10 {
			copy(packBuf[nSum:], readBuf[0:nLen])
			fmt.Println("<<6", string(packBuf), nSum)
			continue
		}
		fmt.Println(">6", string(packBuf), nSum)
		packLen := BytesToInt(packBuf[2:6])
		if nSum >= packLen {
			ProcPacket(packBuf[0:packLen])
			copy(packBuf, packBuf[nSum:])
			nSum = nSum - packLen
		}
	}
}

func main() {
	fmt.Println("hello")

	//fmt.Println(hex([]byte("F")))
	//fmt.Println(hex.EncodeToString([]byte("F")))

	//	check_str := "ABC"
	//	hash.write(check_str)
	//	hash.Sum()
	//	ieee := crc32.NewIEEE()

	//	fmt.Printf("%X", crc32.ChecksumIEEE([]byte(check_str)))
	//	io.WriteString(ieee, check_str)
	//s := ieee.Sum32()
	//fmt.Println(s)
	var tcpAddr *net.TCPAddr
	tcpAddr, _ = net.ResolveTCPAddr("tcp", ":8089")
	tcpListener, _ := net.ListenTCP("tcp", tcpAddr)
	defer tcpListener.Close()
	for {
		tcpConn, err := tcpListener.AcceptTCP()
		if err != nil {
			continue
		}
		fmt.Println("A client connected : " + tcpConn.RemoteAddr().String())
		go tcpPipe(tcpConn)
	}
}
