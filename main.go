// rcs_contract_mgr project main.go
package main

import (
	"bufio"

	//	"encoding/hex"
	"encoding/json"
	"fmt"

	//	"hash/crc32"

	//	"io"
	"net"
)

func Cmd_HeartBeat(conn *net.TCPConn, heart Heartbeat) {
	fmt.Println("Cmd_HeartBeat======>", heart)
	var heartResp HeartbeatResp
	heartResp.Chip_id = heart.Chip_id
	heartResp.Method = heart.Method
	heartResp.Sn = heart.Sn
	heartResp.Success = true
	heartBuf, err := json.Marshal(&heartResp)
	if err != nil {
		fmt.Println(err)
	}
	len, err := conn.Write(heartBuf)
	fmt.Println(len)
}

func Cmd_OnLine(conn *net.TCPConn, online Online) {
	fmt.Println("Cmd_OnLine======>", online)
	var onlineResp OnlineResp
	onlineResp.Method = online.Method
	onlineResp.Sn = online.Sn
	onlineResp.Success = true
}

func Cmd_GetColoPhon(conn *net.TCPConn, coloPhon GetColophon) {
	fmt.Println("Cmd_HeartBeat======>", coloPhon)
	var coloPhonResp GetColophonResp
	coloPhonResp.Method = coloPhon.Method
	coloPhonResp.Sn = coloPhon.Sn
	coloPhonResp.Success = true
}

func Cmd_GetInstallDrive(conn *net.TCPConn, getInstDrive GetInstallDataDrive) {
	fmt.Println("Cmd_GetColoPhon======>", getInstDrive)
	var getInstDataDriveResp GetColophonResp
	getInstDataDriveResp.Method = getInstDrive.Method
	getInstDataDriveResp.Sn = getInstDrive.Sn
	getInstDataDriveResp.Success = true

}

func Cmd_PostInstallDrive(conn *net.TCPConn, postInstDrive PostInstallDataDrive) {
	fmt.Println("Cmd_PostInstallDrive======>", postInstDrive)
	var postInstDataDriveResp GetColophonResp
	postInstDataDriveResp.Method = postInstDrive.Method
	postInstDataDriveResp.Sn = postInstDrive.Sn
	postInstDataDriveResp.Success = true
}

func Cmd_GetFile(getFile GetFile) {
	fmt.Println("Cmd_GetFile======>", getFile)
}

func Cmd_PostFileInfo(posFileInfo PostFileInfo) {
	fmt.Println("Cmd_PostFileInfo======>", posFileInfo)
}

func Cmd_PostFile(postFile PostFile) {
	fmt.Println("Cmd_PostFile======>", postFile)
}
func Cmd_PushFileInfo(pushFileInfo PushFileInfo) {
	fmt.Println("Cmd_PushFileInfo======>", pushFileInfo)
}

func Cmd_PushFile(pushFile PushFile) {
	fmt.Println("Cmd_PushFile======>", pushFile)
}

func ProcPacket(conn *net.TCPConn, packBuf []byte) {
	var command Command
	json.Unmarshal(packBuf, &command)
	switch command.Method {
	case HEARTBEAT:
		var heart Heartbeat
		json.Unmarshal(packBuf, &heart)
		Cmd_HeartBeat(conn, heart)
	case ONLINE:
		var online Online
		json.Unmarshal(packBuf, &online)
		Cmd_OnLine(conn, online)
	case GET_COLOPHON:
		var coloPhon GetColophon
		json.Unmarshal(packBuf, &coloPhon)
		Cmd_GetColoPhon(conn, coloPhon)
	case GET_INSTLL_DATADRIVE:
		var getInstDrive GetInstallDataDrive
		json.Unmarshal(packBuf, &getInstDrive)
		Cmd_GetInstallDrive(conn, getInstDrive)
	case POST_INSTLL_DATADRIVE:
		var postInstDrive PostInstallDataDrive
		json.Unmarshal(packBuf, &postInstDrive)
		Cmd_PostInstallDrive(conn, postInstDrive)
	case GET_FILE:
		var getFile GetFile
		json.Unmarshal(packBuf, &getFile)
		Cmd_GetFile(getFile)
	case POST_FILE_INFO:
		var postFileInfo PostFileInfo
		json.Unmarshal(packBuf, &postFileInfo)
		Cmd_PostFileInfo(postFileInfo)
	case POST_FILE:
		var postFile PostFile
		json.Unmarshal(packBuf, &postFile)
		Cmd_PostFile(postFile)

	case PUSH_FILE_INFO:
		var pushFileInfo PushFileInfo
		json.Unmarshal(packBuf, &pushFileInfo)
		Cmd_PushFileInfo(pushFileInfo)
	case PUSH_FILE:
		var pushFile PushFile
		json.Unmarshal(packBuf, &pushFile)
		Cmd_PushFile(pushFile)
	}
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
		fmt.Println("Recv Len==", nLen, string(readBuf[0:nLen]))
		if err != nil || nLen <= 0 {
			fmt.Println(err)
			return
		}
		copy(packBuf[nSum:], readBuf[0:nLen])
		nSum = nSum + int32(nLen)
		if nSum < HEAD_LEN {
			continue
		}
		packLen := BytesToInt(packBuf[2:6])
		if nSum >= packLen {
			ProcPacket(conn, packBuf[6:packLen])
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
	//	buf := []byte("123456789")
	//	fmt.Println(string(buf))
	//	copy(buf[0:], []byte("fffff"))
	//	fmt.Println(string(buf))

}
