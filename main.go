// rcs_contract_mgr project main.go
package main

import (
	"bufio"

	"encoding/hex"
	"encoding/json"
	"fmt"

	"hash/crc32"
	"net"
	"net/http"
	"sync"
	"time"
)

var (
	http_srv   *http.Server
	dbUrl      string
	ccdbUrl    string
	listenPort int
	idleConns  int
	openConns  int
	GConnMap   = &sync.Map{}
)

var heartCnt int

func Send_Resp(conn *net.TCPConn, resp string) {
	head := make([]byte, 6)
	head[0] = 0x7E
	head[1] = 0x13
	packLenBytes := IntToBytes(len(resp) + 6)
	copy(head[2:], packLenBytes)
	head = append(head, []byte(resp)...)
	n, err := conn.Write([]byte(head))
	fmt.Println("====>Send_Resp======>", n, string([]byte(head)), err)
}

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
	heartCnt = heartCnt + 1
	Send_Resp(conn, string(heartBuf))

}

func Cmd_OnLine(conn *net.TCPConn, online Online) {
	fmt.Println("Cmd_OnLine======>", online)
	var onlineResp OnlineResp
	onlineResp.Method = online.Method
	onlineResp.Sn = online.Devices[0].Sn
	onlineResp.Success = true
	onlineResp.Chip_id = online.Devices[0].Chip_id
	onlineBuf, err := json.Marshal(&onlineResp)
	if err != nil {
		fmt.Println(err)
	}
	//存储客服端的链接
	fmt.Println("online===", conn)
	GConnMap.Store(online.Devices[0].Sn, conn)

	Send_Resp(conn, string(onlineBuf))

	time.Sleep(time.Second * 1)
	fmt.Println("wait.....")

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

func Cmd_GetFileResp(getFileResp GetFileResp) {
	fmt.Println("Cmd_GetFileResp======>", getFileResp)
}

func Cmd_PostFileInfo(conn *net.TCPConn, posFileInfo PostFileInfo) {
	fmt.Println("Cmd_PostFileInfo======>", posFileInfo)
	var infoResp PostFileInfoResp
	infoResp.Method = posFileInfo.Method
	infoResp.Sn = posFileInfo.Sn
	infoResp.Success = true
	infoResp.Chip_id = posFileInfo.Chip_id
	infoResp.File_in_procesing = 1
	infoResp.Total_file = posFileInfo.Total_file
	infoBuf, err := json.Marshal(&infoResp)
	if err != nil {
		fmt.Println(err)
	}
	Send_Resp(conn, string(infoBuf))
}

func Cmd_PostFile(conn *net.TCPConn, postFile PostFile) {
	fmt.Println("Cmd_PostFile======>", postFile)
	fileBuf, err := hex.DecodeString(postFile.Fragment.Source)
	if err != nil {
		fmt.Println(err)
	}
	crcCode := crc32.ChecksumIEEE([]byte(fileBuf))
	fmt.Println("crc code==>", crcCode)
	fmt.Printf("crc code==>:%X", crcCode)
	if postFile.Fragment.Checksum != crcCode {
		fmt.Println("CRC CHECK ERROR")
	}
	fmt.Println(string(fileBuf))
	var fResp PostFileResp
	fResp.Method = postFile.Method
	fResp.Sn = postFile.Sn
	fResp.Success = true
	fResp.Chip_id = postFile.Chip_id
	fBuf, err := json.Marshal(&fResp)
	if err != nil {
		fmt.Println(err)
	}
	Send_Resp(conn, string(fBuf))
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
		err := json.Unmarshal(packBuf, &online)
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println("online===", conn)
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
	case GET_FILE_RESP:
		var getFileResp GetFileResp
		err := json.Unmarshal(packBuf, &getFileResp)
		if err != nil {
			fmt.Println(err)
		}
		Cmd_GetFileResp(getFileResp)
	case POST_FILE_INFO:
		var postFileInfo PostFileInfo
		err := json.Unmarshal(packBuf, &postFileInfo)
		if err != nil {
			fmt.Println(err)
		}
		Cmd_PostFileInfo(conn, postFileInfo)
	case POST_FILE:
		var postFile PostFile
		json.Unmarshal(packBuf, &postFile)
		Cmd_PostFile(conn, postFile)
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
	packBuf := make([]byte, 8000)
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
		for {
			packLen := BytesToInt(packBuf[2:6])
			fmt.Println("Packet Len===>", packLen)
			if nSum >= packLen {
				ProcPacket(conn, packBuf[6:packLen])
				nSum = nSum - packLen

				fmt.Println("nSum===>", nSum)
				if nSum > 0 {
					copy(packBuf, packBuf[packLen:])
				}
			} else {
				break
			}
		}
	}
}

func go_WebServer() {
	fmt.Println("HttpServer start....")
	http.HandleFunc("/getfile", GetFileControl)

	http_srv = &http.Server{
		Addr: ":7088",
	}
	if err := http_srv.ListenAndServe(); err != nil {
		fmt.Printf("listen: %s\n", err)
	}
}
func main() {

	go go_WebServer()
	fmt.Println(".................")

	var tcpAddr *net.TCPAddr
	tcpAddr, _ = net.ResolveTCPAddr("tcp", ":8089")
	tcpListener, _ := net.ListenTCP("tcp", tcpAddr)
	defer tcpListener.Close()
	for {
		//fmt.Println("Start accept1111.....")
		tcpConn, err := tcpListener.AcceptTCP()
		if err != nil {
			continue
		}
		fmt.Println("A client connected : " + tcpConn.RemoteAddr().String())
		go tcpPipe(tcpConn)
	}

	//1671877088
	//521391064 65 4e554d4245522c4c4f542c524553554c542c444154450a312c3139363636342ce6a0a1e58786e9809ae8bf872c323031392d30352d32382031303a31333a30330a
	//	check_str := "4e554d4245522c4c4f542c524553554c542c444154450a312c3139363636342ce6a0a1e58786e9809ae8bf872c323031392d30352d32382031303a31333a30330a"
	//	a := crc32.ChecksumIEEE([]byte(check_str))
	//	fmt.Println("crc32==", a)
	//	buf, _ := hex.DecodeString(check_str)
	//	fmt.Println(string(buf))
	//	b := crc32.ChecksumIEEE(buf)
	//	fmt.Println("crc32==", b)
}
