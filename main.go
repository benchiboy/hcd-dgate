// rcs_contract_mgr project main.go
package main

import (
	"bufio"

	"encoding/hex"
	"encoding/json"

	"fmt"

	"io"
	"net"
	"net/http"

	"flag"
	"hcd-dgate/service/dbcomm"
	"hcd-dgate/service/device"
	"log"
	"os"
	"sync"
	"time"

	goconf "github.com/pantsing/goconf"

	"gopkg.in/natefinch/lumberjack.v2"
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
	log.Println("====>Send_Resp======>", n, string([]byte(head)), err)
}

/*
	接收设备的心跳，完成功能如下：
	1、更新当前设备连接的在线时间戳
*/
func Cmd_HeartBeat(conn *net.TCPConn, heart Heartbeat) {
	log.Println("Cmd_HeartBeat======>", heart)
	var heartResp HeartbeatResp
	heartResp.Chip_id = heart.Chip_id
	heartResp.Method = heart.Method
	heartResp.Sn = heart.Sn
	heartResp.Success = true
	heartBuf, err := json.Marshal(&heartResp)
	if err != nil {
		log.Println(err)
	}
	Send_Resp(conn, string(heartBuf))
}

/*
	接收设备在线登录，完成功能如下：
	1、把设备的当前连接保存在全局MAP
	2、记录在线的时间戳，以便定时检测是否在线
	3、更新数据库的在线状态
*/
func Cmd_OnLine(conn *net.TCPConn, online Online) {
	PrintHead("Online")
	var onlineResp OnlineResp
	onlineResp.Method = online.Method
	onlineResp.Sn = online.Devices[0].Sn
	onlineResp.Success = true
	onlineResp.Chip_id = online.Devices[0].Chip_id
	onlineBuf, err := json.Marshal(&onlineResp)
	if err != nil {
		log.Println(err)
	}
	//检测设备是否存在，如果不存在，插入；否则更新
	r := device.New(dbcomm.GetDB(), device.DEBUG)
	var search device.Search
	search.Sn = onlineResp.Sn
	if e, err := r.Get(search); err == nil {
		onlineMap := map[string]interface{}{UPDATE_TIME: time.Now().Format("2006-01-02 15:04:05"),
			IS_ONLINE: 1}
		err = r.UpdateMap(fmt.Sprintf("%d", e.Id), onlineMap, nil)
		if err != nil {
			log.Println("更新失败", err)
		}

	} else {
		var e device.Device
		e.Sn = online.Devices[0].Sn
		e.ChipId = online.Devices[0].Chip_id
		e.CreateTime = time.Now().Format("2006-01-02 15:04:05")
		r.InsertEntity(e, nil)
	}
	//存储客服端的链接
	GConnMap.Store(online.Devices[0].Sn, conn)
	Send_Resp(conn, string(onlineBuf))

	PrintTail("Online")
}

func Cmd_GetColoPhonResp(conn *net.TCPConn, coloPhon GetColophon) {
	log.Println("Cmd_GetColoPhonResp======>", coloPhon)
	var coloPhonResp GetColophonResp
	coloPhonResp.Method = coloPhon.Method
	coloPhonResp.Sn = coloPhon.Sn
	coloPhonResp.Success = true
}

func Cmd_GetInstallDriveResp(conn *net.TCPConn, getDriveResp GetInstallDataDriveResp) {
	PrintHead("GetInstallDrive", getDriveResp)
	PrintTail("GetInstallDrive")
}

func Cmd_PostInstallDrive(conn *net.TCPConn, postInstDrive PostInstallDataDrive) {
	PrintHead("PostDataDrive")
	var driveResp PostInstallDataDriveResp
	driveResp.Method = postInstDrive.Method
	driveResp.Sn = postInstDrive.Sn
	driveResp.Success = true
	respBuf, err := json.Marshal(&driveResp)
	if err != nil {
		log.Println(err)
	}
	Send_Resp(conn, string(respBuf))
	PrintTail("PostDataDrive")
}

func Cmd_GetFileResp(getFileResp GetFileResp) {
	log.Println("Cmd_GetFileResp======>", getFileResp)
}

func Cmd_PostFileInfo(conn *net.TCPConn, posFileInfo PostFileInfo) {
	log.Println("Cmd_PostFileInfo======>", posFileInfo)
	var infoResp PostFileInfoResp
	infoResp.Method = posFileInfo.Method
	infoResp.Sn = posFileInfo.Sn
	infoResp.Success = true
	infoResp.Chip_id = posFileInfo.Chip_id
	infoResp.File_in_procesing = 1
	infoResp.Total_file = posFileInfo.Total_file
	infoBuf, err := json.Marshal(&infoResp)
	if err != nil {
		log.Println(err)
	}
	Send_Resp(conn, string(infoBuf))
}

func Cmd_PostFile(conn *net.TCPConn, postFile PostFile) {
	log.Println("Cmd_PostFile======>", postFile)
	fileBuf, err := hex.DecodeString(postFile.Fragment.Source)
	if err != nil {
		log.Println(err)
	}
	crcCode := softwareCrc32([]byte(fileBuf), len(fileBuf))
	log.Println("crc code==>", crcCode)

	if postFile.Fragment.Checksum != crcCode {
		log.Println("CRC CHECK ERROR")
	}
	var fResp PostFileResp
	fResp.Method = postFile.Method
	fResp.Sn = postFile.Sn
	fResp.Success = true
	fResp.Chip_id = postFile.Chip_id
	fBuf, err := json.Marshal(&fResp)
	if err != nil {
		log.Println(err)
	}
	Send_Resp(conn, string(fBuf))
}

/*
	接收客户端发来的PUSHINFO文件确认
*/
func Cmd_PushFileInfoResp(conn *net.TCPConn, infoResp PushFileInfoResp) {

	log.Println("Cmd_PushFileInfoResp======>", infoResp)
	//开始发送文件内容
	var pushFile PushFile
	pushFile.Chip_id = infoResp.Chip_id
	pushFile.Method = PUSH_FILE
	pushFile.Sn = infoResp.Sn
	pushFile.Fragment.Index = 1
	pushFile.Fragment.Length = 10
	pushFile.Fragment.Eof = true
	pushFile.Fragment.Source = "1092109210921029102910111111112"
	pushFile.Fragment.Checksum = 1000000
	fBuf, err := json.Marshal(&pushFile)
	if err != nil {
		log.Println(err)
	}
	Send_Resp(conn, string(fBuf))
}

/*
	接收客户端发来的PUSH文件确认
*/
func Cmd_PushFileResp(conn *net.TCPConn, fileResp PushFileResp) {
	log.Println("Cmd_PushFilResp======>", fileResp)

}

/*
	接收客户端发来的PUSH文件确认
*/
func Cmd_PushInfoResp(conn *net.TCPConn, infoResp PushInfoResp) {
	log.Println("Cmd_PushInfoResp======>", infoResp)

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
		if err := json.Unmarshal(packBuf, &online); err != nil {
			log.Println(err)
		}
		Cmd_OnLine(conn, online)
	case GET_COLOPHON_RESP:
		var coloPhon GetColophon
		if err := json.Unmarshal(packBuf, &coloPhon); err != nil {
			log.Println(err)
		}
		Cmd_GetColoPhonResp(conn, coloPhon)
	case GET_INSTLL_DATADRIVE_RESP:
		var getInstDrive GetInstallDataDriveResp
		if err := json.Unmarshal(packBuf, &getInstDrive); err != nil {
			log.Println(err)
		}
		Cmd_GetInstallDriveResp(conn, getInstDrive)
	case POST_INSTLL_DATADRIVE:
		var postInstDrive PostInstallDataDrive
		if err := json.Unmarshal(packBuf, &postInstDrive); err != nil {
			log.Println(err)
		}
		Cmd_PostInstallDrive(conn, postInstDrive)
	case GET_FILE_RESP:
		var getFileResp GetFileResp
		err := json.Unmarshal(packBuf, &getFileResp)
		if err != nil {
			log.Println(err)
		}
		Cmd_GetFileResp(getFileResp)

	case POST_FILE_INFO:
		var postFileInfo PostFileInfo
		err := json.Unmarshal(packBuf, &postFileInfo)
		if err != nil {
			log.Println(err)
		}
		Cmd_PostFileInfo(conn, postFileInfo)
	case POST_FILE:
		var postFile PostFile
		json.Unmarshal(packBuf, &postFile)
		Cmd_PostFile(conn, postFile)

	case PUSH_FILE_INFO_RESP:
		var infoResp PushFileInfoResp
		json.Unmarshal(packBuf, &infoResp)
		Cmd_PushFileInfoResp(conn, infoResp)

	case PUSH_FILE_RESP:
		var fileResp PushFileResp
		json.Unmarshal(packBuf, &fileResp)
		Cmd_PushFileResp(conn, fileResp)

	case PUSH_INFO_RESP:
		var infoResp PushInfoResp
		json.Unmarshal(packBuf, &infoResp)
		Cmd_PushInfoResp(conn, infoResp)
	}
}

func tcpPipe(conn *net.TCPConn) {
	ipStr := conn.RemoteAddr().String()
	defer func() {
		log.Println("disconnected :" + ipStr)
		conn.Close()
	}()

	reader := bufio.NewReader(conn)
	packBuf := make([]byte, 8000)
	var nSum int32
	for {
		readBuf := make([]byte, 1024)
		var nLen int
		nLen, err := reader.Read(readBuf)
		log.Println("Recv Len==", nLen, string(readBuf[0:nLen]))
		if err != nil || nLen <= 0 {
			log.Println(err)
			return
		}
		copy(packBuf[nSum:], readBuf[0:nLen])
		nSum = nSum + int32(nLen)
		if nSum < HEAD_LEN {
			continue
		}
		for {
			packLen := BytesToInt(packBuf[2:6])
			log.Println("Packet Len===>", packLen)
			if nSum >= packLen {
				ProcPacket(conn, packBuf[6:packLen])
				nSum = nSum - packLen

				log.Println("nSum===>", nSum)
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
	log.Println("........HttpServer start.........")
	http.HandleFunc("/dgate/busiGetFile", GetFileControl)
	http.HandleFunc("/dgate/busiPushFile", PushFileControl)
	http.HandleFunc("/dgate/busiGetVersions", GetVerListControl)
	http.HandleFunc("/dgate/busiGetDataDrives", GetDataDriveControl)
	http.HandleFunc("/dgate/busiPushInfo", PushInfoControl)
	http.HandleFunc("/dgate/busiQueryStatus", QueryStatusControl)

	http_srv = &http.Server{
		Addr: ":7088",
	}
	if err := http_srv.ListenAndServe(); err != nil {
		log.Printf("listen: %s\n", err)
	}
}

func init() {
	log.Println("System Paras Init......")
	log.SetFlags(log.Ldate | log.Lshortfile | log.Lmicroseconds)
	log.SetOutput(io.MultiWriter(os.Stdout, &lumberjack.Logger{
		Filename:   "jcd.log",
		MaxSize:    500, // megabytes
		MaxBackups: 50,
		MaxAge:     90, //days
	}))
	envConf := flag.String("env", "config-ci.json", "select a environment config file")
	flag.Parse()
	log.Println("config file ==", *envConf)
	c, err := goconf.New(*envConf)
	if err != nil {
		log.Fatalln("读配置文件出错", err)
	}

	//填充配置文件
	c.Get("/config/LISTEN_PORT", &listenPort)
	c.Get("/config/DB_URL", &dbUrl)
	c.Get("/config/CCDB_URL", &ccdbUrl)
	c.Get("/config/OPEN_CONNS", &openConns)
	c.Get("/config/IDLE_CONNS", &idleConns)

}

func main() {

	dbcomm.InitDB(dbUrl, ccdbUrl, idleConns, openConns)
	log.Println("MicroPoint Device Gate Starting....")
	log.Println("	V0.1    ")

	go go_WebServer()
	var tcpAddr *net.TCPAddr
	tcpAddr, _ = net.ResolveTCPAddr("tcp", ":8089")
	tcpListener, _ := net.ListenTCP("tcp", tcpAddr)
	defer tcpListener.Close()
	for {
		tcpConn, err := tcpListener.AcceptTCP()
		if err != nil {
			continue
		}
		log.Println("A client connected : " + tcpConn.RemoteAddr().String())
		go tcpPipe(tcpConn)
	}

}
