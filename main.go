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
	"hcd-dgate/service/chip"
	"hcd-dgate/service/dbcomm"
	"hcd-dgate/service/device"
	"hcd-dgate/service/dfile"

	"hcd-dgate/service/mfile"

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
func CmdHeartBeat(conn *net.TCPConn, heart Heartbeat) {
	log.Println("CmdHeartBeat======>", heart)
	var heartResp HeartbeatResp
	heartResp.Chip_id = heart.Chip_id
	heartResp.Method = HEARTBEAT_RESP
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
func CmdOnLine(conn *net.TCPConn, online Online) {
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
	GConnMap.Store(online.Devices[0].Sn, StoreInfo{CurrConn: conn, SignInTime: time.Now()})

	Send_Resp(conn, string(onlineBuf))
	PrintTail("Online")
}

func CmdGetColoPhonResp(conn *net.TCPConn, phonResp GetColophonResp) {
	log.Println("CmdGetColoPhonResp======>", phonResp)

	currNode, _ := getCurrNode(phonResp.Sn)
	r := mfiles.New(dbcomm.GetDB(), mfiles.DEBUG)
	var ne mfiles.MFiles
	ne.EndTime = time.Now().Format("2006-01-02 15:04:05")
	currNode.Status = STATUS_INIT
	GConnMap.Store(phonResp.Sn, currNode)

	r.UpdataEntity(currNode.BatchNo, ne, nil)

}

func CmdGetInstallDriveResp(conn *net.TCPConn, getDriveResp GetInstallDataDriveResp) {
	PrintHead("GetInstallDrive", getDriveResp)
	PrintTail("GetInstallDrive")
}

/*
	接收从设备上传芯片参数信息
	1、做数据库记录
*/
func CmdPostInstallDrive(conn *net.TCPConn, postInstDrive PostInstallDataDrive) {
	PrintHead("PostDataDrive")
	var driveResp PostInstallDataDriveResp
	driveResp.Method = postInstDrive.Method
	driveResp.Sn = postInstDrive.Sn
	driveResp.Success = true
	respBuf, err := json.Marshal(&driveResp)
	if err != nil {
		log.Println(err)
	}
	//记录数据库
	r := chips.New(dbcomm.GetDB(), chips.DEBUG)
	var search chips.Search
	search.Sn = postInstDrive.Sn
	if err := r.Delete(search.Sn, nil); err != nil {
		fmt.Println(err)
	}
	var e chips.Chips
	e.Sn = postInstDrive.Sn
	for _, v := range postInstDrive.Datadrive {
		e.ChipLot = v.Lot
		e.ChipInstallDate = v.Install_time
		e.ActiveDate = v.Create_time
		e.CreateTime = time.Now().Format("2006-01-02 15:04:05")
		r.InsertEntity(e, nil)
	}
	Send_Resp(conn, string(respBuf))

	currNode, _ := getCurrNode(postInstDrive.Sn)
	currNode.Status = STATUS_INIT
	GConnMap.Store(postInstDrive.Sn, currNode)

	PrintTail("PostDataDrive")
}

/*
	接收设备获取文件指令的应答
	1、更新数据库的MFILE 表
*/
func CmdGetFileResp(fileResp GetFileResp) {
	PrintHead("GetFileResp")

	currNode, _ := getCurrNode(fileResp.Sn)
	var e mfiles.MFiles
	r := mfiles.New(dbcomm.GetDB(), mfiles.DEBUG)
	e.TodoCount = fileResp.Total_file
	r.UpdataEntity(currNode.BatchNo, e, nil)

	PrintTail("GetFileResp")
}

/*
	1
*/
func CmdPostFileInfo(conn *net.TCPConn, postFileInfo PostFileInfo) {
	PrintHead("PostFileInfo", postFileInfo)
	//记录数据库
	r := dfiles.New(dbcomm.GetDB(), dfiles.DEBUG)
	var e dfiles.DFiles
	e.FileName = postFileInfo.File.Name
	e.FileLength = postFileInfo.File.Length
	e.FileCrc32 = postFileInfo.File.File_crc
	e.FileType = postFileInfo.Type
	e.FileIndex = postFileInfo.File_in_procesing
	e.Sn = postFileInfo.Sn
	currNode, _ := getCurrNode(e.Sn)
	e.BatchNo = currNode.BatchNo
	e.ChipId = postFileInfo.Chip_id
	e.BeginTime = time.Now().Format("2006-01-02 15:04:05")
	e.CreateTime = time.Now().Format("2006-01-02 15:04:05")

	currNode.FileIndex = postFileInfo.File_in_procesing
	currNode.FileName = postFileInfo.File.Name
	GConnMap.Store(postFileInfo.Sn, currNode)

	if err := r.InsertEntity(e, nil); err != nil {
		log.Println(err)
	}
	var infoResp PostFileInfoResp
	infoResp.Method = postFileInfo.Method
	infoResp.Sn = postFileInfo.Sn
	infoResp.Success = true
	infoResp.Chip_id = postFileInfo.Chip_id
	infoResp.File_in_procesing = 1
	infoResp.Total_file = postFileInfo.Total_file
	infoBuf, err := json.Marshal(&infoResp)
	if err != nil {
		fmt.Println(err)
	}
	Send_Resp(conn, string(infoBuf))
	PrintTail("PostFileInfo", postFileInfo)
}

/*
	接收设备上传的文件
	1、
*/
func CmdPostFile(conn *net.TCPConn, postFile PostFile) {
	PrintHead("PostFile")
	fileBuf, err := hex.DecodeString(postFile.Fragment.Source)
	if err != nil {
		log.Println(err)
	}
	crcCode := softwareCrc32([]byte(fileBuf), len(fileBuf))
	if postFile.Fragment.Checksum != crcCode {
		log.Println("crc32 check error!")
	}
	currNode, _ := getCurrNode(postFile.Sn)
	//文件开始时
	var f *os.File
	if postFile.Fragment.Index == 1 {
		f, err := os.Create("./tmp/" + currNode.FileName)
		if err != nil {
			fmt.Println(err)
		}
		f.Write(fileBuf)
	} else {
		f, err := os.OpenFile("./tmp/"+currNode.FileName, os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			fmt.Println(err)
		}
		f.Write(fileBuf)
	}

	if postFile.Fragment.Eof == true {
		if f != nil {
			f.Close()
		}
		r := mfiles.New(dbcomm.GetDB(), mfiles.DEBUG)
		var search mfiles.Search
		search.BatchNo = currNode.BatchNo
		e, _ := r.Get(search)
		var ne mfiles.MFiles
		if e.TodoCount-e.DoneCount == 1 {
			fmt.Println("=========>", e.TodoCount, e.DoneCount)
			//获取文件任务完成
			ne.EndTime = time.Now().Format("2006-01-02 15:04:05")
			currNode.Status = STATUS_INIT
			ne.Status = STATUS_SUCC
			GConnMap.Store(postFile.Sn, currNode)
		}
		ne.DoneCount = e.DoneCount + 1
		r.UpdataEntity(currNode.BatchNo, ne, nil)
		//更新明细的结束时间
		rr := dfiles.New(dbcomm.GetDB(), dfiles.DEBUG)
		var de dfiles.DFiles
		de.EndTime = time.Now().Format("2006-01-02 15:04:05")
		rr.UpdataEntityExt(currNode.BatchNo, currNode.FileIndex, de, nil)
	}

	var fResp PostFileResp
	fResp.Method = postFile.Method
	fResp.Sn = postFile.Sn
	fResp.Success = true
	fResp.Chip_id = postFile.Chip_id
	if fBuf, err := json.Marshal(&fResp); err != nil {
		log.Println(err)
	} else {
		Send_Resp(conn, string(fBuf))
	}
	PrintTail("PostFile")
}

/*
	接收客户端发来的PUSHINFO文件确认
*/
func CmdPushFileInfoResp(conn *net.TCPConn, infoResp PushFileInfoResp) {
	PrintHead("PUSH_FILE_INFO_RESP", infoResp)

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

	r := dfiles.New(dbcomm.GetDB(), dfiles.DEBUG)
	var e dfiles.DFiles
	e.FileName = "ssssss"
	e.FileLength = 10
	e.FileCrc32 = pushFile.Fragment.Checksum
	e.Sn = pushFile.Sn
	currNode, _ := getCurrNode(e.Sn)
	e.BatchNo = currNode.BatchNo
	e.ChipId = pushFile.Chip_id
	e.BeginTime = time.Now().Format("2006-01-02 15:04:05")
	e.CreateTime = time.Now().Format("2006-01-02 15:04:05")

	currNode.FileIndex = 1
	currNode.FileName = "1111"
	GConnMap.Store(pushFile.Sn, currNode)
	if err := r.InsertEntity(e, nil); err != nil {
		log.Println(err)
	}

	Send_Resp(conn, string(fBuf))
	PrintTail("PUSH_FILE_INFO_RESP")
}

/*
	接收客户端发来的PUSHINFO文件确认
*/
func CmdCheckUpdate(conn *net.TCPConn, upDate CheckUpdate) {

	PrintHead("CHECK_UPDATE")
	var upResp CheckUpdateResp
	upResp.Chip_id = upDate.Chip_id
	upResp.Method = CHECK_UDATE_RESP
	upResp.Sn = upDate.Sn
	upResp.Success = true
	upResp.Type = "chip"

	if upBuf, err := json.Marshal(&upResp); err != nil {
		log.Println(err)
	} else {
		Send_Resp(conn, string(upBuf))
	}
	PrintTail("CHECK_UPDATE")
}

/*
	接收客户端发来的PUSH文件确认
*/
func CmdPushFileResp(conn *net.TCPConn, fileResp PushFileResp) {
	PrintHead("PushFileResp")
	currNode, _ := getCurrNode(fileResp.Sn)
	r := mfiles.New(dbcomm.GetDB(), mfiles.DEBUG)
	var search mfiles.Search
	search.BatchNo = currNode.BatchNo
	e, _ := r.Get(search)
	var ne mfiles.MFiles
	if e.TodoCount-e.DoneCount == 1 {
		fmt.Println("=========>", e.TodoCount, e.DoneCount)
		//获取文件任务完成
		ne.EndTime = time.Now().Format("2006-01-02 15:04:05")
		currNode.Status = STATUS_INIT
		ne.Status = STATUS_SUCC
		GConnMap.Store(fileResp.Sn, currNode)
	}
	ne.DoneCount = e.DoneCount + 1
	r.UpdataEntity(currNode.BatchNo, ne, nil)
	//更新明细的结束时间
	rr := dfiles.New(dbcomm.GetDB(), dfiles.DEBUG)
	var de dfiles.DFiles
	de.EndTime = time.Now().Format("2006-01-02 15:04:05")
	rr.UpdataEntityExt(currNode.BatchNo, currNode.FileIndex, de, nil)

	PrintTail("PushFileResp")
}

/*
	接收客户端发来的PUSH文件确认
*/
func CmdPushInfoResp(conn *net.TCPConn, infoResp PushInfoResp) {
	log.Println("CmdPushInfoResp======>", infoResp)

}

func ProcPacket(conn *net.TCPConn, packBuf []byte) {
	var command Command
	if err := json.Unmarshal(packBuf, &command); err != nil {
		log.Println(err)
	}
	switch command.Method {
	case HEARTBEAT:
		var heart Heartbeat
		json.Unmarshal(packBuf, &heart)
		CmdHeartBeat(conn, heart)
	case ONLINE:
		var online Online
		if err := json.Unmarshal(packBuf, &online); err != nil {
			log.Println(err)
		}
		CmdOnLine(conn, online)
	case GET_COLOPHON_RESP:
		var phonResp GetColophonResp
		if err := json.Unmarshal(packBuf, &phonResp); err != nil {
			log.Println(err)
		}
		CmdGetColoPhonResp(conn, phonResp)
	case GET_INSTLL_DATADRIVE_RESP:
		var getInstDrive GetInstallDataDriveResp
		if err := json.Unmarshal(packBuf, &getInstDrive); err != nil {
			log.Println(err)
		}
		CmdGetInstallDriveResp(conn, getInstDrive)
	case POST_INSTLL_DATADRIVE:
		var postInstDrive PostInstallDataDrive
		if err := json.Unmarshal(packBuf, &postInstDrive); err != nil {
			log.Println(err)
		}
		CmdPostInstallDrive(conn, postInstDrive)
	case GET_FILE_RESP:
		var getFileResp GetFileResp
		err := json.Unmarshal(packBuf, &getFileResp)
		if err != nil {
			log.Println(err)
		}
		CmdGetFileResp(getFileResp)

	case POST_FILE_INFO:
		var postFileInfo PostFileInfo
		err := json.Unmarshal(packBuf, &postFileInfo)
		if err != nil {
			log.Println(err)
		}
		CmdPostFileInfo(conn, postFileInfo)
	case POST_FILE:
		var postFile PostFile
		json.Unmarshal(packBuf, &postFile)
		CmdPostFile(conn, postFile)

	case PUSH_FILE_INFO_RESP:
		var infoResp PushFileInfoResp
		json.Unmarshal(packBuf, &infoResp)
		CmdPushFileInfoResp(conn, infoResp)

	case PUSH_FILE_RESP:
		var fileResp PushFileResp
		json.Unmarshal(packBuf, &fileResp)
		CmdPushFileResp(conn, fileResp)

	case PUSH_INFO_RESP:
		var infoResp PushInfoResp
		json.Unmarshal(packBuf, &infoResp)
		CmdPushInfoResp(conn, infoResp)

	case CHECK_UDATE:
		var upDate CheckUpdate
		if err := json.Unmarshal(packBuf, &upDate); err != nil {
			log.Println(err)
		}
		CmdCheckUpdate(conn, upDate)
	}

}

func tcpPipe(conn *net.TCPConn) {
	ipStr := conn.RemoteAddr().String()
	defer func() {
		log.Println("disconnected :" + ipStr)
		conn.Close()
	}()

	reader := bufio.NewReader(conn)
	packBuf := make([]byte, 1024*1024*5)
	var nSum int32
	for {
		readBuf := make([]byte, 1024*100)
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
			if nSum >= packLen {
				log.Println("接收到一个完整的包!")
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
	http.HandleFunc("/dgate/busiGetFile", BusiGetFileCtl)
	http.HandleFunc("/dgate/busiPushFile", BusiPushFileCtl)
	http.HandleFunc("/dgate/busiGetVersions", BusiGetVerListCtl)
	http.HandleFunc("/dgate/busiGetDataDrives", BusiGetDataDriveCtl)
	http.HandleFunc("/dgate/busiPushInfo", BusiPushInfoCtl)
	http.HandleFunc("/dgate/busiQueryStatus", BusiQueryStatusCtl)

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

	//	var e mfiles.MFiles
	//	r := mfiles.New(dbcomm.GetDB(), mfiles.DEBUG)
	//	e.BatchNo = fmt.Sprintf("%d", time.Now().Unix())
	//	e.ChipId = "23232323"
	//	e.Sn = "23232323"
	//	e.Type = "3232323"
	//	e.FromDate = "2019-12-12"
	//	e.ToDate = "2019-12-12"
	//	e.Frange = "232	32"
	//	e.CmdType = "GETFILE"

	//	if err := r.InsertEntity(e, nil); err != nil {

	//		return
	//	}

}
