// rcs_contract_mgr project main.go
package main

import (
	"bufio"

	"encoding/hex"
	"encoding/json"
	"io/ioutil"

	"fmt"

	"io"
	"net"
	"net/http"
	"strings"

	"flag"
	"hcd-dgate/service/chip"
	"hcd-dgate/service/dbcomm"
	"hcd-dgate/service/device"
	"hcd-dgate/service/dfile"
	"hcd-dgate/service/onlinehis"
	"hcd-dgate/service/ver"
	"html/template"

	"hcd-dgate/service/mfile"

	"log"
	"os"
	"sync"
	"time"

	goconf "github.com/pantsing/goconf"

	"gopkg.in/natefinch/lumberjack.v2"
)

var (
	http_srv    *http.Server
	dbUrl       string
	ccdbUrl     string
	listenPort  int
	idleConns   int
	openConns   int
	GSn2ConnMap = &sync.Map{}
	GConn2SnMap = &sync.Map{}
)

func SysConsole(w http.ResponseWriter, req *http.Request) {
	t, _ := template.ParseFiles("./html/index.html")
	l := make([]string, 0)
	GConn2SnMap.Range(func(k, v interface{}) bool {
		fmt.Println("iterate:", k, v)
		V, _ := v.(string)
		l = append(l, V)
		return true
	})
	t.Execute(w, l)
}

func Send_Resp(conn *net.TCPConn, resp string) {
	head := make([]byte, 6)
	head[0] = 0x7E
	head[1] = 0x13
	packLenBytes := IntToBytes(len(resp) + 6)
	copy(head[2:], packLenBytes)
	head = append(head, []byte(resp)...)
	n, err := conn.Write([]byte(head))
	log.Println("---Send Command--->", n, string([]byte(head)), err)
}

/*
	接收设备的心跳，完成功能如下：
	1、更新当前设备连接的在线时间戳
*/
func CmdHeartBeat(conn *net.TCPConn, heart Heartbeat) {
	PrintHead(HEARTBEAT)
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
	PrintTail(HEARTBEAT)
}

/*
	接收设备在线登录，完成功能如下：
	1、把设备的当前连接保存在全局MAP
	2、记录在线的时间戳，以便定时检测是否在线
	3、更新数据库的在线状态
*/
func CmdOnLine(conn *net.TCPConn, online Online) {
	PrintHead(ONLINE)
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
		onlineMap := map[string]interface{}{
			IS_ONLINE: 1, DEVICE_TIME: time.Now().Format("2006-01-02 15:04:05")}
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
	//插入设备在线历史
	rr := onlinehis.New(dbcomm.GetDB(), onlinehis.DEBUG)
	var ne onlinehis.OnlineHis
	ne.Sn = online.Devices[0].Sn
	ne.ChipId = online.Devices[0].Chip_id
	ne.DeviceName = online.Devices[0].Device_name
	ne.DeviceSeries = online.Devices[0].Device_series
	ne.DeviceTime = online.Devices[0].Device_time
	ne.DeviceVer = online.Devices[0].Device_ver
	ne.HwVer = online.Devices[0].Hw_ver
	ne.SwVer = online.Devices[0].Sw_ver
	ne.RemoteIp = conn.RemoteAddr().String()
	ne.ActionType = ACTION_ONLINE
	ne.CreateTime = time.Now().Format("2006-01-02 15:04:05")
	if err := rr.InsertEntity(ne, nil); err != nil {
		log.Println(err.Error())
	}

	//更新设备的版本信息

	//	ne.Sn = online.Devices[0].Sn
	//	ne.ChipId = online.Devices[0].Chip_id

	//	ne.CreateTime = time.Now().Format("2006-01-02 15:04:05")
	//	if err := rrr.(ne, nil); err != nil {
	//		log.Println(err.Error())
	//	}

	//存储客服端的链接
	GSn2ConnMap.Store(online.Devices[0].Sn, StoreInfo{CurrConn: conn, SignInTime: time.Now()})
	GConn2SnMap.Store(conn, online.Devices[0].Sn)
	Send_Resp(conn, string(onlineBuf))
	PrintTail(ONLINE)
}

func CmdGetColoPhonResp(conn *net.TCPConn, phonResp GetColophonResp) {
	PrintHead(GET_COLOPHON_RESP)

	currNode, _ := getCurrNode(phonResp.Sn)
	r := mfiles.New(dbcomm.GetDB(), mfiles.DEBUG)
	var ne mfiles.MFiles
	ne.EndTime = time.Now().Format("2006-01-02 15:04:05")
	ne.UpdateTime = ne.EndTime
	ne.UpdateBy = UPDATE_USER
	if phonResp.Success {
		ne.Status = STATUS_SUCC
	} else {
		ne.Status = STATUS_FAIL
	}

	rr := onlinehis.New(dbcomm.GetDB(), onlinehis.DEBUG)
	var search onlinehis.Search
	search.Sn = phonResp.Sn
	search.ActionType = ACTION_ONLINE

	if e, err1 := rr.GetLast(search); err1 == nil {

		fmt.Println("=======>", e, err1)
		rrr := vers.New(dbcomm.GetDB(), vers.DEBUG)
		var vinfo vers.Vers
		vinfo.DeviceVer = e.DeviceVer
		vinfo.SwVer = e.SwVer
		vinfo.Sn = e.Sn
		vinfo.HwVer = e.HwVer

		var search vers.Search
		search.Sn = e.Sn
		if ee, err := rrr.Get(search); err == nil {
			rrr.UpdataEntity(fmt.Sprintf("%d", ee.Id), vinfo, nil)
		} else {
			rrr.InsertEntity(vinfo, nil)
		}
	}

	currNode.Status = STATUS_INIT

	GSn2ConnMap.Store(phonResp.Sn, currNode)

	r.UpdataEntity(currNode.BatchNo, ne, nil)

	PrintTail(GET_COLOPHON_RESP)
}

func CmdGetInstallDriveResp(conn *net.TCPConn, getDriveResp GetInstallDataDriveResp) {
	PrintHead(GET_INSTLL_DATADRIVE_RESP)
	log.Println(getDriveResp)
	PrintTail(GET_INSTLL_DATADRIVE_RESP)
}

/*
	接收从设备上传芯片参数信息
	1、做数据库记录
*/
func CmdPostInstallDrive(conn *net.TCPConn, postInstDrive PostInstallDataDrive) {
	PrintHead(POST_INSTLL_DATADRIVE)

	//记录数据库
	r := chips.New(dbcomm.GetDB(), chips.DEBUG)
	var search chips.Search
	search.Sn = postInstDrive.Sn
	if err := r.Delete(search.Sn, nil); err != nil {
		log.Println(err)
	}
	currNode, _ := getCurrNode(postInstDrive.Sn)

	var e chips.Chips
	e.Sn = postInstDrive.Sn
	for _, v := range postInstDrive.Datadrive {
		e.ChipLot = v.Lot
		e.ChipInstallDate = v.Install_time
		e.ActiveDate = v.Create_time
		e.CreateTime = time.Now().Format("2006-01-02 15:04:05")
		r.InsertEntity(e, nil)
	}

	rr := mfiles.New(dbcomm.GetDB(), mfiles.DEBUG)
	var ne mfiles.MFiles
	ne.EndTime = time.Now().Format("2006-01-02 15:04:05")
	ne.UpdateTime = ne.EndTime
	ne.UpdateBy = UPDATE_USER
	ne.TodoCount = postInstDrive.Dd_cnt
	ne.DoneCount = postInstDrive.Dd_cnt

	ne.Status = STATUS_SUCC
	rr.UpdataEntity(currNode.BatchNo, ne, nil)

	var driveResp PostInstallDataDriveResp
	driveResp.Method = POST_INSTLL_DATADRIVE_RESP
	driveResp.Sn = postInstDrive.Sn
	driveResp.Success = true
	respBuf, err := json.Marshal(&driveResp)
	if err != nil {
		log.Println(err)
	}
	Send_Resp(conn, string(respBuf))

	currNode.Status = STATUS_INIT
	GSn2ConnMap.Store(postInstDrive.Sn, currNode)

	PrintTail(POST_INSTLL_DATADRIVE_RESP)
}

/*
	接收设备获取文件指令的应答
	1、更新数据库的MFILE 表
*/
func CmdGetFileResp(fileResp GetFileResp) {
	PrintHead(GET_FILE_RESP)

	currNode, _ := getCurrNode(fileResp.Sn)
	var e mfiles.MFiles
	r := mfiles.New(dbcomm.GetDB(), mfiles.DEBUG)
	e.TodoCount = fileResp.Total_file
	r.UpdataEntity(currNode.BatchNo, e, nil)

	PrintTail(GET_FILE_RESP)
}

/*
	1
*/
func CmdPostFileInfo(conn *net.TCPConn, postFileInfo PostFileInfo) {
	PrintHead(POST_FILE_INFO)
	//记录数据库
	r := dfiles.New(dbcomm.GetDB(), dfiles.DEBUG)
	currNode, _ := getCurrNode(postFileInfo.Sn)
	var e dfiles.DFiles
	e.FileName = currNode.BatchNo + strings.Replace(postFileInfo.File.Name, "#", "_", 1)
	e.FileUrl = DEFAULT_PATH + e.FileName
	e.FileLength = postFileInfo.File.Length
	e.FileCrc32 = int32(postFileInfo.File.File_crc)
	e.FileType = postFileInfo.Type
	e.FileIndex = postFileInfo.File_in_procesing
	e.Sn = postFileInfo.Sn
	e.BatchNo = currNode.BatchNo
	e.ChipId = postFileInfo.Chip_id
	e.BeginTime = time.Now().Format("2006-01-02 15:04:05")
	e.CreateTime = time.Now().Format("2006-01-02 15:04:05")

	currNode.FileIndex = postFileInfo.File_in_procesing
	currNode.FileName = e.FileName

	GSn2ConnMap.Store(postFileInfo.Sn, currNode)

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

	PrintTail(POST_FILE_INFO, postFileInfo)
}

/*
	接收设备上传的文件
	1、
*/
func CmdPostFile(conn *net.TCPConn, postFile PostFile) {
	PrintHead(POST_FILE)

	currNode, _ := getCurrNode(postFile.Sn)

	fileBuf, err := hex.DecodeString(postFile.Fragment.Source)
	if err != nil {
		log.Println(err)
	}

	crcCode := softwareCrc32([]byte(fileBuf), len(fileBuf))
	if postFile.Fragment.Checksum != int32(crcCode) {
		log.Println("crc32 check error!" + currNode.FileName)
	}
	//文件开始时
	var f *os.File
	if postFile.Fragment.Index == 1 {
		f, err := os.Create(DEFAULT_PATH + currNode.FileName)
		if err != nil {
			fmt.Println(err)
		}
		f.Write(fileBuf)
	} else {
		f, err := os.OpenFile(DEFAULT_PATH+currNode.FileName, os.O_WRONLY|os.O_APPEND, 0666)
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
			//获取文件任务完成
			ne.EndTime = time.Now().Format("2006-01-02 15:04:05")
			currNode.Status = STATUS_INIT
			ne.Status = STATUS_SUCC
			GSn2ConnMap.Store(postFile.Sn, currNode)
		}
		ne.DoneCount = e.DoneCount + 1
		ne.UpdateTime = time.Now().Format("2006-01-02 15:04:05")
		ne.UpdateBy = UPDATE_USER
		r.UpdataEntity(currNode.BatchNo, ne, nil)
		//更新明细的结束时间
		rr := dfiles.New(dbcomm.GetDB(), dfiles.DEBUG)
		var de dfiles.DFiles
		de.EndTime = time.Now().Format("2006-01-02 15:04:05")
		de.UpdateTime = de.EndTime
		de.UpdateBy = UPDATE_USER
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
	PrintTail(POST_FILE)
}

/*
	接收客户端发来的PUSHINFO文件确认
*/
func CmdPushFileInfoResp(conn *net.TCPConn, infoResp PushFileInfoResp) {
	PrintHead(PUSH_FILE_INFO_RESP)

	currNode, _ := getCurrNode(infoResp.Sn)
	var pushFile PushFile
	pushFile.Chip_id = infoResp.Chip_id
	pushFile.Method = PUSH_FILE
	pushFile.Sn = infoResp.Sn
	pushFile.Fragment.Index = 1
	pushFile.Fragment.Length = int(currNode.FileSize)
	pushFile.Fragment.Eof = true

	fileBuf, err := ioutil.ReadFile(currNode.FileName)
	pushFile.Fragment.Source = hex.EncodeToString(fileBuf)

	crc32 := softwareCrc32(fileBuf, len(fileBuf))
	pushFile.Fragment.Checksum = crc32
	fBuf, err := json.Marshal(&pushFile)
	if err != nil {
		log.Println(err)
	}

	r := dfiles.New(dbcomm.GetDB(), dfiles.DEBUG)
	var e dfiles.DFiles
	e.FileName = currNode.FileName
	e.FileLength = int(currNode.FileSize)
	e.FileCrc32 = pushFile.Fragment.Checksum
	e.Sn = pushFile.Sn

	e.BatchNo = currNode.BatchNo
	e.ChipId = pushFile.Chip_id
	e.BeginTime = time.Now().Format("2006-01-02 15:04:05")
	e.CreateTime = time.Now().Format("2006-01-02 15:04:05")

	currNode.FileIndex = 1
	GSn2ConnMap.Store(pushFile.Sn, currNode)
	if err := r.InsertEntity(e, nil); err != nil {
		log.Println(err)
	}
	Send_Resp(conn, string(fBuf))

	PrintTail(PUSH_FILE_INFO_RESP)
}

/*
	接收客户端发来的PUSHINFO文件确认
*/
func CmdCheckUpdate(conn *net.TCPConn, upDate CheckUpdate) {
	PrintHead(CHECK_UDATE)

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
	PrintTail(CHECK_UDATE)
}

/*
	接收客户端发来的PUSH文件确认
*/
func CmdPushFileResp(conn *net.TCPConn, fileResp PushFileResp) {
	PrintHead(PUSH_FILE_RESP)

	currNode, _ := getCurrNode(fileResp.Sn)
	r := mfiles.New(dbcomm.GetDB(), mfiles.DEBUG)
	var e mfiles.MFiles
	e.EndTime = time.Now().Format("2006-01-02 15:04:05")
	if fileResp.Success {
		e.Status = STATUS_SUCC
	} else {
		e.Status = STATUS_FAIL
	}
	e.UpdateBy = UPDATE_USER
	e.UpdateTime = e.EndTime
	r.UpdataEntity(currNode.BatchNo, e, nil)

	currNode.Status = STATUS_INIT
	GSn2ConnMap.Store(fileResp.Sn, currNode)
	//更新明细的结束时间
	rr := dfiles.New(dbcomm.GetDB(), dfiles.DEBUG)
	var de dfiles.DFiles
	de.EndTime = time.Now().Format("2006-01-02 15:04:05")
	de.UpdateTime = de.EndTime
	de.UpdateBy = UPDATE_USER
	rr.UpdataEntity(currNode.BatchNo, de, nil)

	PrintTail(PUSH_FILE_RESP)
}

/*
	接收设备端反馈的PUSH INFO 确认
	1、确认后，根据结果更新数据表
	2、刷新缓存的结果状态
*/
func CmdPushInfoResp(conn *net.TCPConn, infoResp PushInfoResp) {
	PrintHead(PUSH_INFO_RESP)
	currNode, _ := getCurrNode(infoResp.Sn)
	currNode.Status = STATUS_INIT
	GSn2ConnMap.Store(infoResp.Sn, currNode)
	r := mfiles.New(dbcomm.GetDB(), mfiles.DEBUG)
	var e mfiles.MFiles
	if infoResp.Success {
		e.Status = STATUS_SUCC
	} else {
		e.Status = STATUS_FAIL
	}
	e.EndTime = time.Now().Format("2006-01-02 15:04:05")
	e.UpdateTime = e.EndTime
	e.UpdateBy = UPDATE_USER
	r.UpdataEntity(currNode.BatchNo, e, nil)

	PrintTail(PUSH_INFO_RESP)

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

/*
	网络端口，更新设备为线下

*/
func OffLine(sn string) {
	PrintHead(OFFLINE, sn)
	r := device.New(dbcomm.GetDB(), device.DEBUG)
	onlineMap := map[string]interface{}{DEVICE_TIME: time.Now().Format("2006-01-02 15:04:05"),
		IS_ONLINE: 2}
	err := r.UpdateMapEx(sn, onlineMap, nil)
	if err != nil {
		log.Println("更新失败", err)
	}

	//插入设备在线历史
	rr := onlinehis.New(dbcomm.GetDB(), onlinehis.DEBUG)
	var ne onlinehis.OnlineHis
	ne.Sn = sn
	ne.ActionType = ACTION_OFFLINE
	ne.DeviceTime = time.Now().Format("2006-01-02 15:04:05")
	if err := rr.InsertEntity(ne, nil); err != nil {
		log.Println(err.Error())
	}

	PrintTail(OFFLINE)
}

func tcpPipe(conn *net.TCPConn) {
	ipStr := conn.RemoteAddr().String()
	defer func() {
		log.Println("disconnected :" + ipStr)
		snIf, ok := GConn2SnMap.Load(conn)
		if ok {
			sn, _ := snIf.(string)
			if sn != "" {
				OffLine(sn)
			}
			GConn2SnMap.Delete(conn)
		}
		GConn2SnMap.Delete(conn)
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
		if nSum == 0 {
			if int(readBuf[0]) != 0x7E {
				log.Println("Error Packet And Close Connection...")
				return
			}
		}
		copy(packBuf[nSum:], readBuf[0:nLen])
		nSum = nSum + int32(nLen)
		if nSum < HEAD_LEN {
			continue
		}
		for {
			packLen := BytesToInt(packBuf[2:6])
			if nSum >= packLen {
				log.Println("接收到一个完整的包!", packBuf[0], packBuf[1])
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
	log.Println("<==========HttpServer Starting...==========>")
	http.HandleFunc("/dgate/busiGetFile", BusiGetFileCtl)
	http.HandleFunc("/dgate/busiPushFile", BusiPushFileCtl)
	http.HandleFunc("/dgate/busiGetVersions", BusiGetVerListCtl)
	http.HandleFunc("/dgate/busiGetDataDrives", BusiGetDataDriveCtl)
	http.HandleFunc("/dgate/busiPushInfo", BusiPushInfoCtl)
	http.HandleFunc("/dgate/busiQueryStatus", BusiQueryStatusCtl)
	http.HandleFunc("/dgate/console", SysConsole)
	http_srv = &http.Server{
		Addr: ":7088",
	}
	if err := http_srv.ListenAndServe(); err != nil {
		log.Printf("listen: %s\n", err)
	}
}

func init() {
	log.Println("<==========System Params Init...==========>")
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
	log.Println("<==========MicroPoint Gate Starting...==========>")
	log.Println("	V0.2    ")
	dbcomm.InitDB(dbUrl, ccdbUrl, idleConns, openConns)
	go go_WebServer()
	var tcpAddr *net.TCPAddr
	tcpAddr, _ = net.ResolveTCPAddr("tcp", ":8089")
	tcpListener, _ := net.ListenTCP("tcp", tcpAddr)
	defer tcpListener.Close()
	for {
		tcpConn, err := tcpListener.AcceptTCP()
		GConn2SnMap.Store(tcpConn, "")
		if err != nil {
			continue
		}
		log.Println("has a new conn: " + tcpConn.RemoteAddr().String())
		go tcpPipe(tcpConn)
	}

	//	str := "4e554d4245522c4c4556454c2c524553554c542c444154452c4c4f542c4954454d2c514349442c5553455249440a"
	//	buf, _ := hex.DecodeString(str)
	//	d := softwareCrc32(buf, len(buf))
	//	fmt.Println(string(buf), int32(d))

	//	if 0X7E == 126 {
	//		fmt.Println("OK")
	//	}
}
