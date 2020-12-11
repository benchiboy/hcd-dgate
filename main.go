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
	"os/signal"
	"strings"
	"syscall"

	"flag"
	"hcd-dgate/service/chip"
	"hcd-dgate/service/dbcomm"
	"hcd-dgate/service/device"
	"hcd-dgate/service/dfile"
	"hcd-dgate/service/heartbeat"
	"hcd-dgate/service/onlinehis"
	"hcd-dgate/service/ver"

	//	"hcd-dgate/util"
	"html/template"
	"math/rand"

	"hcd-dgate/service/mfile"

	"log"
	"os"
	"sync"
	"time"

	goconf "github.com/pantsing/goconf"

	"gopkg.in/natefinch/lumberjack.v2"
)

var (
	http_srv     *http.Server
	dbUrl        string
	ccdbUrl      string
	listenPort   int
	idleConns    int
	saveHearbeat bool
	openConns    int
	GSn2ConnMap  = &sync.Map{}
	GConn2SnMap  = &sync.Map{}
)

func SysConsoleDetail(w http.ResponseWriter, req *http.Request) {
	var snDetail BusiSnDetail
	var snDetailResp BusiSnDetailResp
	reqBuf, err := ioutil.ReadAll(req.Body)
	if err = json.Unmarshal(reqBuf, &snDetail); err != nil {
		snDetailResp.ErrorCode = ERR_CODE_JSONERR
		snDetailResp.ErrorMsg = err.Error()
		Write_Response(snDetailResp, w, req, GET_FILE)
		return
	}
	defer req.Body.Close()

	snDetailResp.ErrorCode = ERR_CODE_SUCCESS
	snDetailResp.ErrorMsg = ERROR_MAP[ERR_CODE_SUCCESS]
	e, _ := GSn2ConnMap.Load(snDetail.Sn)
	info, _ := e.(StoreInfo)
	snDetailResp.Info = info
	Write_Response(snDetailResp, w, req, "DETAIL")

}

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

func Send_Resp(threadId int, conn *net.TCPConn, resp string) error {
	head := make([]byte, 6)
	head[0] = 0x7E
	head[1] = 0x13
	packLenBytes := IntToBytes(len(resp) + 6)

	if conn == nil {
		PrintLog(threadId, "Conn is Null")
		return fmt.Errorf("Conn is null")
	}

	//PrintLog(threadId, "MsgLen:==Package Head Len===>", len(resp), packLenBytes)
	copy(head[2:], packLenBytes)
	head = append(head, []byte(resp)...)

	var err error
	var sendLen int
	totalLen := len(head)
	nSum := 0
	for totalLen > 0 {
		sendLen, err = conn.Write([]byte(head)[nSum:])
		if err != nil {
			PrintLog(threadId, "Send_Resp Error:", err)
			conn.Close()
			return err
		}
		//		if totalLen > 256 {
		//			//PrintLog(threadId, "SendPush_File", sendLen, totalLen, nSum)
		//		} else {
		//PrintLog(threadId, "Send Len And Msg===>", string(head))
		//		}
		nSum += sendLen
		totalLen = totalLen - sendLen
	}
	return nil
}

/*
	接收设备的心跳，完成功能如下：
	1、更新当前设备连接的在线时间戳
	2、更新数据表的心跳最新时间

*/
func CmdHeartBeat(threadId int, conn *net.TCPConn, heart Heartbeat) {
	//PrintHead(threadId, HEARTBEAT)
	PrintLog(threadId, heart.Sn, "HearBeat Remote Ip===>", heart.Sn, conn.RemoteAddr().String())
	var heartResp HeartbeatResp
	heartResp.Chip_id = heart.Chip_id
	heartResp.Method = HEARTBEAT_RESP
	heartResp.Sn = heart.Sn
	heartResp.Success = true
	heartBuf, err := json.Marshal(&heartResp)
	if err != nil {
		PrintLog(threadId, err)
	}
	currNode, err := getCurrNode(threadId, heart.Sn)
	if err != nil {
		PrintLog(threadId, heart.Sn+"未在线收到心跳包", conn)
		conn.Close()
	} else {

		r := device.New(dbcomm.GetDB(), device.INFO)
		var search device.Search
		search.Sn = heart.Sn
		if e, err := r.Get(search); err == nil {
			if e.IsOnline == STATUS_OFFLINE {
				PrintLog(threadId, heart.Sn+"未在线收到心跳包,更新数据库", conn)
				onlineMap := map[string]interface{}{
					IS_ONLINE: STATUS_ONLINE, DEVICE_TIME: time.Now().Format("2006-01-02 15:04:05")}
				err = r.UpdateMap(fmt.Sprintf("%d", e.Id), onlineMap, nil)
				if err != nil {
					log.Println("更新失败", err)
				}
			}
		}

		//log.Println("SaveHearbeat===>")
		h := heartbeat.New(dbcomm.GetDB(), heartbeat.INFO)
		var e heartbeat.Heartbeat
		e.Sn = heart.Sn
		e.CreateTime = time.Now().Format("2006-01-02 15:04:05")
		h.InsertEntity(e, nil)

		currNode.SignInTime = time.Now()
		currNode.CurrConn = conn

		GSn2ConnMap.Store(heart.Sn, currNode)
		GConn2SnMap.Store(conn, heart.Sn)
		Send_Resp(threadId, conn, string(heartBuf))
	}
	//PrintTail(threadId, HEARTBEAT)
}

/*
	接收设备在线登录，完成功能如下：
	1、把设备的当前连接保存在全局MAP
	2、记录在线的时间戳，以便定时检测是否在线
	3、更新数据库的在线状态
*/
func CmdOnLine(threadId int, conn *net.TCPConn, online Online) {
	PrintHead(threadId, ONLINE+"--->"+online.Devices[0].Sn)
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

	log.Printf("Online %+v", online)

	r := device.New(dbcomm.GetDB(), device.DEBUG)
	var search device.Search
	search.Sn = onlineResp.Sn
	if e, err := r.Get(search); err == nil {
		onlineMap := map[string]interface{}{"icicd": online.Devices[0].Icicd,
			IS_ONLINE: STATUS_ONLINE, DEVICE_TIME: time.Now().Format("2006-01-02 15:04:05")}
		err = r.UpdateMap(fmt.Sprintf("%d", e.Id), onlineMap, nil)
		if err != nil {
			log.Println("更新失败", err)
		}
	} else {
		for _, v := range online.Devices {
			var e device.Device
			e.IsOnline = STATUS_ONLINE
			e.DeviceTime = time.Now().Format("2006-01-02 15:04:05")
			e.Sn = v.Sn
			e.IsEnable = 1
			e.Icicd = v.Icicd
			e.FcdClass = "C"
			e.ChipId = v.Chip_id
			e.ProductType = v.Device_series
			e.ProductNo = v.Device_name
			e.CreateTime = time.Now().Format("2006-01-02 15:04:05")
			r.InsertEntity(e, nil)
		}
	}
	//插入设备在线历史
	rr := onlinehis.New(dbcomm.GetDB(), onlinehis.INFO)
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
	if _, ok := GSn2ConnMap.Load(online.Devices[0].Sn); ok {
		PrintLog(threadId, online.Devices[0].Sn+"上线时,发现已经存在Map中!")
	}
	//存储客服端的链接
	GConn2SnMap.Store(conn, online.Devices[0].Sn)
	GSn2ConnMap.Store(online.Devices[0].Sn, StoreInfo{CurrConn: conn, SignInTime: time.Now()})
	Send_Resp(threadId, conn, string(onlineBuf))

	PrintTail(threadId, ONLINE+"--->"+online.Devices[0].Sn)

}

func CmdGetColoPhonResp(threadId int, conn *net.TCPConn, phonResp GetColophonResp) {
	PrintHead(threadId, GET_COLOPHON_RESP)

	currNode, _ := getCurrNode(threadId, phonResp.Sn)
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
		rrr := vers.New(dbcomm.GetDB(), vers.DEBUG)
		var vinfo vers.Vers
		vinfo.DeviceVer = e.DeviceVer
		vinfo.SwVer = e.SwVer
		vinfo.Sn = e.Sn
		vinfo.HwVer = e.HwVer

		var search vers.Search
		search.Sn = e.Sn
		if ee, err := rrr.Get(search); err == nil {
			vinfo.UpdateTime = time.Now().Format("2006-01-02 15:04:05")
			rrr.UpdataEntity(fmt.Sprintf("%d", ee.Id), vinfo, nil)
		} else {
			vinfo.CreateTime = time.Now().Format("2006-01-02 15:04:05")
			rrr.InsertEntity(vinfo, nil)
		}
	}

	currNode.Status = STATUS_INIT
	GSn2ConnMap.Store(phonResp.Sn, currNode)
	r.UpdataEntity(currNode.BatchNo, ne, nil)

	PrintTail(threadId, GET_COLOPHON_RESP)
}

func CmdGetInstallDriveResp(threadId int, conn *net.TCPConn, getDriveResp GetInstallDataDriveResp) {
	PrintHead(threadId, GET_INSTLL_DATADRIVE_RESP)
	PrintLog(threadId, getDriveResp)

	currNode, _ := getCurrNode(threadId, getDriveResp.Sn)
	rr := mfiles.New(dbcomm.GetDB(), mfiles.DEBUG)
	var ne mfiles.MFiles

	if getDriveResp.Success {
		ne.Status = STATUS_SUCC
	} else {
		ne.Status = STATUS_FAIL
	}
	ne.TodoCount = getDriveResp.Total_cnt
	ne.EndTime = time.Now().Format("2006-01-02 15:04:05")
	ne.UpdateTime = ne.EndTime

	currNode.Status = STATUS_INIT
	GSn2ConnMap.Store(getDriveResp.Sn, currNode)
	rr.UpdataEntity(currNode.BatchNo, ne, nil)

	PrintTail(threadId, GET_INSTLL_DATADRIVE_RESP)
}

/*
	接收从设备上传芯片参数信息
	1、做数据库记录
*/
func CmdPostInstallDrive(threadId int, conn *net.TCPConn, postInstDrive PostInstallDataDrive) {
	PrintHead(threadId, POST_INSTLL_DATADRIVE)
	//记录数据库
	r := chips.New(dbcomm.GetDB(), chips.DEBUG)
	var search chips.Search
	search.Sn = postInstDrive.Sn
	if err := r.Delete(search.Sn, nil); err != nil {
		log.Println(err)
	}
	currNode, _ := getCurrNode(threadId, postInstDrive.Sn)
	var e chips.Chips
	e.Sn = postInstDrive.Sn
	log.Printf("postInstDrive===>%+v\n", postInstDrive)
	for _, v := range postInstDrive.Datadrive {
		e.ChipLot = v.Lot
		e.ChipInstallDate = v.Install_time
		e.ProductDate = v.Create_time
		e.CreateTime = time.Now().Format("2006-01-02 15:04:05")
		e.InstallType = v.Install_type
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
	respBuf, _ := json.Marshal(&driveResp)

	Send_Resp(threadId, conn, string(respBuf))

	currNode.Status = STATUS_INIT
	GSn2ConnMap.Store(postInstDrive.Sn, currNode)

	PrintTail(threadId, POST_INSTLL_DATADRIVE_RESP)
}

/*
	接收设备获取文件指令的应答
	1、更新数据库的MFILE 表
*/
func CmdGetFileResp(threadId int, fileResp GetFileResp) {
	PrintHead(threadId, GET_FILE_RESP)
	currNode, _ := getCurrNode(threadId, fileResp.Sn)
	var e mfiles.MFiles
	r := mfiles.New(dbcomm.GetDB(), mfiles.DEBUG)

	e.TodoCount = fileResp.Total_file
	if fileResp.Total_file == 0 {
		//没有文件直接结束
		e.TodoCount = 0
		e.DoneCount = 0
		e.Status = STATUS_SUCC
		e.EndTime = time.Now().Format("2006-01-02 15:04:05")
		currNode.Status = STATUS_INIT
		GSn2ConnMap.Store(fileResp.Sn, currNode)
	}
	e.UpdateTime = time.Now().Format("2006-01-02 15:04:05")
	r.UpdataEntity(currNode.BatchNo, e, nil)

	PrintTail(threadId, GET_FILE_RESP)
}

/*
	1
*/
func CmdPostFileInfo(threadId int, conn *net.TCPConn, postFileInfo PostFileInfo) {
	PrintHead(threadId, POST_FILE_INFO+postFileInfo.Sn)
	//记录数据库
	r := dfiles.New(dbcomm.GetDB(), dfiles.INFO)
	currNode, _ := getCurrNode(threadId, postFileInfo.Sn)
	var e dfiles.DFiles
	e.FileName = currNode.BatchNo + strings.Replace(postFileInfo.File.Name, "#", "_", 1)
	newPath := DEFAULT_PATH + time.Now().Format("2006-01-02") + "/"
	if !PathIsExist(newPath) {
		err := os.Mkdir(newPath, os.ModePerm)
		if err != nil {
			PrintLog(threadId, "Mkdir===>", err)
		}
	}
	e.FileUrl = newPath + e.FileName
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
	currNode.FileName = newPath + e.FileName
	currNode.FileTotal = postFileInfo.Total_file
	GSn2ConnMap.Store(postFileInfo.Sn, currNode)

	if err := r.InsertEntity(e, nil); err != nil {
		PrintLog(threadId, err)
	}
	var infoResp PostFileInfoResp
	infoResp.Method = postFileInfo.Method
	infoResp.Sn = postFileInfo.Sn
	infoResp.Success = true
	infoResp.Chip_id = postFileInfo.Chip_id
	infoResp.File_in_procesing = postFileInfo.File_in_procesing
	infoResp.Total_file = postFileInfo.Total_file
	infoBuf, err := json.Marshal(&infoResp)
	if err != nil {
		PrintLog(threadId, err)
	}
	PrintLog(threadId, postFileInfo.Sn+"待传文件信息===>", postFileInfo.File.Name, postFileInfo.Total_file, postFileInfo.File_in_procesing)
	Send_Resp(threadId, conn, string(infoBuf))
	PrintTail(threadId, POST_FILE_INFO+postFileInfo.Sn)
}

/*
	接收设备上传的文件
	1、
*/
func CmdPostFile(threadId int, conn *net.TCPConn, postFile PostFile) {
	PrintHead(threadId, POST_FILE+postFile.Sn)
	currNode, _ := getCurrNode(threadId, postFile.Sn)

	fileBuf, err := hex.DecodeString(postFile.Fragment.Source)
	if err != nil {
		PrintLog(threadId, postFile.Sn+"POST_ERROR"+err.Error())
		conn.Close()
		return
	}
	crcCode := softwareCrc32([]byte(fileBuf), len(fileBuf))
	if postFile.Fragment.Checksum != int32(crcCode) {
		PrintLog(threadId, "Crc32 check error!"+currNode.FileName)
		conn.Close()
		return
	}
	PrintLog(threadId, postFile.Sn+"PostFile Len===>", len(fileBuf))
	currNode.SignInTime = time.Now()
	//文件开始时
	var f *os.File
	if postFile.Fragment.Index == 1 {
		f, err := os.Create(currNode.FileName)
		if err != nil {
			PrintLog(threadId, "CreateFile"+err.Error())
			conn.Close()
			return
		}
		f.Write(fileBuf)
	} else {
		f, err := os.OpenFile(currNode.FileName, os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			PrintLog(threadId, err)
			conn.Close()
			return
		}
		f.Write(fileBuf)
	}
	//增加类型断言处理，微点有些设备传的是布尔，有些设备传的是字符串
	var eofTag = false
	switch t := postFile.Fragment.Eof.(type) {
	default:
		log.Printf("unexpected type %T", t)
		break
	case string:
		if t == "true" {
			eofTag = true
		}
	case bool:
		if t {
			eofTag = true
		}
	}

	if eofTag == true {
		if f != nil {
			f.Close()
		}
		PrintLog(threadId, postFile.Sn+"完成文件索引===>", currNode.FileIndex, currNode.FileTotal)
		if currNode.FileTotal == currNode.FileIndex {
			r := mfiles.New(dbcomm.GetDB(), mfiles.INFO)
			var search mfiles.Search
			search.BatchNo = currNode.BatchNo
			var ne mfiles.MFiles
			ne.EndTime = time.Now().Format("2006-01-02 15:04:05")
			ne.Status = STATUS_SUCC
			ne.DoneCount = currNode.FileIndex
			ne.UpdateTime = time.Now().Format("2006-01-02 15:04:05")
			ne.UpdateBy = UPDATE_USER
			r.UpdataEntity(currNode.BatchNo, ne, nil)
			PrintLog(threadId, postFile.Sn+"===>所有文件传输完毕!")
			currNode.Status = STATUS_INIT
			GSn2ConnMap.Store(postFile.Sn, currNode)
		}
		//更新明细的结束时间
		rr := dfiles.New(dbcomm.GetDB(), dfiles.INFO)
		var de dfiles.DFiles
		de.EndTime = time.Now().Format("2006-01-02 15:04:05")
		de.UpdateTime = de.EndTime
		de.UpdateBy = UPDATE_USER
		rr.UpdataEntityExt(currNode.BatchNo, currNode.FileIndex, de, nil)
		//更新文件传输的进度
		{
			rate := GetPercent(int64(currNode.FileIndex), int64(currNode.FileTotal))
			PrintLog(threadId, "传输%", rate)
			r := mfiles.New(dbcomm.GetDB(), mfiles.INFO)
			var search mfiles.Search
			search.BatchNo = currNode.BatchNo
			var ne mfiles.MFiles
			ne.Percent = rate
			r.UpdataEntity(currNode.BatchNo, ne, nil)
		}

	}

	var fResp PostFileResp
	fResp.Method = postFile.Method
	fResp.Sn = postFile.Sn
	fResp.Success = true
	fResp.Chip_id = postFile.Chip_id
	if fBuf, err := json.Marshal(&fResp); err != nil {
		PrintLog(threadId, err)
	} else {
		PrintLog(threadId, string(fBuf))
		Send_Resp(threadId, conn, string(fBuf))
	}
	PrintTail(threadId, POST_FILE+postFile.Sn)
}

/*
	接收客户端发来的PUSHINFO文件确认
*/
func CmdPushFileInfoResp(threadId int, conn *net.TCPConn, infoResp PushFileInfoResp) {
	PrintHead(threadId, PUSH_FILE_INFO_RESP+infoResp.Sn)

	currNode, _ := getCurrNode(threadId, infoResp.Sn)
	_, crcCode := pubPushFile(threadId, conn, infoResp.Sn, infoResp.Chip_id)

	PrintLog(threadId, "收到机器应答确认===>", infoResp.Total_file, currNode.FileName)

	r := dfiles.New(dbcomm.GetDB(), dfiles.DEBUG)
	var e dfiles.DFiles
	e.FileName = currNode.FileName
	e.FileLength = int(currNode.FileSize)
	e.FileCrc32 = crcCode
	e.Sn = infoResp.Sn
	e.BatchNo = currNode.BatchNo
	e.ChipId = infoResp.Chip_id
	e.BeginTime = time.Now().Format("2006-01-02 15:04:05")
	e.CreateTime = time.Now().Format("2006-01-02 15:04:05")
	if err := r.InsertEntity(e, nil); err != nil {
		log.Println(err)
	}

	PrintTail(threadId, PUSH_FILE_INFO_RESP+infoResp.Sn)
}

/*
	接收客户端发来的PUSHINFO文件确认
*/
func CmdCheckUpdate(threadId int, conn *net.TCPConn, upDate CheckUpdate) {
	PrintHead(threadId, CHECK_UDATE)

	var upResp CheckUpdateResp
	upResp.Chip_id = upDate.Chip_id
	upResp.Method = CHECK_UDATE_RESP
	upResp.Sn = upDate.Sn
	upResp.Success = true
	upResp.Type = upDate.Type
	if upBuf, err := json.Marshal(&upResp); err != nil {
		log.Println(err)
	} else {
		Send_Resp(threadId, conn, string(upBuf))
	}

	PrintTail(threadId, CHECK_UDATE)
}

func pubPushFile(threadId int, conn *net.TCPConn, sn string, chipId string) (error, int32) {
	var pushFile PushFile
	currNode, _ := getCurrNode(threadId, sn)
	if currNode.FileSize <= currNode.FileOffset+FILE_BLOCK_SIZE {
		pushFile.Fragment.Eof = true
		currNode.ReadSize = currNode.FileSize - currNode.FileOffset
		if currNode.ReadSize < 0 {
			currNode.ReadSize = 0
			currNode.FileOffset = currNode.FileSize
		}
		PrintHead(threadId, "文件最后一块===>", currNode.FileName, sn, currNode.FileOffset, currNode.ReadSize, currNode.FileIndex)

	} else {
		pushFile.Fragment.Eof = false
		currNode.ReadSize = FILE_BLOCK_SIZE
		PrintHead(threadId, "Push的索引值===》", sn, currNode.FileOffset, currNode.ReadSize, currNode.FileIndex)
	}
	pushFile.Fragment.Length = int(currNode.ReadSize)
	pushFile.Chip_id = chipId
	pushFile.Method = PUSH_FILE
	pushFile.Sn = sn
	pushFile.Fragment.Index = currNode.FileIndex
	var err error
	fileBuf, err := ioutil.ReadFile(currNode.FileName)
	if err != nil {
		PrintLog(threadId, err)
		return err, 0
	}
	pushFile.Fragment.Source = hex.EncodeToString(fileBuf[currNode.FileOffset : currNode.FileOffset+currNode.ReadSize])
	crc32 := softwareCrc32(fileBuf[currNode.FileOffset:currNode.FileOffset+currNode.ReadSize], len(fileBuf[currNode.FileOffset:currNode.FileOffset+currNode.ReadSize]))
	PrintLog(threadId, " 文件Crc32校验值===>", crc32)
	pushFile.Fragment.Checksum = crc32
	fBuf, _ := json.Marshal(&pushFile)
	err = Send_Resp(threadId, conn, string(fBuf))

	GSn2ConnMap.Store(sn, currNode)

	return err, crc32
}

/*
	接收客户端发来的PUSH文件确认
*/
func CmdPushFileResp(threadId int, conn *net.TCPConn, fileResp PushFileResp) {
	PrintHead(threadId, PUSH_FILE_RESP+fileResp.Sn)

	currNode, _ := getCurrNode(threadId, fileResp.Sn)
	if fileResp.Success {
		PrintLog(threadId, "确认的索引值", fileResp.Index, "开始下一个")
		currNode.SignInTime = time.Now()
		currNode.FileOffset += currNode.ReadSize
		currNode.FileIndex += 1
		GSn2ConnMap.Store(fileResp.Sn, currNode)

		if currNode.FileOffset == currNode.FileSize {
			PrintLog(threadId, "文件结束===>", currNode.FileOffset, currNode.FileSize)
			currNode.Status = STATUS_INIT
			GSn2ConnMap.Store(fileResp.Sn, currNode)
			r := mfiles.New(dbcomm.GetDB(), mfiles.DEBUG)
			var e mfiles.MFiles
			e.UpdateBy = UPDATE_USER
			e.Status = STATUS_SUCC
			e.EndTime = time.Now().Format("2006-01-02 15:04:05")
			e.UpdateTime = time.Now().Format("2006-01-02 15:04:05")
			r.UpdataEntity(currNode.BatchNo, e, nil)
			//更新明细的结束时间
			rr := dfiles.New(dbcomm.GetDB(), dfiles.DEBUG)
			var de dfiles.DFiles
			de.EndTime = time.Now().Format("2006-01-02 15:04:05")
			de.UpdateTime = de.EndTime
			de.UpdateBy = UPDATE_USER
			rr.UpdataEntity2(currNode.BatchNo, de, nil)
		} else if currNode.FileOffset < currNode.FileSize {
			err, _ := pubPushFile(threadId, conn, fileResp.Sn, fileResp.Chip_id)
			if err != nil {
				currNode.Status = STATUS_INIT
				GSn2ConnMap.Store(fileResp.Sn, currNode)
			}
		} else {
			PrintLog(threadId, "收到意外的确认...")
		}
		//更新进度百分比
		{
			rate := GetPercent(int64(currNode.FileOffset), int64(currNode.FileSize))
			PrintLog(threadId, "传输%", rate)
			r := mfiles.New(dbcomm.GetDB(), mfiles.INFO)
			var search mfiles.Search
			search.BatchNo = currNode.BatchNo
			var ne mfiles.MFiles
			ne.Percent = rate
			r.UpdataEntity(currNode.BatchNo, ne, nil)
		}

	} else {
		PrintLog(threadId, "设备不确认...")
	}
	PrintTail(threadId, PUSH_FILE_RESP+fileResp.Sn)
}

/*
	接收设备端反馈的PUSH INFO 确认
	1、确认后，根据结果更新数据表
	2、刷新缓存的结果状态
*/
func CmdPushInfoResp(threadId int, conn *net.TCPConn, infoResp PushInfoResp) {
	PrintHead(threadId, PUSH_INFO_RESP)

	currNode, _ := getCurrNode(threadId, infoResp.Sn)
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

	PrintTail(threadId, PUSH_INFO_RESP)
}

/*
	处理设备的各个指令，根据指令调用各自的函数进行处理
*/
func ProcPacket(threadId int, conn *net.TCPConn, packBuf []byte) error {
	var command Command
	if err := json.Unmarshal(packBuf, &command); err != nil {
		log.Println("错误包===>", err)
		return err
	}
	if command.Method != HEARTBEAT {
		if len(packBuf) < 256 {
			PrintLog(threadId, string(packBuf))
		} else {
			PrintLog(threadId, "Recv Package Len===>", len(packBuf))
		}
	}
	switch command.Method {
	case HEARTBEAT:
		var heart Heartbeat
		if err := json.Unmarshal(packBuf, &heart); err != nil {
			PrintHead(threadId, err)
			conn.Close()
			return err
		}
		CmdHeartBeat(threadId, conn, heart)
	case ONLINE:
		var online Online
		if err := json.Unmarshal(packBuf, &online); err != nil {
			log.Println(err)
			return err
		}
		CmdOnLine(threadId, conn, online)
	case GET_COLOPHON_RESP:
		var phonResp GetColophonResp
		if err := json.Unmarshal(packBuf, &phonResp); err != nil {
			log.Println(err)
			return err
		}
		CmdGetColoPhonResp(threadId, conn, phonResp)
	case GET_INSTLL_DATADRIVE_RESP:
		var getInstDrive GetInstallDataDriveResp
		if err := json.Unmarshal(packBuf, &getInstDrive); err != nil {
			log.Println(err)
			return err
		}
		CmdGetInstallDriveResp(threadId, conn, getInstDrive)
	case POST_INSTLL_DATADRIVE:
		var postInstDrive PostInstallDataDrive
		if err := json.Unmarshal(packBuf, &postInstDrive); err != nil {
			log.Println(err)
			return err
		}
		CmdPostInstallDrive(threadId, conn, postInstDrive)
	case GET_FILE_RESP:
		var getFileResp GetFileResp
		err := json.Unmarshal(packBuf, &getFileResp)
		if err != nil {
			log.Println(err)
			return err
		}
		CmdGetFileResp(threadId, getFileResp)

	case POST_FILE_INFO:
		var postFileInfo PostFileInfo
		err := json.Unmarshal(packBuf, &postFileInfo)
		if err != nil {
			log.Println(err)
			return err
		}
		CmdPostFileInfo(threadId, conn, postFileInfo)
	case POST_FILE:
		var postFile PostFile
		err := json.Unmarshal(packBuf, &postFile)
		if err != nil {
			log.Println(err)
			return err
		}
		CmdPostFile(threadId, conn, postFile)

	case PUSH_FILE_INFO_RESP:
		var infoResp PushFileInfoResp
		if err := json.Unmarshal(packBuf, &infoResp); err != nil {
			log.Println(err)
			return err
		}
		CmdPushFileInfoResp(threadId, conn, infoResp)

	case PUSH_FILE_RESP:
		var fileResp PushFileResp
		if err := json.Unmarshal(packBuf, &fileResp); err != nil {
			log.Println(err)
			return err
		}
		CmdPushFileResp(threadId, conn, fileResp)

	case PUSH_INFO_RESP:
		var infoResp PushInfoResp
		if err := json.Unmarshal(packBuf, &infoResp); err != nil {
			log.Panicln(err)
			return err
		}
		CmdPushInfoResp(threadId, conn, infoResp)

	case CHECK_UDATE:
		var upDate CheckUpdate
		if err := json.Unmarshal(packBuf, &upDate); err != nil {
			log.Println(err)
			return err
		}
		CmdCheckUpdate(threadId, conn, upDate)
	}
	return nil
}

/*
	返回在线数量
*/
func getOnlineCount() (int, int) {
	i := 0
	j := 0

	GConn2SnMap.Range(func(k, v interface{}) bool {
		i++
		return true
	})
	GSn2ConnMap.Range(func(k, v interface{}) bool {
		j++
		return true
	})
	return i, j
}

/*
	网络端口，更新设备为线下
*/
func OffLine(threadId int, sn string, offType string) {
	PrintHead(threadId, OFFLINE, sn)
	r := device.New(dbcomm.GetDB(), device.INFO)
	onlineMap := map[string]interface{}{DEVICE_TIME: time.Now().Format("2006-01-02 15:04:05"),
		IS_ONLINE: STATUS_OFFLINE}
	err := r.UpdateMapEx(sn, onlineMap, nil)
	if err != nil {
		PrintLog(threadId, "设备下线更新失败", err)
	}
	//插入设备在线历史
	rr := onlinehis.New(dbcomm.GetDB(), onlinehis.INFO)
	var ne onlinehis.OnlineHis
	ne.Sn = sn
	ne.ActionType = offType
	ne.DeviceTime = time.Now().Format("2006-01-02 15:04:05")
	if err := rr.InsertEntity(ne, nil); err != nil {
		PrintLog(threadId, err.Error())
	}
	//检查是否有未完成的工作，如果有设置为失败
	rrr := mfiles.New(dbcomm.GetDB(), mfiles.DEBUG)
	var search mfiles.Search
	search.Sn = sn
	search.Status = STATUS_INIT
	e, err := rrr.Get(search)
	if err == nil {
		var mf mfiles.MFiles
		mf.UpdateTime = time.Now().Format("2006-01-02 15:04:05")
		mf.UpdateBy = UPDATE_USER
		mf.EndTime = mf.UpdateTime
		mf.Status = STATUS_FAIL
		if offType == FORCE_OFFLINE {
			mf.FailMsg = "网络中断或设备关闭连接错误"
		} else {
			mf.FailMsg = "终止文件传输"
		}
		rrr.UpdataEntity(e.BatchNo, mf, nil)
		//删除终止引起下载的文件
		rrrr := dfiles.New(dbcomm.GetDB(), dfiles.DEBUG)
		var dsearch dfiles.Search
		dsearch.BatchNo = e.BatchNo
		ee, _ := rrrr.GetList(dsearch)
		for _, vv := range ee {
			PrintLog(threadId, "Remove--->", vv.FileUrl)
			os.Remove(vv.FileUrl)
		}
	}
	//更新明细的结束时间
	PrintTail(threadId, OFFLINE)
}

/*
	程序重启时，初始化设备状态
    逻辑：如果设备超过5分钟没有心跳，则视为下线状态

*/
func InitStatus() {
	PrintHead(MAIN_THREAD, INIT_STATUS)
	r := device.New(dbcomm.GetDB(), device.DEBUG)
	var search device.Search
	if dl, err := r.GetListEx(search); err == nil {
		for _, v := range dl {
			if v.IsOnline == STATUS_ONLINE {
				onlineMap := map[string]interface{}{UPDATE_TIME: time.Now().Format("2006-01-02 15:04:05"),
					IS_ONLINE: STATUS_OFFLINE}
				err = r.UpdateMapEx(v.Sn, onlineMap, nil)
				PrintLog(MAIN_THREAD, v.Sn, "程序重启,设备复位！", v.Sn)
			}
		}
	}
	PrintTail(MAIN_THREAD, INIT_STATUS)
}

/*
	处理TCP部分，是网关程序的核心模块
	1、接收设备发送的各个指令包
	2、按协议进行解析，根据命令进行处理
	3、如果出现超时或网络断开，进行清理处理
*/
func tcpPipe(threadId int, conn *net.TCPConn) {
	ipStr := conn.RemoteAddr().String()
	var err error
	defer func(threadId int, inErr error) {
		PrintLog(threadId, "Disconnect===>:"+ipStr, "Conn:", conn)
		snIf, _ := GConn2SnMap.Load(conn)
		sn, ok := snIf.(string)
		if ok {
			i, j := getOnlineCount()
			PrintLog(threadId, "断线之前在线数量:CONN", i, "SN:", j, sn)
			vv, _ := GSn2ConnMap.Load(sn)
			if node, ok := vv.(StoreInfo); ok == true {
				if node.CurrConn == conn {
					PrintLog(threadId, sn+"连接句柄匹配", node.CurrConn, conn)
					GSn2ConnMap.Delete(sn)
					if strings.Contains(err.Error(), "use of closed network connection") {
						OffLine(threadId, sn, FORCE_OFFLINE)
					} else {
						OffLine(threadId, sn, ACTION_OFFLINE)
					}
				} else {
					PrintLog(threadId, sn+"连接句柄不匹配", node.CurrConn, conn)
				}
			} else {
				PrintLog(threadId, "GSn2ConnMap 非法的连接,断言错误.")
			}
			GConn2SnMap.Delete(conn)
			i, j = getOnlineCount()
			PrintLog(threadId, "断线之后在线数量:CONN", i, "SN:", j, sn)
		} else {
			PrintLog(threadId, "GConn2SnMap非法的连接,断言错误...")
		}
		conn.Close()
	}(threadId, err)

	reader := bufio.NewReader(conn)
	packBuf := make([]byte, 1024*1024*2)

	var nSum int32
	for {
		conn.SetReadDeadline(time.Now().Add(time.Second * 420))
		readBuf := make([]byte, 1024*100)
		var nLen int
		nLen, err = reader.Read(readBuf)
		if err != nil || nLen <= 0 {
			PrintLog(threadId, "Read Error===>", err.Error(), "Len==", nLen)
			return
		}
		conn.SetReadDeadline(time.Time{})
		if nSum == 0 {
			if int(readBuf[0]) != 0x7E {
				err = fmt.Errorf("Error Packet And Close Connection...")
				PrintLog(threadId, "Error Packet And Close Connection...")
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
			if packLen < 6 {
				PrintLog(threadId, "PackLen <6 Error!")
				err = fmt.Errorf("PackLen <6 Error!")
				return
			}
			if nSum >= packLen {
				if err = ProcPacket(threadId, conn, packBuf[6:packLen]); err != nil {
					conn.Close()
					return
				}
				nSum = nSum - packLen
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
	PrintHead(HTTP_THREAD, "WebServer")
	http.HandleFunc("/dgate/busiGetFile", BusiGetFileCtl)
	http.HandleFunc("/dgate/busiPushFile", BusiPushFileCtl)
	http.HandleFunc("/dgate/busiGetVersions", BusiGetVerListCtl)
	http.HandleFunc("/dgate/busiGetDataDrives", BusiGetDataDriveCtl)
	http.HandleFunc("/dgate/busiPushInfo", BusiPushInfoCtl)
	http.HandleFunc("/dgate/busiQueryStatus", BusiQueryStatusCtl)
	http.HandleFunc("/dgate/consoleDetail", SysConsoleDetail)
	http.HandleFunc("/dgate/console", SysConsole)
	http_srv = &http.Server{
		Addr: ":7088",
	}
	if err := http_srv.ListenAndServe(); err != nil {
		log.Printf("listen: %s\n", err)
	}
	PrintTail(0, "WebServer")
}

func init() {
	PrintHead(MAIN_THREAD, "Init...")
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
	c.Get("/config/SAVE_HEARBEAT", &saveHearbeat)

	PrintTail(MAIN_THREAD, "Init...")
}

func main() {
	PrintLog(MAIN_THREAD, "<==========MicroPoint Gate Starting...==========>")
	PrintLog(MAIN_THREAD, "<==========Version:", MainVer, "===================>")
	dbcomm.InitDB(dbUrl, ccdbUrl, idleConns, openConns)

	InitStatus()

	var tcpAddr *net.TCPAddr
	stop_chan := make(chan os.Signal) // 接收系统中断信号
	signal.Notify(stop_chan, syscall.SIGINT, syscall.SIGTERM)

	go go_WebServer()
	///// 观察设备连接
	go func() {
		for {
			i := 0
			GConn2SnMap.Range(func(k, v interface{}) bool {
				i++
				return true
			})
			j := 0
			GSn2ConnMap.Range(func(k, v interface{}) bool {
				if value, ok := v.(StoreInfo); ok == true {
					duration := time.Now().Sub(value.SignInTime)
					sn, _ := k.(string)
					if duration.Minutes() > 6.00 {
						PrintLog(MONITOR_THREAD, "发现超时设备", duration.Minutes(), sn)
					}
				}
				j++
				return true
			})
			PrintLog(MONITOR_THREAD, "当前在线总数 Conn Cnt==>:", i, "Sn Cnt===>:", j)
			time.Sleep(time.Second * 10)
		}
	}()

	tcpAddr, _ = net.ResolveTCPAddr("tcp", ":8089")
	tcpListener, _ := net.ListenTCP("tcp", tcpAddr)

	///// 程序关闭善后处理
	go func(listen *net.TCPListener) {
		<-stop_chan
		listen.Close()
	}(tcpListener)

	defer func() {
		PrintLog(MAIN_THREAD, "退出前准备..")
		GSn2ConnMap.Range(func(k, v interface{}) bool {
			if value, ok := v.(StoreInfo); ok == true {
				value.CurrConn.Close()
			}
			return true
		})
		for {
			i, _ := getOnlineCount()
			if i == 0 {
				PrintLog(MAIN_THREAD, "链接完全释放...")
				break
			}
			PrintLog(MAIN_THREAD, "有链接还未断开...")
			time.Sleep(time.Second * 1)
		}
		PrintLog(MAIN_THREAD, "完全退出！")
	}()
	/////主流程处理
	for {
		tcpConn, err := tcpListener.AcceptTCP()
		if err != nil {
			PrintLog(MAIN_THREAD, "Accept==>", err)
			break
		}
		PrintLog(MAIN_THREAD, "New Conn====>"+tcpConn.RemoteAddr().String())
		rand.Seed(time.Now().UnixNano())
		go tcpPipe(rand.Intn(20000), tcpConn)
	}
}
