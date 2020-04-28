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

	//	"hcd-dgate/util"
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

func Send_Resp(conn *net.TCPConn, resp string) error {
	head := make([]byte, 6)
	head[0] = 0x7E
	head[1] = 0x13
	packLenBytes := IntToBytes(len(resp) + 6)

	log.Println("MsgLen:==Package Head Len===>", len(resp), packLenBytes)
	copy(head[2:], packLenBytes)
	head = append(head, []byte(resp)...)

	var err error
	var sendLen int
	totalLen := len(head)
	nSum := 0
	for totalLen > 0 {
		sendLen, err = conn.Write([]byte(head)[nSum:])
		if err != nil {
			log.Println("Send_Resp Error:", err)
			conn.Close()
			return err
		}
		if totalLen > 512 {
			log.Println("SendPush_File", sendLen, totalLen, nSum)
		} else {
			log.Println("Send Len And Msg===>", string(head))
		}
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
	currNode, _ := getCurrNode(heart.Sn)
	currNode.SignInTime = time.Now()

	//更新设备的最新心跳
	r := device.New(dbcomm.GetDB(), device.DEBUG)

	onlineMap := map[string]interface{}{
		UPDATE_TIME: time.Now().Format("2006-01-02 15:04:05")}
	err = r.UpdateMapEx(heart.Sn, onlineMap, nil)
	if err != nil {
		log.Println("更新心跳失败", err)
	}

	GSn2ConnMap.Store(heart.Sn, currNode)
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
			IS_ONLINE: STATUS_ONLINE, DEVICE_TIME: time.Now().Format("2006-01-02 15:04:05")}
		err = r.UpdateMap(fmt.Sprintf("%d", e.Id), onlineMap, nil)
		if err != nil {
			log.Println("更新失败", err)
		}
	} else {
		var e device.Device
		e.IsOnline = STATUS_ONLINE
		e.DeviceTime = time.Now().Format("2006-01-02 15:04:05")
		e.Sn = online.Devices[0].Sn
		e.IsEnable = 1
		e.FcdClass = "C"
		e.ChipId = online.Devices[0].Chip_id
		e.ProductType = online.Devices[0].Device_series
		e.ProductNo = online.Devices[0].Device_name
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

	PrintTail(GET_COLOPHON_RESP)
}

func CmdGetInstallDriveResp(conn *net.TCPConn, getDriveResp GetInstallDataDriveResp) {
	PrintHead(GET_INSTLL_DATADRIVE_RESP)
	log.Println(getDriveResp)

	currNode, _ := getCurrNode(getDriveResp.Sn)
	rr := mfiles.New(dbcomm.GetDB(), mfiles.DEBUG)
	var ne mfiles.MFiles
	if !getDriveResp.Success || getDriveResp.Total_cnt == 0 {
		ne.Status = STATUS_SUCC
		ne.TodoCount = 0
		ne.EndTime = time.Now().Format("2006-01-02 15:04:05")
		ne.UpdateTime = ne.EndTime

		currNode.Status = STATUS_INIT
		GSn2ConnMap.Store(getDriveResp.Sn, currNode)
		rr.UpdataEntity(currNode.BatchNo, ne, nil)
	}

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
		e.ProductDate = v.Create_time
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
	respBuf, _ := json.Marshal(&driveResp)

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
	//增加类型断言处理，微点有些设备传的是布尔，有些设备传的是字符串
	var eofTag = false
	switch t := postFile.Fragment.Eof.(type) {
	default:
		fmt.Printf("unexpected type %T", t)
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

	log.Println("Push File Info OK...")

	currNode, _ := getCurrNode(infoResp.Sn)
	_, crcCode := pubPushFile(conn, infoResp.Sn, infoResp.Chip_id)

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
	upResp.Type = upDate.Type
	if upBuf, err := json.Marshal(&upResp); err != nil {
		log.Println(err)
	} else {
		Send_Resp(conn, string(upBuf))
	}

	PrintTail(CHECK_UDATE)
}

func pubPushFile(conn *net.TCPConn, sn string, chipId string) (error, int32) {

	var pushFile PushFile
	currNode, _ := getCurrNode(sn)

	if currNode.FileSize <= currNode.FileOffset+FILE_BLOCK_SIZE {
		pushFile.Fragment.Eof = true
		currNode.ReadSize = currNode.FileSize - currNode.FileOffset
		log.Println("File eof block will happen!===>", sn, currNode.FileOffset, currNode.ReadSize)

	} else {
		pushFile.Fragment.Eof = false
		currNode.ReadSize = FILE_BLOCK_SIZE
		log.Println("FileOffSet===>", sn, currNode.FileOffset, currNode.ReadSize, currNode.FileIndex)
	}
	pushFile.Fragment.Length = int(currNode.ReadSize)
	pushFile.Chip_id = chipId
	pushFile.Method = PUSH_FILE
	pushFile.Sn = sn
	pushFile.Fragment.Index = currNode.FileIndex
	fileBuf, err := ioutil.ReadFile(currNode.FileName)
	if err != nil {
		log.Println(err)
		return err, 0
	}
	pushFile.Fragment.Source = hex.EncodeToString(fileBuf[currNode.FileOffset : currNode.FileOffset+currNode.ReadSize])
	crc32 := softwareCrc32(fileBuf, len(fileBuf))
	pushFile.Fragment.Checksum = crc32
	fBuf, _ := json.Marshal(&pushFile)
	err = Send_Resp(conn, string(fBuf))

	GSn2ConnMap.Store(sn, currNode)
	return err, crc32

}

/*
	接收客户端发来的PUSH文件确认
*/
func CmdPushFileResp(conn *net.TCPConn, fileResp PushFileResp) {
	PrintHead(PUSH_FILE_RESP)
	currNode, _ := getCurrNode(fileResp.Sn)
	if fileResp.Success {
		log.Println("Recv Push File Resp===>")

		currNode.FileOffset += currNode.ReadSize
		currNode.FileIndex += 1
		GSn2ConnMap.Store(fileResp.Sn, currNode)

		if currNode.FileOffset == currNode.FileSize {
			log.Println("File Eof==>", currNode.FileOffset, currNode.FileSize)
			r := mfiles.New(dbcomm.GetDB(), mfiles.DEBUG)
			var e mfiles.MFiles
			e.UpdateBy = UPDATE_USER
			e.UpdateTime = time.Now().Format("2006-01-02 15:04:05")
			r.UpdataEntity(currNode.BatchNo, e, nil)
			//更新明细的结束时间
			rr := dfiles.New(dbcomm.GetDB(), dfiles.DEBUG)
			var de dfiles.DFiles
			de.EndTime = time.Now().Format("2006-01-02 15:04:05")
			de.UpdateTime = de.EndTime
			de.UpdateBy = UPDATE_USER
			rr.UpdataEntity(currNode.BatchNo, de, nil)
		} else {
			pubPushFile(conn, fileResp.Sn, fileResp.Chip_id)
		}
	}
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

/*
	处理设备的各个指令，根据指令调用各自的函数进行处理
*/
func ProcPacket(conn *net.TCPConn, packBuf []byte) error {
	var command Command
	if err := json.Unmarshal(packBuf, &command); err != nil {
		log.Println(err)
	}
	switch command.Method {
	case HEARTBEAT:
		var heart Heartbeat
		if err := json.Unmarshal(packBuf, &heart); err != nil {
			log.Println(err)
			return err
		}
		CmdHeartBeat(conn, heart)
	case ONLINE:
		var online Online
		if err := json.Unmarshal(packBuf, &online); err != nil {
			log.Println(err)
			return err
		}
		CmdOnLine(conn, online)
	case GET_COLOPHON_RESP:
		var phonResp GetColophonResp
		if err := json.Unmarshal(packBuf, &phonResp); err != nil {
			log.Println(err)
			return err
		}
		CmdGetColoPhonResp(conn, phonResp)
	case GET_INSTLL_DATADRIVE_RESP:
		var getInstDrive GetInstallDataDriveResp
		if err := json.Unmarshal(packBuf, &getInstDrive); err != nil {
			log.Println(err)
			return err
		}
		CmdGetInstallDriveResp(conn, getInstDrive)
	case POST_INSTLL_DATADRIVE:
		var postInstDrive PostInstallDataDrive
		if err := json.Unmarshal(packBuf, &postInstDrive); err != nil {
			log.Println(err)
			return err
		}
		CmdPostInstallDrive(conn, postInstDrive)
	case GET_FILE_RESP:
		var getFileResp GetFileResp
		err := json.Unmarshal(packBuf, &getFileResp)
		if err != nil {
			log.Println(err)
			return err
		}
		CmdGetFileResp(getFileResp)

	case POST_FILE_INFO:
		var postFileInfo PostFileInfo
		err := json.Unmarshal(packBuf, &postFileInfo)
		if err != nil {
			log.Println(err)
			return err
		}
		CmdPostFileInfo(conn, postFileInfo)
	case POST_FILE:
		var postFile PostFile
		err := json.Unmarshal(packBuf, &postFile)
		if err != nil {
			log.Println(err)
			return err
		}
		CmdPostFile(conn, postFile)

	case PUSH_FILE_INFO_RESP:
		var infoResp PushFileInfoResp
		if err := json.Unmarshal(packBuf, &infoResp); err != nil {
			log.Println(err)
			return err
		}
		CmdPushFileInfoResp(conn, infoResp)

	case PUSH_FILE_RESP:
		var fileResp PushFileResp
		if err := json.Unmarshal(packBuf, &fileResp); err != nil {
			log.Println(err)
			return err
		}
		CmdPushFileResp(conn, fileResp)

	case PUSH_INFO_RESP:
		var infoResp PushInfoResp
		if err := json.Unmarshal(packBuf, &infoResp); err != nil {
			log.Panicln(err)
			return err
		}
		CmdPushInfoResp(conn, infoResp)

	case CHECK_UDATE:
		var upDate CheckUpdate
		if err := json.Unmarshal(packBuf, &upDate); err != nil {
			log.Println(err)
			return err
		}
		CmdCheckUpdate(conn, upDate)
	}
	return nil
}

/*
	网络端口，更新设备为线下

*/
func OffLine(sn string) {
	PrintHead(OFFLINE, sn)

	r := device.New(dbcomm.GetDB(), device.DEBUG)
	onlineMap := map[string]interface{}{DEVICE_TIME: time.Now().Format("2006-01-02 15:04:05"),
		IS_ONLINE: STATUS_OFFLINE}
	err := r.UpdateMapEx(sn, onlineMap, nil)
	if err != nil {
		log.Println("设备下线更新失败", err)
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

	//检查是否有未完成的工作，如果有设置为失败
	rrr := mfiles.New(dbcomm.GetDB(), mfiles.DEBUG)
	var search mfiles.Search
	search.Sn = sn
	search.Status = STATUS_INIT
	e, err := rrr.Get(search)
	if err == nil {
		var mf mfiles.MFiles
		if e.TodoCount > e.DoneCount {
			mf.UpdateTime = time.Now().Format("2006-01-02 15:04:05")
			mf.UpdateBy = UPDATE_USER
			mf.EndTime = mf.UpdateTime
			mf.Status = STATUS_FAIL
			mf.FailMsg = "网络中断错误"
			rrr.UpdataEntity(e.BatchNo, mf, nil)
		}
	}
	//更新明细的结束时间

	PrintTail(OFFLINE)
}

/*
	程序重启时，初始化设备状态
    逻辑：如果设备超过5分钟没有心跳，则视为下线状态

*/
func InitStatus() {
	PrintHead(INIT_STATUS)
	r := device.New(dbcomm.GetDB(), device.DEBUG)
	var search device.Search
	if dl, err := r.GetListEx(search); err == nil {
		for k, v := range dl {
			log.Println(k, v.Sn, v.UpdateTime)
			if v.UpdateTime == "" {
				continue
			}
			utime, _ := time.ParseInLocation("2006-01-02 15:04:05", v.UpdateTime, time.Local)
			duration := time.Now().Sub(utime)
			if duration.Minutes() < 5.00 {
				continue
			}
			onlineMap := map[string]interface{}{DEVICE_TIME: time.Now().Format("2006-01-02 15:04:05"),
				IS_ONLINE: STATUS_OFFLINE}
			err = r.UpdateMapEx(v.Sn, onlineMap, nil)
			if err != nil {
				log.Println("设备下线更新失败", err)
			} else {
				log.Println(v.Sn, "虚假在线，更新为下线状态")
			}
		}
	}

	PrintTail(INIT_STATUS)
}

/*
	处理TCP部分，是网关程序的核心模块
	1、接收设备发送的各个指令包
	2、按协议进行解析，根据命令进行处理
	3、如果出现超时或网络断开，进行清理处理
*/
func tcpPipe(conn *net.TCPConn) {
	ipStr := conn.RemoteAddr().String()
	defer func() {
		log.Println("Disconnect===>:"+ipStr, "Conn:", conn)
		snIf, ok := GConn2SnMap.Load(conn)
		if ok {
			sn, ok := snIf.(string)
			if !ok {
				log.Println("Assertion is  error")
			}
			if sn != "" {
				OffLine(sn)
				GSn2ConnMap.Delete(sn)
			}
			if conn != nil {
				GConn2SnMap.Delete(conn)
			}
		} else {
			log.Println("GConn2SnMap.Load Error....")
		}
		log.Println("")
		conn.Close()
	}()

	reader := bufio.NewReader(conn)
	packBuf := make([]byte, 1024*1024*2)
	var nSum int32
	for {
		conn.SetReadDeadline(time.Now().Add(time.Second * 180))
		readBuf := make([]byte, 1024*100)
		var nLen int
		nLen, err := reader.Read(readBuf)
		log.Println("Recv Len==", nLen, string(readBuf[0:nLen]))
		if err != nil || nLen <= 0 {
			log.Println(err)
			return
		}
		//cancel timeout
		conn.SetReadDeadline(time.Time{})
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
			log.Println("包的长度==》", packLen)
			if nSum >= packLen {
				log.Println("接收到一个完整的包!", packBuf[0], packBuf[1])
				if err := ProcPacket(conn, packBuf[6:packLen]); err != nil {
					conn.Close()
					return
				}
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
	PrintHead("WebServer")
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
	PrintTail("WebServer")
}

func init() {
	PrintHead("Init...")
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
	PrintTail("Init...")
}

func main() {
	log.Println("<==========MicroPoint Gate Starting...==========>")
	log.Println("<==========Version:", MainVer, "===================>")

	dbcomm.InitDB(dbUrl, ccdbUrl, idleConns, openConns)
	log.Println("<==========Init Device Online Status==========>")

	InitStatus()

	go go_WebServer()

	go func() {
		for {
			i := 0
			GConn2SnMap.Range(func(k, v interface{}) bool {
				i++
				return true
			})
			log.Println("当前在线总数============>:", i)
			time.Sleep(time.Second * 10)
		}
	}()

	var tcpAddr *net.TCPAddr
	tcpAddr, _ = net.ResolveTCPAddr("tcp", ":8089")
	tcpListener, _ := net.ListenTCP("tcp", tcpAddr)
	defer tcpListener.Close()
	for {
		tcpConn, err := tcpListener.AcceptTCP()
		if err != nil {
			continue
		}
		log.Println("Has A New Connection===>:" + tcpConn.RemoteAddr().String())
		GConn2SnMap.Store(tcpConn, "")
		go tcpPipe(tcpConn)
	}

}
