package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hcd-dgate/service/dbcomm"
	"hcd-dgate/service/mfile"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"path"
	"time"
)

const ONLINE = "online"
const ONLINE_RESP = "online"

const GET_COLOPHON = "get_colophon"
const GET_COLOPHON_RESP = "get_colophon"

const GET_INSTLL_DATADRIVE = "get_install_datadrive"
const GET_INSTLL_DATADRIVE_RESP = "get_install_datadrive"

const POST_INSTLL_DATADRIVE = "post_install_datadrive"
const POST_INSTLL_DATADRIVE_RESP = "post_install_datadrive"

const GET_FILE = "get_file"
const GET_FILE_RESP = "get_file"

const POST_FILE_INFO = "post_file_info"
const POST_FILE_INFO_RESP = "post_file_info"
const POST_FILE = "post_file"
const POST_FILE_RESP = "post_file"

const PUSH_FILE_INFO = "push_file_info"
const PUSH_FILE_INFO_RESP = "push_file_info"
const PUSH_FILE = "push_file"
const PUSH_FILE_RESP = "push_file"

const PUSH_INFO = "push_info"
const PUSH_INFO_RESP = "push_info"

const CHECK_UDATE = "check_update"
const CHECK_UDATE_RESP = "check_update"

const HEARTBEAT = "heartbeat"
const HEARTBEAT_RESP = "heartbeat"

const QUERY_STATUS = "query_status"
const QUERY_STATUS_RESP = "query_status_resp"

const OFFLINE = "off_line"

const TYPE_CHIP = "chip"
const TYPE_UPGRADE = "upgrade"
const TYPE_CONFIG = "config"
const TYPE_RESULT = "result"
const TYPE_RAW = "raw"
const TYPE_LOG = "log"

const CMDTYPE_GET = "getfile"
const CMDTYPE_PUSH = "pushfile"
const CMDTYPE_INFO = "pushinfo"
const CMDTYPE_VER = "getver"
const CMDTYPE_DRIVE = "getdrive"

const ACTION_ONLINE = "ON"
const ACTION_OFFLINE = "OFF"

const STATUS_INIT = "I"
const STATUS_DOING = "D"

const STATUS_SUCC = "S"
const STATUS_FAIL = "F"

const HEAD_LEN = 6
const UPDATE_TIME = "update_time"
const DEVICE_TIME = "device_time"
const IS_ONLINE = "is_online"
const ACTION_TYPE = "action_type"
const DEFAULT_PATH = "/data/app/hcd/tmp/"

const UPDATE_USER = 9000000

const ERR_CODE_SUCCESS = "0000"
const ERR_CODE_DBERROR = "1001"
const ERR_CODE_JSONERR = "2001"
const ERR_CODE_STATUSD = "3001"
const ERR_CODE_TYPEERR = "4000"
const ERR_CODE_STATUS = "5000"
const ERR_CODE_NOTEXIST = "5020"

var (
	ERROR_MAP map[string]string = map[string]string{
		ERR_CODE_SUCCESS:  "执行成功:",
		ERR_CODE_DBERROR:  "DB执行错误:",
		ERR_CODE_JSONERR:  "JSON格式错误:",
		ERR_CODE_TYPEERR:  "类型转换错误:",
		ERR_CODE_STATUSD:  "正在处理中",
		ERR_CODE_NOTEXIST: "文件不存在",
		ERR_CODE_STATUS:   "状态不正确:",
	}
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

/*
	SEESION存储信息
*/
type StoreInfo struct {
	CurrConn   *net.TCPConn
	SignInTime time.Time
	BatchNo    string
	Status     string
	FileIndex  int
	FileName   string
	FileSize   int64
	FileCrc32  int
}

/*
	设备定义
*/
type Device struct {
	Potocol       string `json:"protocol"`
	Device_series string `json:"device_series"`
	Device_name   string `json:"device_name"`
	Device_ver    string `json:"device_ver"`
	Hw_ver        string `json:"hw_ver"`
	Sw_ver        string `json:"sw_ver"`
	Sn            string `json:"sn"`
	Chip_id       string `json:"chip_id"`
	Device_time   string `json:"device_time"`
}

/*
	在线命令定义
*/
type Command struct {
	Method string `json:"method"`
}

/*
	在线命令定义
*/
type Online struct {
	Method  string   `json:"method"`
	Gate    string   `json:"gate"`
	Ip      string   `json:"ip"`
	Dev_cnt int      `json:"dev_cnt"`
	Devices []Device `json:"devices"`
}

type OnlineResp struct {
	Method  string `json:"method"`
	Sn      string `json:"sn"`
	Chip_id string `json:"chip_id"`
	Success bool   `json:"success"`
}

type Version struct {
	Sw_ver       string `json:"sw_ver"`
	Upgrade_time string `json:"upgrade_time"`
	Channel      string `json:"channel"`
}

type GetColophonResp struct {
	Method   string    `json:"method"`
	Sn       string    `json:"sn"`
	Chip_id  string    `json:"chip_id"`
	Success  bool      `json:"success"`
	Versions []Version `json:"versions"`
}

type GetColophon struct {
	Method  string `json:"method"`
	Sn      string `json:"sn"`
	Chip_id string `json:"chip_id"`
}

type GetInstallDataDriveResp struct {
	Method    string `json:"method"`
	Sn        string `json:"sn"`
	Chip_id   string `json:"chip_id"`
	Success   bool   `json:"success"`
	Total_cnt int    `json:"total_cnt"`
}

type GetInstallDataDrive struct {
	Method  string `json:"method"`
	Sn      string `json:"sn"`
	Chip_id string `json:"chip_id"`
}

type Datadrive struct {
	Lot          string `json:"lot"`
	Create_time  string `json:"create_time"`
	Install_time string `json:"install_time"`
}

type PostInstallDataDriveResp struct {
	Method      string `json:"method"`
	Sn          string `json:"sn"`
	Chip_id     string `json:"chip_id"`
	Start_index int    `json:"start_index"`
	Dd_cnt      int    `json:"dd_cnt"`
	Success     bool   `json:"success"`
}

type PostInstallDataDrive struct {
	Method      string      `json:"method"`
	Sn          string      `json:"sn"`
	Chip_id     string      `json:"chip_id"`
	Start_index int         `json:"start_index"`
	Dd_cnt      int         `json:"dd_cnt"`
	Datadrive   []Datadrive `json:"datadrive"`
}

type GetFile struct {
	Method  string `json:"method"`
	Sn      string `json:"sn"`
	Chip_id string `json:"chip_id"`
	Type    string `json:"type"`
	Range   string `json:"range"`
	Count   int    `json:"count"`
	From    string `json:"from"`
	To      string `json:"to"`
}

type GetFileResp struct {
	Method     string `json:"method"`
	Sn         string `json:"sn"`
	Chip_id    string `json:"chip_id"`
	Type       string `json:"type"`
	Total_file int    `json:"total_file"`
	Total_size int    `json:"total_size"`
	Success    bool   `json:"success"`
}

type File struct {
	Name     string `json:"name"`
	Length   int    `json:"length"`
	File_crc int32  `json:"file_crc"`
}

type PostFileInfo struct {
	Method            string `json:"method"`
	Sn                string `json:"sn"`
	Chip_id           string `json:"chip_id"`
	Type              string `json:"type"`
	Total_file        int    `json:"total_file"`
	File_in_procesing int    `json:"file_in_procesing"`
	File              File   `json:"file"`
}

type PostFileInfoResp struct {
	Method            string `json:"method"`
	Sn                string `json:"sn"`
	Chip_id           string `json:"chip_id"`
	Type              string `json:"type"`
	Total_file        int    `json:"total_file"`
	File_in_procesing int    `json:"file_in_procesing"`
	Success           bool   `json:"success"`
}

type Fragment struct {
	Index    int    `json:"index"`
	Eof      bool   `json:"eof"`
	Checksum int32  `json:"checksum"`
	Length   int    `json:"length"`
	Source   string `json:"source"`
}

type PostFile struct {
	Method   string   `json:"method"`
	Sn       string   `json:"sn"`
	Chip_id  string   `json:"chip_id"`
	Fragment Fragment `json:"fragment"`
}

type PostFileResp struct {
	Method  string `json:"method"`
	Sn      string `json:"sn"`
	Chip_id string `json:"chip_id"`
	Index   int    `json:"index"`
	Success bool   `json:"success"`
}

type PushFileInfo struct {
	Method            string `json:"method"`
	Sn                string `json:"sn"`
	Chip_id           string `json:"chip_id"`
	Type              string `json:"type"`
	Total_file        int    `json:"total_file"`
	File_in_procesing int    `json:"file_in_procesing"`
	File              File   `json:"file"`
}

type PushFileInfoResp struct {
	Method            string `json:"method"`
	Sn                string `json:"sn"`
	Chip_id           string `json:"chip_id"`
	Type              string `json:"type"`
	Total_file        int    `json:"total_file"`
	File_in_procesing int    `json:"file_in_procesing"`
	Success           bool   `json:"success"`
}

type PushFile struct {
	Method   string   `json:"method"`
	Sn       string   `json:"sn"`
	Chip_id  string   `json:"chip_id"`
	Fragment Fragment `json:"fragment"`
}

type PushFileResp struct {
	Method  string `json:"method"`
	Sn      string `json:"sn"`
	Chip_id string `json:"chip_id"`
	Index   int    `json:"index"`
	Success bool   `json:"success"`
}

type PushInfo struct {
	Method  string `json:"method"`
	Sn      string `json:"sn"`
	Chip_id string `json:"chip_id"`
	Type    string `json:"type"`
	Purpose string `json:"purpose"`
	Info    string `json:"info"`
}

type PushInfoResp struct {
	Method  string `json:"method"`
	Sn      string `json:"sn"`
	Chip_id string `json:"chip_id"`
	Type    string `json:"type"`
	Purpose bool   `json:"purpose"`
	Confirm string `json:"confirm"`
	Success bool   `json:"success"`
}

type CheckUpdate struct {
	Method  string `json:"method"`
	Sn      string `json:"sn"`
	Chip_id string `json:"chip_id"`
	Type    string `json:"type"`
}

type CheckUpdateResp struct {
	Method  string `json:"method"`
	Sn      string `json:"sn"`
	Chip_id string `json:"chip_id"`
	Type    string `json:"type"`
	Success bool   `json:"success"`
}

type Heartbeat struct {
	Method  string `json:"method"`
	Sn      string `json:"sn"`
	Chip_id string `json:"chip_id"`
}

type HeartbeatResp struct {
	Method  string `json:"method"`
	Sn      string `json:"sn"`
	Chip_id string `json:"chip_id"`
	Success bool   `json:"success"`
}

type BusiPushFile struct {
	UserId  int64  `json:"user_id"`
	Sn      string `json:"sn"`
	Chip_id string `json:"chip_id"`
	Type    string `json:"type"`
	Name    string `json:"name"`
	Length  int    `json:"length"`
}

type BusiPushFileResp struct {
	No        string `json:"no"`
	ErrorCode string `json:"err_code"`
	ErrorMsg  string `json:"err_msg"`
}

type BusiPushInfo struct {
	UserId  int64  `json:"user_id"`
	Sn      string `json:"sn"`
	Chip_id string `json:"chip_id"`
	Type    string `json:"type"`
	Purpose string `json:"purpose"`
	Info    string `json:"info"`
}

type BusiPushInfoResp struct {
	No        string `json:"no"`
	ErrorCode string `json:"err_code"`
	ErrorMsg  string `json:"err_msg"`
}

type BusiGetFile struct {
	UserId  int64  `json:"user_id"`
	Sn      string `json:"sn"`
	Chip_id string `json:"chip_id"`
	Type    string `json:"type"`
	Range   string `json:"range"`
	Count   int    `json:"count"`
	From    string `json:"from"`
	To      string `json:"to"`
}

type BusiGetFileResp struct {
	No        string `json:"no"`
	ErrorCode string `json:"err_code"`
	ErrorMsg  string `json:"err_msg"`
}

type BusiGetColophon struct {
	UserId  int64  `json:"user_id"`
	Sn      string `json:"sn"`
	Chip_id string `json:"chip_id"`
}

type BusiGetColophonResp struct {
	No        string `json:"no"`
	ErrorCode string `json:"err_code"`
	ErrorMsg  string `json:"err_msg"`
}

type BusiGetDataDrive struct {
	UserId  int64  `json:"user_id"`
	Sn      string `json:"sn"`
	Chip_id string `json:"chip_id"`
}

type BusiGetDataDriveResp struct {
	No        string `json:"no"`
	ErrorCode string `json:"err_code"`
	ErrorMsg  string `json:"err msg"`
}

type BusiQueryStatus struct {
	UserId int64  `json:"user_id"`
	No     string `json:"sn"`
}

type BusiQueryStatusResp struct {
	No        string `json:"no"`
	Status    string `json:"status"`
	ErrorCode string `json:"err_code"`
	ErrorMsg  string `json:"err_msg"`
}

/*
	获取业务指令，触发向机器发送获取文件指令
	1、基础校验（B、查看是否空闲，A、参数是否正确）
	2、插入MFILES 主表
	3、向设备发送指令
*/

func BusiGetFileCtl(w http.ResponseWriter, req *http.Request) {
	PrintHead(GET_FILE)

	var busiFile BusiGetFile
	var busiFileResp BusiGetFileResp
	reqBuf, err := ioutil.ReadAll(req.Body)
	if err = json.Unmarshal(reqBuf, &busiFile); err != nil {
		busiFileResp.ErrorCode = ERR_CODE_JSONERR
		busiFileResp.ErrorMsg = err.Error()
		Write_Response(busiFileResp, w, req, GET_FILE)
		return
	}
	defer req.Body.Close()

	currNode, err := getCurrNode(busiFile.Sn)
	if err != nil {
		busiFileResp.ErrorCode = ERR_CODE_TYPEERR
		busiFileResp.ErrorMsg = err.Error()
		Write_Response(busiFileResp, w, req, GET_FILE)
		return
	}

	if currNode.Status == STATUS_DOING {
		busiFileResp.ErrorCode = ERR_CODE_STATUSD
		busiFileResp.ErrorMsg = ERROR_MAP[ERR_CODE_STATUSD]
		Write_Response(busiFileResp, w, req, GET_FILE)
		return
	}
	var e mfiles.MFiles
	r := mfiles.New(dbcomm.GetDB(), mfiles.DEBUG)
	e.BatchNo = fmt.Sprintf("%d", time.Now().UnixNano())
	e.ChipId = busiFile.Chip_id
	e.Sn = busiFile.Sn
	e.Type = busiFile.Type
	e.FromDate = busiFile.From
	e.CreateBy = busiFile.UserId
	e.ToDate = busiFile.To
	e.CmdType = CMDTYPE_GET
	e.Frange = busiFile.Range
	e.StartTime = time.Now().Format("2006-01-02 15:04:05")
	if err := r.InsertEntity(e, nil); err != nil {
		busiFileResp.ErrorCode = ERR_CODE_DBERROR
		busiFileResp.ErrorMsg = err.Error()
		Write_Response(busiFileResp, w, req, GET_FILE)
		return
	}
	var getFile GetFile
	getFile.Method = GET_FILE
	getFile.Chip_id = busiFile.Chip_id
	getFile.Sn = busiFile.Sn
	getFile.Type = busiFile.Type
	getFile.Range = busiFile.Range
	getFile.Count = busiFile.Count
	getFile.From = busiFile.From
	getFile.To = busiFile.To
	getBuf, _ := json.Marshal(getFile)
	//发起指令
	Send_Resp(currNode.CurrConn, string(getBuf))

	currNode.BatchNo = e.BatchNo
	currNode.Status = STATUS_DOING
	GSn2ConnMap.Store(getFile.Sn, currNode)

	busiFileResp.No = e.BatchNo
	busiFileResp.ErrorCode = ERR_CODE_SUCCESS
	busiFileResp.ErrorMsg = ERROR_MAP[ERR_CODE_SUCCESS]
	Write_Response(busiFileResp, w, req, GET_FILE)
}

/*
 下发文件的到设备的指令控制
*/

func BusiPushFileCtl(w http.ResponseWriter, req *http.Request) {
	PrintHead(PUSH_FILE_INFO)

	var pushFile BusiPushFile
	var pushFileResp BusiPushFileResp
	reqBuf, err := ioutil.ReadAll(req.Body)
	err = json.Unmarshal(reqBuf, &pushFile)
	if err != nil {
		pushFileResp.ErrorCode = ERR_CODE_JSONERR
		pushFileResp.ErrorMsg = err.Error()
		Write_Response(pushFileResp, w, req, PUSH_FILE_INFO)
		return
	}
	defer req.Body.Close()
	currNode, err := getCurrNode(pushFile.Sn)
	if err != nil {
		pushFileResp.ErrorCode = ERR_CODE_TYPEERR
		pushFileResp.ErrorMsg = err.Error()
		Write_Response(pushFileResp, w, req, PUSH_FILE_INFO)
		return
	}
	if pushFile.Type != "chip" && pushFile.Type != "upgrade" && pushFile.Type != "config" {
		pushFileResp.ErrorCode = ERR_CODE_TYPEERR
		pushFileResp.ErrorMsg = "文件类型错误"
		Write_Response(pushFileResp, w, req, PUSH_FILE_INFO)
		return
	}

	//检查文件是否存在
	stat, err := os.Stat(pushFile.Name)
	if err != nil {
		if os.IsNotExist(err) {
			pushFileResp.ErrorCode = ERR_CODE_NOTEXIST
			pushFileResp.ErrorMsg = pushFile.Name + "文件不存在！"
			Write_Response(pushFileResp, w, req, PUSH_FILE_INFO)
			return
		}
	}
	currNode.FileSize = stat.Size()
	currNode.FileName = pushFile.Name
	fileBuf, err := ioutil.ReadFile(pushFile.Name)
	fileCrc32 := softwareCrc32(fileBuf, len(fileBuf))

	if currNode.Status == STATUS_DOING {
		pushFileResp.ErrorCode = ERR_CODE_STATUSD
		pushFileResp.ErrorMsg = ERROR_MAP[ERR_CODE_STATUSD]
		Write_Response(pushFileResp, w, req, PUSH_FILE_INFO)
		return
	}

	var e mfiles.MFiles
	r := mfiles.New(dbcomm.GetDB(), mfiles.DEBUG)
	e.BatchNo = fmt.Sprintf("%d", time.Now().UnixNano())
	e.ChipId = pushFile.Chip_id
	e.Sn = pushFile.Sn
	e.Type = pushFile.Type
	e.CreateBy = pushFile.UserId
	e.CmdType = CMDTYPE_PUSH
	e.StartTime = time.Now().Format("2006-01-02 15:04:05")
	if err := r.InsertEntity(e, nil); err != nil {
		pushFileResp.ErrorCode = ERR_CODE_DBERROR
		pushFileResp.ErrorMsg = err.Error()
		Write_Response(pushFileResp, w, req, PUSH_FILE_INFO)
		return
	}

	var info PushFileInfo
	info.Method = PUSH_FILE_INFO
	info.Chip_id = pushFile.Chip_id
	info.Sn = pushFile.Sn
	info.Total_file = 1
	info.Type = pushFile.Type
	info.File_in_procesing = 1
	info.File = File{Name: path.Base(pushFile.Name), Length: int(currNode.FileSize), File_crc: fileCrc32}

	infoBuf, _ := json.Marshal(info)
	Send_Resp(currNode.CurrConn, string(infoBuf))

	currNode.BatchNo = e.BatchNo
	currNode.Status = STATUS_DOING
	GSn2ConnMap.Store(pushFile.Sn, currNode)

	pushFileResp.No = e.BatchNo
	pushFileResp.ErrorCode = ERR_CODE_SUCCESS
	pushFileResp.ErrorMsg = ERROR_MAP[ERR_CODE_SUCCESS]
	Write_Response(pushFileResp, w, req, PUSH_FILE_INFO)

}

/*
 下发文件的到设备的指令控制
*/

func BusiPushInfoCtl(w http.ResponseWriter, req *http.Request) {
	PrintHead(PUSH_INFO)

	var busiInfo BusiPushInfo
	var busiInfoResp BusiPushInfoResp
	reqBuf, err := ioutil.ReadAll(req.Body)
	err = json.Unmarshal(reqBuf, &busiInfo)
	if err != nil {
		busiInfoResp.ErrorCode = ERR_CODE_JSONERR
		busiInfoResp.ErrorMsg = err.Error()
		Write_Response(busiInfoResp, w, req, PUSH_INFO)
		return
	}
	defer req.Body.Close()
	currNode, err := getCurrNode(busiInfo.Sn)
	if err != nil {
		busiInfoResp.ErrorCode = ERR_CODE_TYPEERR
		busiInfoResp.ErrorMsg = err.Error()
		Write_Response(busiInfoResp, w, req, PUSH_INFO)
		return
	}

	if busiInfo.Type != "chip" && busiInfo.Type != "upgrade" && busiInfo.Type != "config" && busiInfo.Type != "private" {
		busiInfoResp.ErrorCode = ERR_CODE_TYPEERR
		busiInfoResp.ErrorMsg = "消息类型错误"
		Write_Response(busiInfoResp, w, req, PUSH_FILE_INFO)
		return
	}

	if busiInfo.Purpose != "update" && busiInfo.Purpose != "agreement" {
		busiInfoResp.ErrorCode = ERR_CODE_TYPEERR
		busiInfoResp.ErrorMsg = "消息目的类型错误"
		Write_Response(busiInfoResp, w, req, PUSH_FILE_INFO)
		return
	}

	if currNode.Status == STATUS_DOING {
		busiInfoResp.ErrorCode = ERR_CODE_STATUSD
		busiInfoResp.ErrorMsg = ERROR_MAP[ERR_CODE_STATUSD]
		Write_Response(busiInfoResp, w, req, PUSH_INFO)
		return
	}
	//数据落地
	var e mfiles.MFiles
	r := mfiles.New(dbcomm.GetDB(), mfiles.DEBUG)
	e.BatchNo = fmt.Sprintf("%d", time.Now().UnixNano())
	e.ChipId = busiInfo.Chip_id
	e.Sn = busiInfo.Sn
	e.Type = busiInfo.Type
	e.CreateBy = busiInfo.UserId
	e.CmdType = CMDTYPE_INFO
	e.Purpose = busiInfo.Purpose
	e.InfoMsg = busiInfo.Info
	e.StartTime = time.Now().Format("2006-01-02 15:04:05")
	if err := r.InsertEntity(e, nil); err != nil {
		busiInfoResp.ErrorCode = ERR_CODE_DBERROR
		busiInfoResp.ErrorMsg = err.Error()
		Write_Response(busiInfoResp, w, req, PUSH_INFO)
		return
	}

	var info PushInfo
	info.Method = PUSH_INFO
	info.Chip_id = busiInfo.Chip_id
	info.Sn = busiInfo.Sn
	info.Purpose = busiInfo.Purpose
	info.Type = busiInfo.Type
	info.Info = busiInfo.Info

	infoBuf, _ := json.Marshal(info)
	Send_Resp(currNode.CurrConn, string(infoBuf))
	currNode.BatchNo = e.BatchNo
	currNode.Status = STATUS_DOING
	GSn2ConnMap.Store(busiInfo.Sn, currNode)

	busiInfoResp.No = e.BatchNo
	busiInfoResp.ErrorCode = ERR_CODE_SUCCESS
	busiInfoResp.ErrorMsg = ERROR_MAP[ERR_CODE_SUCCESS]

	Write_Response(busiInfoResp, w, req, PUSH_INFO)
}

/*
	从设备获取版本记录的指令控制
	1、基础参数校验
	2、数据落库记录
*/

func BusiGetVerListCtl(w http.ResponseWriter, req *http.Request) {
	PrintHead(GET_COLOPHON)
	var busiPhon BusiGetColophon
	var busiPhonResp BusiGetColophonResp
	reqBuf, err := ioutil.ReadAll(req.Body)
	err = json.Unmarshal(reqBuf, &busiPhon)
	if err != nil {
		busiPhonResp.ErrorCode = ERR_CODE_JSONERR
		busiPhonResp.ErrorMsg = err.Error()
		Write_Response(busiPhonResp, w, req, GET_COLOPHON)
		return
	}
	defer req.Body.Close()

	currNode, err := getCurrNode(busiPhon.Sn)
	if err != nil {
		busiPhonResp.ErrorCode = ERR_CODE_TYPEERR
		busiPhonResp.ErrorMsg = err.Error()
		Write_Response(busiPhonResp, w, req, GET_COLOPHON)
		return
	}
	if currNode.Status == STATUS_DOING {
		busiPhonResp.ErrorCode = ERR_CODE_STATUSD
		busiPhonResp.ErrorMsg = ERROR_MAP[ERR_CODE_STATUSD]
		Write_Response(busiPhonResp, w, req, GET_COLOPHON)
		return
	}
	var e mfiles.MFiles
	r := mfiles.New(dbcomm.GetDB(), mfiles.DEBUG)
	e.BatchNo = fmt.Sprintf("%d", time.Now().UnixNano())
	e.ChipId = busiPhon.Chip_id
	e.Sn = busiPhon.Sn
	e.CmdType = CMDTYPE_VER
	e.StartTime = time.Now().Format("2006-01-02 15:04:05")
	if err := r.InsertEntity(e, nil); err != nil {
		busiPhonResp.ErrorCode = ERR_CODE_DBERROR
		busiPhonResp.ErrorMsg = err.Error()
		Write_Response(busiPhonResp, w, req, GET_COLOPHON)
		return
	}

	var getColoPhon GetColophon
	getColoPhon.Method = GET_COLOPHON
	getColoPhon.Chip_id = busiPhon.Chip_id
	getColoPhon.Sn = busiPhon.Sn
	getBuf, _ := json.Marshal(getColoPhon)

	Send_Resp(currNode.CurrConn, string(getBuf))

	currNode.BatchNo = e.BatchNo
	currNode.Status = STATUS_DOING
	GSn2ConnMap.Store(busiPhon.Sn, currNode)

	busiPhonResp.No = e.BatchNo
	busiPhonResp.ErrorCode = ERR_CODE_SUCCESS
	busiPhonResp.ErrorMsg = ERROR_MAP[ERR_CODE_SUCCESS]
	Write_Response(busiPhonResp, w, req, GET_COLOPHON)

}

/*
	从设备获取已经安装芯片的指令控制
*/

func BusiGetDataDriveCtl(w http.ResponseWriter, req *http.Request) {
	PrintHead(GET_INSTLL_DATADRIVE)

	var busiDrive BusiGetDataDrive
	var busiDriveResp BusiGetDataDriveResp
	reqBuf, err := ioutil.ReadAll(req.Body)
	err = json.Unmarshal(reqBuf, &busiDrive)
	if err != nil {
		busiDriveResp.ErrorCode = ERR_CODE_JSONERR
		busiDriveResp.ErrorMsg = err.Error()
		Write_Response(busiDriveResp, w, req, GET_INSTLL_DATADRIVE)
		return
	}

	defer req.Body.Close()

	currNode, err := getCurrNode(busiDrive.Sn)
	if err != nil {
		busiDriveResp.ErrorCode = ERR_CODE_TYPEERR
		busiDriveResp.ErrorMsg = err.Error()
		Write_Response(busiDriveResp, w, req, GET_INSTLL_DATADRIVE)
		return
	}
	if currNode.Status == STATUS_DOING {
		busiDriveResp.ErrorCode = ERR_CODE_STATUSD
		busiDriveResp.ErrorMsg = ERROR_MAP[ERR_CODE_STATUSD]
		Write_Response(busiDriveResp, w, req, GET_INSTLL_DATADRIVE)
		return
	}
	var e mfiles.MFiles
	r := mfiles.New(dbcomm.GetDB(), mfiles.DEBUG)
	e.BatchNo = fmt.Sprintf("%d", time.Now().UnixNano())
	e.ChipId = busiDrive.Chip_id
	e.Sn = busiDrive.Sn
	e.CmdType = CMDTYPE_DRIVE
	e.CreateBy = busiDrive.UserId
	e.StartTime = time.Now().Format("2006-01-02 15:04:05")
	if err := r.InsertEntity(e, nil); err != nil {
		busiDriveResp.ErrorCode = ERR_CODE_DBERROR
		busiDriveResp.ErrorMsg = err.Error()
		Write_Response(busiDriveResp, w, req, GET_FILE)
		return
	}

	var dataDrive GetInstallDataDrive
	dataDrive.Method = GET_INSTLL_DATADRIVE
	dataDrive.Sn = busiDrive.Sn
	dataDrive.Chip_id = busiDrive.Chip_id
	getBuf, _ := json.Marshal(dataDrive)

	Send_Resp(currNode.CurrConn, string(getBuf))
	//记录状态
	currNode.BatchNo = e.BatchNo
	currNode.Status = STATUS_DOING
	GSn2ConnMap.Store(busiDrive.Sn, currNode)

	busiDriveResp.No = currNode.BatchNo
	busiDriveResp.ErrorCode = ERR_CODE_SUCCESS
	busiDriveResp.ErrorMsg = ERROR_MAP[ERR_CODE_SUCCESS]
	Write_Response(busiDriveResp, w, req, GET_INSTLL_DATADRIVE)
}

/*
	查询设备的指令状态
*/

func BusiQueryStatusCtl(w http.ResponseWriter, req *http.Request) {
	PrintHead(QUERY_STATUS)
	var qry BusiQueryStatus
	var qryResp BusiQueryStatusResp

	reqBuf, err := ioutil.ReadAll(req.Body)
	if err = json.Unmarshal(reqBuf, &qry); err != nil {
		qryResp.ErrorCode = ERR_CODE_JSONERR
		qryResp.ErrorMsg = err.Error()

		Write_Response(qryResp, w, req, QUERY_STATUS)
		return
	}
	defer req.Body.Close()

	var search mfiles.Search
	r := mfiles.New(dbcomm.GetDB(), mfiles.DEBUG)
	search.BatchNo = qry.No
	if e, err := r.Get(search); err != nil {
		qryResp.ErrorCode = ERR_CODE_NOTEXIST
		qryResp.ErrorMsg = qry.No + "记录不存在"
		qryResp.No = qry.No

		Write_Response(qryResp, w, req, QUERY_STATUS)
		return
	} else {
		qryResp.ErrorCode = ERR_CODE_SUCCESS
		qryResp.No = qry.No
		qryResp.Status = e.Status
		qryResp.ErrorMsg = ERROR_MAP[ERR_CODE_SUCCESS]
		Write_Response(qryResp, w, req, QUERY_STATUS)
	}

}

/*
	HTTP 应答公共方法
*/
func Write_Response(response interface{}, w http.ResponseWriter, r *http.Request, tag string) {
	json, err := json.Marshal(response)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	log.Println(string(json))
	fmt.Fprintf(w, string(json))
	PrintTail(tag)
}

func getCurrNode(sn string) (StoreInfo, error) {
	var nilNode StoreInfo
	currObj, ok := GSn2ConnMap.Load(sn)
	if !ok {
		log.Println(sn + "缓存信息没有获取到")
		return nilNode, fmt.Errorf(sn + "的设备不存在或设备没有上线")
	}
	currNode, ret := currObj.(StoreInfo)
	if !ret {
		log.Println("类型断言错误")
		return nilNode, fmt.Errorf(sn + "类型断言错误")
	}
	return currNode, nil
}
