package main

import (
	"bytes"
	"encoding/binary"

	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
)

const ONLINE = "online"
const GET_COLOPHON = "get_colophon"
const GET_INSTLL_DATADRIVE = "get_install_datadrive"
const POST_INSTLL_DATADRIVE = "post_install_datadrive"
const GET_FILE_RESP = "get_file"
const START_GET_FILE = "get_file"
const POST_FILE_INFO = "post_file_info"
const PUSH_FILE_INFO = "push_file_info"
const PUSH_FILE = "push_file"
const POST_FILE = "post_file"
const PUSH_INFO = "push_info"
const CHECK_UDATE = "check_update"
const HEARTBEAT = "heartbeat"
const TYPE_CHIP = "chip"
const TYPE_UPGRADE = "upgrade"
const TYPE_CONFIG = "config"
const TYPE_RESULT = "result"
const TYPE_RAW = "raw"
const TYPE_LOG = "log"

const HEAD_LEN = 6

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
	File_crc int    `json:"file_crc"`
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
	Checksum uint32 `json:"checksum"`
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
	File              []File `json:"file"`
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
	Method   string     `json:"method"`
	Sn       string     `json:"sn"`
	Chip_id  string     `json:"chip_id"`
	Fragment []Fragment `json:"fragment"`
}

type PushFileResp struct {
	Method  string `json:"method"`
	Sn      string `json:"sn"`
	Chip_id string `json:"chip_id"`
	Index   string `json:"index"`
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

type BusiGetFile struct {
	No      string `json:"no"`
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
	ErrorCode string `json:"error_code"`
	ErrorMsg  string `json:"error msg"`
}

type BusiGetColophon struct {
	Sn      string `json:"sn"`
	Chip_id string `json:"chip_id"`
}

type BusiGetColophonResp struct {
	No        string `json:"no"`
	ErrorCode string `json:"error_code"`
	ErrorMsg  string `json:"error msg"`
}

type BusiGetDataDrive struct {
	Sn      string `json:"sn"`
	Chip_id string `json:"chip_id"`
}

type BusiGetDataDriveResp struct {
	No        string `json:"no"`
	ErrorCode string `json:"error_code"`
	ErrorMsg  string `json:"error msg"`
}

/*
	从设备获取文件的指令控制
*/
func GetFileControl(w http.ResponseWriter, req *http.Request) {
	fmt.Println("GetFileControl")
	var busiFile BusiGetFile
	reqBuf, err := ioutil.ReadAll(req.Body)
	err = json.Unmarshal(reqBuf, &busiFile)
	if err != nil {
		fmt.Println("Unmarshal error")
		return
	}
	defer req.Body.Close()
	var getFile GetFile
	getFile.Method = START_GET_FILE
	getFile.Chip_id = busiFile.Chip_id
	getFile.Sn = busiFile.Sn
	getFile.Type = busiFile.Type
	getFile.Range = busiFile.Range
	getFile.Count = busiFile.Count
	getFile.From = busiFile.From
	getFile.To = busiFile.To
	c, ok := GConnMap.Load(busiFile.Sn)
	if ok {
		fmt.Println("load ok....")
	}
	getBuf, _ := json.Marshal(getFile)
	conn, ret := c.(*net.TCPConn)
	if ret {
		Send_Resp(conn, string(getBuf))
	} else {
	}
	w.Write([]byte("ok"))
}

/*
 下发文件的到设备的指令控制
*/

func PushFileControl(w http.ResponseWriter, req *http.Request) {
	fmt.Println("hello")
	var getFile GetFile
	getFile.Method = START_GET_FILE
	getFile.Chip_id = "BJ4233245"
	getFile.Sn = "011401K0500031"
	getFile.Type = "result"
	getFile.Range = "advanced"
	getFile.Count = 0
	getFile.From = "2017-02-02"
	getFile.To = "2019-07-02"
	c, ok := GConnMap.Load("0001")
	if ok {
		fmt.Println("load ok....")
	}
	getBuf, _ := json.Marshal(getFile)
	conn, ret := c.(*net.TCPConn)
	fmt.Println(conn)
	if ret {
		Send_Resp(conn, string(getBuf))
	} else {

	}
}

/*
	从设备获取版本记录的指令控制
*/

func GetVerListControl(w http.ResponseWriter, req *http.Request) {
	fmt.Println("hello")
	var getFile GetFile
	getFile.Method = START_GET_FILE
	getFile.Chip_id = "BJ4233245"
	getFile.Sn = "011401K0500031"
	getFile.Type = "result"
	getFile.Range = "advanced"
	getFile.Count = 0
	getFile.From = "2017-02-02"
	getFile.To = "2019-07-02"
	c, ok := GConnMap.Load("0001")
	if ok {
		fmt.Println("load ok....")
	}
	getBuf, _ := json.Marshal(getFile)
	conn, ret := c.(*net.TCPConn)
	fmt.Println(conn)
	if ret {
		Send_Resp(conn, string(getBuf))
	} else {

	}
}

/*
	从设备获取已经安装芯片的指令控制
*/

func GetDataDriveControl(w http.ResponseWriter, req *http.Request) {
	fmt.Println("hello")
	var getFile GetFile
	getFile.Method = START_GET_FILE
	getFile.Chip_id = "BJ4233245"
	getFile.Sn = "011401K0500031"
	getFile.Type = "result"
	getFile.Range = "advanced"
	getFile.Count = 0
	getFile.From = "2017-02-02"
	getFile.To = "2019-07-02"
	c, ok := GConnMap.Load("0001")
	if ok {
		fmt.Println("load ok....")
	}
	getBuf, _ := json.Marshal(getFile)
	conn, ret := c.(*net.TCPConn)
	fmt.Println(conn)
	if ret {
		Send_Resp(conn, string(getBuf))
	} else {

	}
}
