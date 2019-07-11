package onlinehis

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"
)

const (
	SQL_NEWDB   = "NewDB  ===>"
	SQL_INSERT  = "Insert ===>"
	SQL_UPDATE  = "Update ===>"
	SQL_SELECT  = "Select ===>"
	SQL_DELETE  = "Delete ===>"
	SQL_ELAPSED = "Elapsed===>"
	SQL_ERROR   = "Error  ===>"
	SQL_TITLE   = "===================================="
	DEBUG       = 1
	INFO        = 2
)

type Search struct {
	Id           int64  `json:"id"`
	Sn           string `json:"sn"`
	ChipId       string `json:"chip_id"`
	DeviceName   string `json:"device_name"`
	DeviceSeries string `json:"device_series"`
	DeviceTime   string `json:"device_time"`
	DeviceVer    string `json:"device_ver"`
	HwVer        string `json:"hw_ver"`
	Protocol     string `json:"protocol"`
	SwVer        string `json:"sw_ver"`
	Gate         string `json:"gate"`
	RemoteIp     string `json:"remote_ip"`
	CreateTime   string `json:"create_time"`
	ActionType   string `json:"action_type"`
	Version      int64  `json:"version"`
	PageNo       int    `json:"page_no"`
	PageSize     int    `json:"page_size"`
	ExtraWhere   string `json:"extra_where"`
	SortFld      string `json:"sort_fld"`
}

type OnlineHisList struct {
	DB         *sql.DB
	Level      int
	Total      int         `json:"total"`
	OnlineHiss []OnlineHis `json:"OnlineHis"`
}

type OnlineHis struct {
	Id           int64  `json:"id"`
	Sn           string `json:"sn"`
	ChipId       string `json:"chip_id"`
	DeviceName   string `json:"device_name"`
	DeviceSeries string `json:"device_series"`
	DeviceTime   string `json:"device_time"`
	DeviceVer    string `json:"device_ver"`
	HwVer        string `json:"hw_ver"`
	Protocol     string `json:"protocol"`
	SwVer        string `json:"sw_ver"`
	Gate         string `json:"gate"`
	RemoteIp     string `json:"remote_ip"`
	CreateTime   string `json:"create_time"`
	ActionType   string `json:"action_type"`
	Version      int64  `json:"version"`
}

type Form struct {
	Form OnlineHis `json:"OnlineHis"`
}

/*
	说明：创建实例对象
	入参：db:数据库sql.DB, 数据库已经连接, level:日志级别
	出参：实例对象
*/

func New(db *sql.DB, level int) *OnlineHisList {
	if db == nil {
		log.Println(SQL_SELECT, "Database is nil")
		return nil
	}
	return &OnlineHisList{DB: db, Total: 0, OnlineHiss: make([]OnlineHis, 0), Level: level}
}

/*
	说明：创建实例对象
	入参：url:连接数据的url, 数据库还没有CONNECTED, level:日志级别
	出参：实例对象
*/

func NewUrl(url string, level int) *OnlineHisList {
	var err error
	db, err := sql.Open("mysql", url)
	if err != nil {
		log.Println(SQL_SELECT, "Open database error:", err)
		return nil
	}
	if err = db.Ping(); err != nil {
		log.Println(SQL_SELECT, "Ping database error:", err)
		return nil
	}
	return &OnlineHisList{DB: db, Total: 0, OnlineHiss: make([]OnlineHis, 0), Level: level}
}

/*
	说明：得到符合条件的总条数
	入参：s: 查询条件
	出参：参数1：返回符合条件的总条件, 参数2：如果错误返回错误对象
*/

func (r *OnlineHisList) GetTotal(s Search) (int, error) {
	var where string
	l := time.Now()

	if s.Id != 0 {
		where += " and id=" + fmt.Sprintf("%d", s.Id)
	}

	if s.Sn != "" {
		where += " and sn='" + s.Sn + "'"
	}

	if s.ChipId != "" {
		where += " and chip_id='" + s.ChipId + "'"
	}

	if s.DeviceName != "" {
		where += " and device_name='" + s.DeviceName + "'"
	}

	if s.DeviceSeries != "" {
		where += " and device_series='" + s.DeviceSeries + "'"
	}

	if s.DeviceTime != "" {
		where += " and device_time='" + s.DeviceTime + "'"
	}

	if s.DeviceVer != "" {
		where += " and device_ver='" + s.DeviceVer + "'"
	}

	if s.HwVer != "" {
		where += " and hw_ver='" + s.HwVer + "'"
	}

	if s.Protocol != "" {
		where += " and protocol='" + s.Protocol + "'"
	}

	if s.SwVer != "" {
		where += " and sw_ver='" + s.SwVer + "'"
	}

	if s.Gate != "" {
		where += " and gate='" + s.Gate + "'"
	}

	if s.RemoteIp != "" {
		where += " and remote_ip='" + s.RemoteIp + "'"
	}

	if s.CreateTime != "" {
		where += " and create_time='" + s.CreateTime + "'"
	}

	if s.Version != 0 {
		where += " and version=" + fmt.Sprintf("%d", s.Version)
	}

	if s.ExtraWhere != "" {
		where += s.ExtraWhere
	}

	qrySql := fmt.Sprintf("Select count(1) as total from lk_online_his   where 1=1 %s", where)
	if r.Level == DEBUG {
		log.Println(SQL_SELECT, qrySql)
	}
	rows, err := r.DB.Query(qrySql)
	if err != nil {
		log.Println(SQL_ERROR, err.Error())
		return 0, err
	}
	defer rows.Close()
	var total int
	for rows.Next() {
		rows.Scan(&total)
	}
	if r.Level == DEBUG {
		log.Println(SQL_ELAPSED, time.Since(l))
	}
	return total, nil
}

/*
	说明：根据主键查询符合条件的条数
	入参：s: 查询条件
	出参：参数1：返回符合条件的对象, 参数2：如果错误返回错误对象
*/

func (r OnlineHisList) Get(s Search) (*OnlineHis, error) {
	var where string
	l := time.Now()

	if s.Id != 0 {
		where += " and id=" + fmt.Sprintf("%d", s.Id)
	}

	if s.Sn != "" {
		where += " and sn='" + s.Sn + "'"
	}

	if s.ChipId != "" {
		where += " and chip_id='" + s.ChipId + "'"
	}

	if s.DeviceName != "" {
		where += " and device_name='" + s.DeviceName + "'"
	}

	if s.DeviceSeries != "" {
		where += " and device_series='" + s.DeviceSeries + "'"
	}

	if s.DeviceTime != "" {
		where += " and device_time='" + s.DeviceTime + "'"
	}

	if s.DeviceVer != "" {
		where += " and device_ver='" + s.DeviceVer + "'"
	}

	if s.HwVer != "" {
		where += " and hw_ver='" + s.HwVer + "'"
	}

	if s.Protocol != "" {
		where += " and protocol='" + s.Protocol + "'"
	}

	if s.SwVer != "" {
		where += " and sw_ver='" + s.SwVer + "'"
	}

	if s.Gate != "" {
		where += " and gate='" + s.Gate + "'"
	}

	if s.RemoteIp != "" {
		where += " and remote_ip='" + s.RemoteIp + "'"
	}

	if s.CreateTime != "" {
		where += " and create_time='" + s.CreateTime + "'"
	}

	if s.Version != 0 {
		where += " and version=" + fmt.Sprintf("%d", s.Version)
	}

	if s.ExtraWhere != "" {
		where += s.ExtraWhere
	}

	qrySql := fmt.Sprintf("Select id,sn,chip_id,device_name,device_series,device_time,device_ver,hw_ver,protocol,sw_ver,gate,remote_ip,create_time,version from lk_online_his where 1=1 %s ", where)
	if r.Level == DEBUG {
		log.Println(SQL_SELECT, qrySql)
	}
	rows, err := r.DB.Query(qrySql)
	if err != nil {
		log.Println(SQL_ERROR, err.Error())
		return nil, err
	}
	defer rows.Close()

	var p OnlineHis
	if !rows.Next() {
		return nil, fmt.Errorf("Not Finded Record")
	} else {
		err := rows.Scan(&p.Id, &p.Sn, &p.ChipId, &p.DeviceName, &p.DeviceSeries, &p.DeviceTime, &p.DeviceVer, &p.HwVer, &p.Protocol, &p.SwVer, &p.Gate, &p.RemoteIp, &p.CreateTime, &p.Version)
		if err != nil {
			log.Println(SQL_ERROR, err.Error())
			return nil, err
		}
	}
	log.Println(SQL_ELAPSED, r)
	if r.Level == DEBUG {
		log.Println(SQL_ELAPSED, time.Since(l))
	}
	return &p, nil
}

/*
	说明：根据条件查询复核条件对象列表，支持分页查询
	入参：s: 查询条件
	出参：参数1：返回符合条件的对象列表, 参数2：如果错误返回错误对象
*/

func (r *OnlineHisList) GetList(s Search) ([]OnlineHis, error) {
	var where string
	l := time.Now()

	if s.Id != 0 {
		where += " and id=" + fmt.Sprintf("%d", s.Id)
	}

	if s.Sn != "" {
		where += " and sn='" + s.Sn + "'"
	}

	if s.ChipId != "" {
		where += " and chip_id='" + s.ChipId + "'"
	}

	if s.DeviceName != "" {
		where += " and device_name='" + s.DeviceName + "'"
	}

	if s.DeviceSeries != "" {
		where += " and device_series='" + s.DeviceSeries + "'"
	}

	if s.DeviceTime != "" {
		where += " and device_time='" + s.DeviceTime + "'"
	}

	if s.DeviceVer != "" {
		where += " and device_ver='" + s.DeviceVer + "'"
	}

	if s.HwVer != "" {
		where += " and hw_ver='" + s.HwVer + "'"
	}

	if s.Protocol != "" {
		where += " and protocol='" + s.Protocol + "'"
	}

	if s.SwVer != "" {
		where += " and sw_ver='" + s.SwVer + "'"
	}

	if s.Gate != "" {
		where += " and gate='" + s.Gate + "'"
	}

	if s.RemoteIp != "" {
		where += " and remote_ip='" + s.RemoteIp + "'"
	}

	if s.CreateTime != "" {
		where += " and create_time='" + s.CreateTime + "'"
	}

	if s.Version != 0 {
		where += " and version=" + fmt.Sprintf("%d", s.Version)
	}

	if s.ExtraWhere != "" {
		where += s.ExtraWhere
	}

	var qrySql string
	if s.PageSize == 0 && s.PageNo == 0 {
		qrySql = fmt.Sprintf("Select id,sn,chip_id,device_name,device_series,device_time,device_ver,hw_ver,protocol,sw_ver,gate,remote_ip,create_time,version from lk_online_his where 1=1 %s", where)
	} else {
		qrySql = fmt.Sprintf("Select id,sn,chip_id,device_name,device_series,device_time,device_ver,hw_ver,protocol,sw_ver,gate,remote_ip,create_time,version from lk_online_his where 1=1 %s Limit %d offset %d", where, s.PageSize, (s.PageNo-1)*s.PageSize)
	}
	if r.Level == DEBUG {
		log.Println(SQL_SELECT, qrySql)
	}
	rows, err := r.DB.Query(qrySql)
	if err != nil {
		log.Println(SQL_ERROR, err.Error())
		return nil, err
	}
	defer rows.Close()

	var p OnlineHis
	for rows.Next() {
		rows.Scan(&p.Id, &p.Sn, &p.ChipId, &p.DeviceName, &p.DeviceSeries, &p.DeviceTime, &p.DeviceVer, &p.HwVer, &p.Protocol, &p.SwVer, &p.Gate, &p.RemoteIp, &p.CreateTime, &p.Version)
		r.OnlineHiss = append(r.OnlineHiss, p)
	}
	log.Println(SQL_ELAPSED, r)
	if r.Level == DEBUG {
		log.Println(SQL_ELAPSED, time.Since(l))
	}
	return r.OnlineHiss, nil
}

/*
	说明：根据主键查询符合条件的记录，并保持成MAP
	入参：s: 查询条件
	出参：参数1：返回符合条件的对象, 参数2：如果错误返回错误对象
*/
func (r *OnlineHisList) GetExt(s Search) (map[string]string, error) {
	var where string
	l := time.Now()

	if s.Id != 0 {
		where += " and id=" + fmt.Sprintf("%d", s.Id)
	}

	if s.Sn != "" {
		where += " and sn='" + s.Sn + "'"
	}

	if s.ChipId != "" {
		where += " and chip_id='" + s.ChipId + "'"
	}

	if s.DeviceName != "" {
		where += " and device_name='" + s.DeviceName + "'"
	}

	if s.DeviceSeries != "" {
		where += " and device_series='" + s.DeviceSeries + "'"
	}

	if s.DeviceTime != "" {
		where += " and device_time='" + s.DeviceTime + "'"
	}

	if s.DeviceVer != "" {
		where += " and device_ver='" + s.DeviceVer + "'"
	}

	if s.HwVer != "" {
		where += " and hw_ver='" + s.HwVer + "'"
	}

	if s.Protocol != "" {
		where += " and protocol='" + s.Protocol + "'"
	}

	if s.SwVer != "" {
		where += " and sw_ver='" + s.SwVer + "'"
	}

	if s.Gate != "" {
		where += " and gate='" + s.Gate + "'"
	}

	if s.RemoteIp != "" {
		where += " and remote_ip='" + s.RemoteIp + "'"
	}

	if s.CreateTime != "" {
		where += " and create_time='" + s.CreateTime + "'"
	}

	if s.Version != 0 {
		where += " and version=" + fmt.Sprintf("%d", s.Version)
	}

	qrySql := fmt.Sprintf("Select id,sn,chip_id,device_name,device_series,device_time,device_ver,hw_ver,protocol,sw_ver,gate,remote_ip,create_time,version from lk_online_his where 1=1 %s ", where)
	if r.Level == DEBUG {
		log.Println(SQL_SELECT, qrySql)
	}
	rows, err := r.DB.Query(qrySql)
	if err != nil {
		log.Println(SQL_ERROR, err.Error())
		return nil, err
	}
	defer rows.Close()

	Columns, _ := rows.Columns()

	values := make([]sql.RawBytes, len(Columns))
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	if !rows.Next() {
		return nil, fmt.Errorf("Not Finded Record")
	} else {
		err = rows.Scan(scanArgs...)
	}

	fldValMap := make(map[string]string)
	for k, v := range Columns {
		fldValMap[v] = string(values[k])
	}

	log.Println(SQL_ELAPSED, "==========>>>>>>>>>>>", fldValMap)
	if r.Level == DEBUG {
		log.Println(SQL_ELAPSED, time.Since(l))
	}
	return fldValMap, nil

}

/*
	说明：插入对象到数据表中，这个方法要求对象的各个属性必须赋值
	入参：p:插入的对象
	出参：参数1：如果出错，返回错误对象；成功返回nil
*/

func (r OnlineHisList) Insert(p OnlineHis) error {
	l := time.Now()
	exeSql := fmt.Sprintf("Insert into  lk_online_his(sn,chip_id,device_name,device_series,device_time,device_ver,hw_ver,protocol,sw_ver,gate,remote_ip,create_time,version)  values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
	if r.Level == DEBUG {
		log.Println(SQL_INSERT, exeSql)
	}
	_, err := r.DB.Exec(exeSql, p.Sn, p.ChipId, p.DeviceName, p.DeviceSeries, p.DeviceTime, p.DeviceVer, p.HwVer, p.Protocol, p.SwVer, p.Gate, p.RemoteIp, p.CreateTime, p.Version)
	if err != nil {
		log.Println(SQL_ERROR, err.Error())
		return err
	}
	if r.Level == DEBUG {
		log.Println(SQL_ELAPSED, time.Since(l))
	}
	return nil
}

/*
	说明：插入对象到数据表中，这个方法会判读对象的各个属性，如果属性不为空，才加入插入列中；
	入参：p:插入的对象
	出参：参数1：如果出错，返回错误对象；成功返回nil
*/

func (r OnlineHisList) InsertEntity(p OnlineHis, tr *sql.Tx) error {
	l := time.Now()
	var colNames, colTags string
	valSlice := make([]interface{}, 0)

	if p.Sn != "" {
		colNames += "sn,"
		colTags += "?,"
		valSlice = append(valSlice, p.Sn)
	}

	if p.ChipId != "" {
		colNames += "chip_id,"
		colTags += "?,"
		valSlice = append(valSlice, p.ChipId)
	}

	if p.DeviceName != "" {
		colNames += "device_name,"
		colTags += "?,"
		valSlice = append(valSlice, p.DeviceName)
	}

	if p.DeviceSeries != "" {
		colNames += "device_series,"
		colTags += "?,"
		valSlice = append(valSlice, p.DeviceSeries)
	}

	if p.DeviceTime != "" {
		colNames += "device_time,"
		colTags += "?,"
		valSlice = append(valSlice, p.DeviceTime)
	}

	if p.DeviceVer != "" {
		colNames += "device_ver,"
		colTags += "?,"
		valSlice = append(valSlice, p.DeviceVer)
	}

	if p.HwVer != "" {
		colNames += "hw_ver,"
		colTags += "?,"
		valSlice = append(valSlice, p.HwVer)
	}

	if p.Protocol != "" {
		colNames += "protocol,"
		colTags += "?,"
		valSlice = append(valSlice, p.Protocol)
	}

	if p.SwVer != "" {
		colNames += "sw_ver,"
		colTags += "?,"
		valSlice = append(valSlice, p.SwVer)
	}

	if p.Gate != "" {
		colNames += "gate,"
		colTags += "?,"
		valSlice = append(valSlice, p.Gate)
	}

	if p.RemoteIp != "" {
		colNames += "remote_ip,"
		colTags += "?,"
		valSlice = append(valSlice, p.RemoteIp)
	}

	if p.CreateTime != "" {
		colNames += "create_time,"
		colTags += "?,"
		valSlice = append(valSlice, p.CreateTime)
	}

	if p.ActionType != "" {
		colNames += "action_type,"
		colTags += "?,"
		valSlice = append(valSlice, p.ActionType)
	}

	if p.Version != 0 {
		colNames += "version,"
		colTags += "?,"
		valSlice = append(valSlice, p.Version)
	}

	colNames = strings.TrimRight(colNames, ",")
	colTags = strings.TrimRight(colTags, ",")
	exeSql := fmt.Sprintf("Insert into  lk_online_his(%s)  values(%s)", colNames, colTags)
	if r.Level == DEBUG {
		log.Println(SQL_INSERT, exeSql)
	}

	var stmt *sql.Stmt
	var err error
	if tr == nil {
		stmt, err = r.DB.Prepare(exeSql)
	} else {
		stmt, err = tr.Prepare(exeSql)
	}
	if err != nil {
		log.Println(SQL_ERROR, err.Error())
		return err
	}
	defer stmt.Close()

	ret, err := stmt.Exec(valSlice...)
	if err != nil {
		log.Println(SQL_INSERT, "Insert data error: %v\n", err)
		return err
	}
	if LastInsertId, err := ret.LastInsertId(); nil == err {
		log.Println(SQL_INSERT, "LastInsertId:", LastInsertId)
	}
	if RowsAffected, err := ret.RowsAffected(); nil == err {
		log.Println(SQL_INSERT, "RowsAffected:", RowsAffected)
	}

	if r.Level == DEBUG {
		log.Println(SQL_ELAPSED, time.Since(l))
	}
	return nil
}

/*
	说明：插入一个MAP到数据表中；
	入参：m:插入的Map
	出参：参数1：如果出错，返回错误对象；成功返回nil
*/

func (r OnlineHisList) InsertMap(m map[string]interface{}, tr *sql.Tx) error {
	l := time.Now()
	var colNames, colTags string
	valSlice := make([]interface{}, 0)
	for k, v := range m {
		colNames += k + ","
		colTags += "?,"
		valSlice = append(valSlice, v)
	}
	colNames = strings.TrimRight(colNames, ",")
	colTags = strings.TrimRight(colTags, ",")

	exeSql := fmt.Sprintf("Insert into  lk_online_his(%s)  values(%s)", colNames, colTags)
	if r.Level == DEBUG {
		log.Println(SQL_INSERT, exeSql)
	}

	var stmt *sql.Stmt
	var err error
	if tr == nil {
		stmt, err = r.DB.Prepare(exeSql)
	} else {
		stmt, err = tr.Prepare(exeSql)
	}

	if err != nil {
		log.Println(SQL_ERROR, err.Error())
		return err
	}
	defer stmt.Close()

	ret, err := stmt.Exec(valSlice...)
	if err != nil {
		log.Println(SQL_INSERT, "insert data error: %v\n", err)
		return err
	}
	if LastInsertId, err := ret.LastInsertId(); nil == err {
		log.Println(SQL_INSERT, "LastInsertId:", LastInsertId)
	}
	if RowsAffected, err := ret.RowsAffected(); nil == err {
		log.Println(SQL_INSERT, "RowsAffected:", RowsAffected)
	}

	if r.Level == DEBUG {
		log.Println(SQL_ELAPSED, time.Since(l))
	}
	return nil
}

/*
	说明：插入对象到数据表中，这个方法会判读对象的各个属性，如果属性不为空，才加入插入列中；
	入参：p:插入的对象
	出参：参数1：如果出错，返回错误对象；成功返回nil
*/

func (r OnlineHisList) UpdataEntity(keyNo string, p OnlineHis, tr *sql.Tx) error {
	l := time.Now()
	var colNames string
	valSlice := make([]interface{}, 0)

	if p.Id != 0 {
		colNames += "id=?,"
		valSlice = append(valSlice, p.Id)
	}

	if p.Sn != "" {
		colNames += "sn=?,"

		valSlice = append(valSlice, p.Sn)
	}

	if p.ChipId != "" {
		colNames += "chip_id=?,"

		valSlice = append(valSlice, p.ChipId)
	}

	if p.DeviceName != "" {
		colNames += "device_name=?,"

		valSlice = append(valSlice, p.DeviceName)
	}

	if p.DeviceSeries != "" {
		colNames += "device_series=?,"

		valSlice = append(valSlice, p.DeviceSeries)
	}

	if p.DeviceTime != "" {
		colNames += "device_time=?,"

		valSlice = append(valSlice, p.DeviceTime)
	}

	if p.DeviceVer != "" {
		colNames += "device_ver=?,"

		valSlice = append(valSlice, p.DeviceVer)
	}

	if p.HwVer != "" {
		colNames += "hw_ver=?,"

		valSlice = append(valSlice, p.HwVer)
	}

	if p.Protocol != "" {
		colNames += "protocol=?,"

		valSlice = append(valSlice, p.Protocol)
	}

	if p.SwVer != "" {
		colNames += "sw_ver=?,"

		valSlice = append(valSlice, p.SwVer)
	}

	if p.Gate != "" {
		colNames += "gate=?,"

		valSlice = append(valSlice, p.Gate)
	}

	if p.RemoteIp != "" {
		colNames += "remote_ip=?,"

		valSlice = append(valSlice, p.RemoteIp)
	}

	if p.CreateTime != "" {
		colNames += "create_time=?,"

		valSlice = append(valSlice, p.CreateTime)
	}

	if p.Version != 0 {
		colNames += "version=?,"
		valSlice = append(valSlice, p.Version)
	}

	colNames = strings.TrimRight(colNames, ",")
	valSlice = append(valSlice, keyNo)

	exeSql := fmt.Sprintf("update  lk_online_his  set %s  where id=? ", colNames)
	if r.Level == DEBUG {
		log.Println(SQL_INSERT, exeSql)
	}

	var stmt *sql.Stmt
	var err error
	if tr == nil {
		stmt, err = r.DB.Prepare(exeSql)
	} else {
		stmt, err = tr.Prepare(exeSql)
	}

	if err != nil {
		log.Println(SQL_ERROR, err.Error())
		return err
	}
	defer stmt.Close()

	ret, err := stmt.Exec(valSlice...)
	if err != nil {
		log.Println(SQL_INSERT, "Update data error: %v\n", err)
		return err
	}
	if LastInsertId, err := ret.LastInsertId(); nil == err {
		log.Println(SQL_INSERT, "LastInsertId:", LastInsertId)
	}
	if RowsAffected, err := ret.RowsAffected(); nil == err {
		log.Println(SQL_INSERT, "RowsAffected:", RowsAffected)
	}

	if r.Level == DEBUG {
		log.Println(SQL_ELAPSED, time.Since(l))
	}
	return nil
}

/*
	说明：根据更新主键及更新Map值更新数据表；
	入参：keyNo:更新数据的关键条件，m:更新数据列的Map
	出参：参数1：如果出错，返回错误对象；成功返回nil
*/

func (r OnlineHisList) UpdateMap(keyNo string, m map[string]interface{}, tr *sql.Tx) error {
	l := time.Now()

	var colNames string
	valSlice := make([]interface{}, 0)
	for k, v := range m {
		colNames += k + "=?,"
		valSlice = append(valSlice, v)
	}
	valSlice = append(valSlice, keyNo)
	colNames = strings.TrimRight(colNames, ",")
	updateSql := fmt.Sprintf("Update lk_online_his set %s where id=?", colNames)
	if r.Level == DEBUG {
		log.Println(SQL_UPDATE, updateSql)
	}
	var stmt *sql.Stmt
	var err error
	if tr == nil {
		stmt, err = r.DB.Prepare(updateSql)
	} else {
		stmt, err = tr.Prepare(updateSql)
	}

	if err != nil {
		log.Println(SQL_ERROR, err.Error())
		return err
	}
	ret, err := stmt.Exec(valSlice...)
	if err != nil {
		log.Println(SQL_UPDATE, "Update data error: %v\n", err)
		return err
	}
	defer stmt.Close()

	if LastInsertId, err := ret.LastInsertId(); nil == err {
		log.Println(SQL_UPDATE, "LastInsertId:", LastInsertId)
	}
	if RowsAffected, err := ret.RowsAffected(); nil == err {
		log.Println(SQL_UPDATE, "RowsAffected:", RowsAffected)
	}
	if r.Level == DEBUG {
		log.Println(SQL_ELAPSED, time.Since(l))
	}
	return nil
}

/*
	说明：根据主键删除一条数据；
	入参：keyNo:要删除的主键值
	出参：参数1：如果出错，返回错误对象；成功返回nil
*/

func (r OnlineHisList) Delete(keyNo string, tr *sql.Tx) error {
	l := time.Now()
	delSql := fmt.Sprintf("Delete from  lk_online_his  where id=?")
	if r.Level == DEBUG {
		log.Println(SQL_UPDATE, delSql)
	}

	var stmt *sql.Stmt
	var err error
	if tr == nil {
		stmt, err = r.DB.Prepare(delSql)
	} else {
		stmt, err = tr.Prepare(delSql)
	}

	if err != nil {
		log.Println(SQL_ERROR, err.Error())
		return err
	}
	ret, err := stmt.Exec(keyNo)
	if err != nil {
		log.Println(SQL_DELETE, "Delete error: %v\n", err)
		return err
	}
	defer stmt.Close()

	if LastInsertId, err := ret.LastInsertId(); nil == err {
		log.Println(SQL_DELETE, "LastInsertId:", LastInsertId)
	}
	if RowsAffected, err := ret.RowsAffected(); nil == err {
		log.Println(SQL_DELETE, "RowsAffected:", RowsAffected)
	}
	if r.Level == DEBUG {
		log.Println(SQL_ELAPSED, time.Since(l))
	}
	return nil
}
