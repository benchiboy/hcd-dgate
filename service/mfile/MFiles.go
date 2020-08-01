package mfiles

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
	Id         int64  `json:"id"`
	BatchNo    string `json:"batch_no"`
	Sn         string `json:"sn"`
	ChipId     string `json:"chip_id"`
	CmdType    string `json:"cmd_type"`
	Type       string `json:"type"`
	Purpose    string `json:"purpose"`
	Frange     string `json:"frange"`
	FromDate   string `json:"from_date"`
	ToDate     string `json:"to_date"`
	TodoCount  int    `json:"todo_count"`
	DoneCount  int    `json:"done_count"`
	InfoMsg    string `json:"info_msg"`
	Status     string `json:"status"`
	Percent    int    `json:"percent"`
	FailMsg    string `json:"fail_msg"`
	StartTime  string `json:"start_time"`
	EndTime    string `json:"end_time"`
	CreateBy   int64  `json:"create_by"`
	UpdateTime string `json:"update_time"`
	UpdateBy   int64  `json:"update_by"`
	Version    int64  `json:"version"`
	PageNo     int    `json:"page_no"`
	PageSize   int    `json:"page_size"`
	ExtraWhere string `json:"extra_where"`
	SortFld    string `json:"sort_fld"`
}

type MFilesList struct {
	DB      *sql.DB
	Level   int
	Total   int      `json:"total"`
	MFiless []MFiles `json:"MFiles"`
}

type MFiles struct {
	Id         int64  `json:"id"`
	BatchNo    string `json:"batch_no"`
	Sn         string `json:"sn"`
	ChipId     string `json:"chip_id"`
	CmdType    string `json:"cmd_type"`
	Type       string `json:"type"`
	Purpose    string `json:"purpose"`
	Frange     string `json:"frange"`
	FromDate   string `json:"from_date"`
	ToDate     string `json:"to_date"`
	TodoCount  int    `json:"todo_count"`
	DoneCount  int    `json:"done_count"`
	InfoMsg    string `json:"info_msg"`
	Status     string `json:"status"`
	Percent    int    `json:"percent"`
	FailMsg    string `json:"fail_msg"`
	StartTime  string `json:"start_time"`
	EndTime    string `json:"end_time"`
	CreateBy   int64  `json:"create_by"`
	UpdateTime string `json:"update_time"`
	UpdateBy   int64  `json:"update_by"`
	Version    int64  `json:"version"`
}

type Form struct {
	Form MFiles `json:"MFiles"`
}

/*
	说明：创建实例对象
	入参：db:数据库sql.DB, 数据库已经连接, level:日志级别
	出参：实例对象
*/

func New(db *sql.DB, level int) *MFilesList {
	if db == nil {
		log.Println(SQL_SELECT, "Database is nil")
		return nil
	}
	return &MFilesList{DB: db, Total: 0, MFiless: make([]MFiles, 0), Level: level}
}

/*
	说明：创建实例对象
	入参：url:连接数据的url, 数据库还没有CONNECTED, level:日志级别
	出参：实例对象
*/

func NewUrl(url string, level int) *MFilesList {
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
	return &MFilesList{DB: db, Total: 0, MFiless: make([]MFiles, 0), Level: level}
}

/*
	说明：得到符合条件的总条数
	入参：s: 查询条件
	出参：参数1：返回符合条件的总条件, 参数2：如果错误返回错误对象
*/

func (r *MFilesList) GetTotal(s Search) (int, error) {
	var where string
	l := time.Now()

	if s.Id != 0 {
		where += " and id=" + fmt.Sprintf("%d", s.Id)
	}

	if s.BatchNo != "" {
		where += " and batch_no='" + s.BatchNo + "'"
	}

	if s.Sn != "" {
		where += " and sn='" + s.Sn + "'"
	}

	if s.ChipId != "" {
		where += " and chip_id='" + s.ChipId + "'"
	}

	if s.CmdType != "" {
		where += " and cmd_type='" + s.CmdType + "'"
	}

	if s.Type != "" {
		where += " and type='" + s.Type + "'"
	}

	if s.Purpose != "" {
		where += " and purpose='" + s.Purpose + "'"
	}

	if s.Frange != "" {
		where += " and frange='" + s.Frange + "'"
	}

	if s.FromDate != "" {
		where += " and from_date='" + s.FromDate + "'"
	}

	if s.ToDate != "" {
		where += " and to_date='" + s.ToDate + "'"
	}

	if s.TodoCount != 0 {
		where += " and todo_count=" + fmt.Sprintf("%d", s.TodoCount)
	}

	if s.DoneCount != 0 {
		where += " and done_count=" + fmt.Sprintf("%d", s.DoneCount)
	}

	if s.InfoMsg != "" {
		where += " and info_msg='" + s.InfoMsg + "'"
	}

	if s.Status != "" {
		where += " and status='" + s.Status + "'"
	}

	if s.FailMsg != "" {
		where += " and fail_msg='" + s.FailMsg + "'"
	}

	if s.StartTime != "" {
		where += " and start_time='" + s.StartTime + "'"
	}

	if s.EndTime != "" {
		where += " and end_time='" + s.EndTime + "'"
	}

	if s.CreateBy != 0 {
		where += " and create_by=" + fmt.Sprintf("%d", s.CreateBy)
	}

	if s.UpdateTime != "" {
		where += " and update_time='" + s.UpdateTime + "'"
	}

	if s.UpdateBy != 0 {
		where += " and update_by=" + fmt.Sprintf("%d", s.UpdateBy)
	}

	if s.Version != 0 {
		where += " and version=" + fmt.Sprintf("%d", s.Version)
	}

	if s.ExtraWhere != "" {
		where += s.ExtraWhere
	}

	qrySql := fmt.Sprintf("Select count(1) as total from lk_device_mfiles   where 1=1 %s", where)
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

func (r MFilesList) Get(s Search) (*MFiles, error) {
	var where string
	l := time.Now()

	if s.Id != 0 {
		where += " and id=" + fmt.Sprintf("%d", s.Id)
	}

	if s.BatchNo != "" {
		where += " and batch_no='" + s.BatchNo + "'"
	}

	if s.Sn != "" {
		where += " and sn='" + s.Sn + "'"
	}

	if s.ChipId != "" {
		where += " and chip_id='" + s.ChipId + "'"
	}

	if s.CmdType != "" {
		where += " and cmd_type='" + s.CmdType + "'"
	}

	if s.Type != "" {
		where += " and type='" + s.Type + "'"
	}

	if s.Purpose != "" {
		where += " and purpose='" + s.Purpose + "'"
	}

	if s.Frange != "" {
		where += " and frange='" + s.Frange + "'"
	}

	if s.FromDate != "" {
		where += " and from_date='" + s.FromDate + "'"
	}

	if s.ToDate != "" {
		where += " and to_date='" + s.ToDate + "'"
	}

	if s.TodoCount != 0 {
		where += " and todo_count=" + fmt.Sprintf("%d", s.TodoCount)
	}

	if s.DoneCount != 0 {
		where += " and done_count=" + fmt.Sprintf("%d", s.DoneCount)
	}

	if s.InfoMsg != "" {
		where += " and info_msg='" + s.InfoMsg + "'"
	}

	if s.Status != "" {
		where += " and status='" + s.Status + "'"
	}

	if s.FailMsg != "" {
		where += " and fail_msg='" + s.FailMsg + "'"
	}

	if s.StartTime != "" {
		where += " and start_time='" + s.StartTime + "'"
	}

	if s.EndTime != "" {
		where += " and end_time='" + s.EndTime + "'"
	}

	if s.CreateBy != 0 {
		where += " and create_by=" + fmt.Sprintf("%d", s.CreateBy)
	}

	if s.UpdateTime != "" {
		where += " and update_time='" + s.UpdateTime + "'"
	}

	if s.UpdateBy != 0 {
		where += " and update_by=" + fmt.Sprintf("%d", s.UpdateBy)
	}

	if s.Version != 0 {
		where += " and version=" + fmt.Sprintf("%d", s.Version)
	}

	if s.ExtraWhere != "" {
		where += s.ExtraWhere
	}

	qrySql := fmt.Sprintf("Select id,batch_no,todo_count,done_count,status from lk_device_mfiles where 1=1 %s  order by id desc limit 1", where)
	if r.Level == DEBUG {
		log.Println(SQL_SELECT, qrySql)
	}
	rows, err := r.DB.Query(qrySql)
	if err != nil {
		log.Println(SQL_ERROR, err.Error())
		return nil, err
	}
	defer rows.Close()

	var p MFiles
	if !rows.Next() {
		return nil, fmt.Errorf("Not Finded Record")
	} else {
		err := rows.Scan(&p.Id, &p.BatchNo, &p.TodoCount, &p.DoneCount, &p.Status)
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

func (r *MFilesList) GetList(s Search) ([]MFiles, error) {
	var where string
	l := time.Now()

	if s.Id != 0 {
		where += " and id=" + fmt.Sprintf("%d", s.Id)
	}

	if s.BatchNo != "" {
		where += " and batch_no='" + s.BatchNo + "'"
	}

	if s.Sn != "" {
		where += " and sn='" + s.Sn + "'"
	}

	if s.ChipId != "" {
		where += " and chip_id='" + s.ChipId + "'"
	}

	if s.CmdType != "" {
		where += " and cmd_type='" + s.CmdType + "'"
	}

	if s.Type != "" {
		where += " and type='" + s.Type + "'"
	}

	if s.Purpose != "" {
		where += " and purpose='" + s.Purpose + "'"
	}

	if s.Frange != "" {
		where += " and frange='" + s.Frange + "'"
	}

	if s.FromDate != "" {
		where += " and from_date='" + s.FromDate + "'"
	}

	if s.ToDate != "" {
		where += " and to_date='" + s.ToDate + "'"
	}

	if s.TodoCount != 0 {
		where += " and todo_count=" + fmt.Sprintf("%d", s.TodoCount)
	}

	if s.DoneCount != 0 {
		where += " and done_count=" + fmt.Sprintf("%d", s.DoneCount)
	}

	if s.InfoMsg != "" {
		where += " and info_msg='" + s.InfoMsg + "'"
	}

	if s.Status != "" {
		where += " and status='" + s.Status + "'"
	}

	if s.FailMsg != "" {
		where += " and fail_msg='" + s.FailMsg + "'"
	}

	if s.StartTime != "" {
		where += " and start_time='" + s.StartTime + "'"
	}

	if s.EndTime != "" {
		where += " and end_time='" + s.EndTime + "'"
	}

	if s.CreateBy != 0 {
		where += " and create_by=" + fmt.Sprintf("%d", s.CreateBy)
	}

	if s.UpdateTime != "" {
		where += " and update_time='" + s.UpdateTime + "'"
	}

	if s.UpdateBy != 0 {
		where += " and update_by=" + fmt.Sprintf("%d", s.UpdateBy)
	}

	if s.Version != 0 {
		where += " and version=" + fmt.Sprintf("%d", s.Version)
	}

	if s.ExtraWhere != "" {
		where += s.ExtraWhere
	}

	var qrySql string
	if s.PageSize == 0 && s.PageNo == 0 {
		qrySql = fmt.Sprintf("Select id,batch_no,sn,chip_id,cmd_type,type,purpose,frange,from_date,to_date,todo_count,done_count,info_msg,status,fail_msg,start_time,end_time,create_by,update_time,update_by,version from lk_device_mfiles where 1=1 %s", where)
	} else {
		qrySql = fmt.Sprintf("Select id,batch_no,sn,chip_id,cmd_type,type,purpose,frange,from_date,to_date,todo_count,done_count,info_msg,status,fail_msg,start_time,end_time,create_by,update_time,update_by,version from lk_device_mfiles where 1=1 %s Limit %d offset %d", where, s.PageSize, (s.PageNo-1)*s.PageSize)
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

	var p MFiles
	for rows.Next() {
		rows.Scan(&p.Id, &p.BatchNo, &p.Sn, &p.ChipId, &p.CmdType, &p.Type, &p.Purpose, &p.Frange, &p.FromDate, &p.ToDate, &p.TodoCount, &p.DoneCount, &p.InfoMsg, &p.Status, &p.FailMsg, &p.StartTime, &p.EndTime, &p.CreateBy, &p.UpdateTime, &p.UpdateBy, &p.Version)
		r.MFiless = append(r.MFiless, p)
	}
	log.Println(SQL_ELAPSED, r)
	if r.Level == DEBUG {
		log.Println(SQL_ELAPSED, time.Since(l))
	}
	return r.MFiless, nil
}

/*
	说明：根据主键查询符合条件的记录，并保持成MAP
	入参：s: 查询条件
	出参：参数1：返回符合条件的对象, 参数2：如果错误返回错误对象
*/

func (r *MFilesList) GetExt(s Search) (map[string]string, error) {
	var where string
	l := time.Now()

	if s.Id != 0 {
		where += " and id=" + fmt.Sprintf("%d", s.Id)
	}

	if s.BatchNo != "" {
		where += " and batch_no='" + s.BatchNo + "'"
	}

	if s.Sn != "" {
		where += " and sn='" + s.Sn + "'"
	}

	if s.ChipId != "" {
		where += " and chip_id='" + s.ChipId + "'"
	}

	if s.CmdType != "" {
		where += " and cmd_type='" + s.CmdType + "'"
	}

	if s.Type != "" {
		where += " and type='" + s.Type + "'"
	}

	if s.Purpose != "" {
		where += " and purpose='" + s.Purpose + "'"
	}

	if s.Frange != "" {
		where += " and frange='" + s.Frange + "'"
	}

	if s.FromDate != "" {
		where += " and from_date='" + s.FromDate + "'"
	}

	if s.ToDate != "" {
		where += " and to_date='" + s.ToDate + "'"
	}

	if s.TodoCount != 0 {
		where += " and todo_count=" + fmt.Sprintf("%d", s.TodoCount)
	}

	if s.DoneCount != 0 {
		where += " and done_count=" + fmt.Sprintf("%d", s.DoneCount)
	}

	if s.InfoMsg != "" {
		where += " and info_msg='" + s.InfoMsg + "'"
	}

	if s.Status != "" {
		where += " and status='" + s.Status + "'"
	}

	if s.FailMsg != "" {
		where += " and fail_msg='" + s.FailMsg + "'"
	}

	if s.StartTime != "" {
		where += " and start_time='" + s.StartTime + "'"
	}

	if s.EndTime != "" {
		where += " and end_time='" + s.EndTime + "'"
	}

	if s.CreateBy != 0 {
		where += " and create_by=" + fmt.Sprintf("%d", s.CreateBy)
	}

	if s.UpdateTime != "" {
		where += " and update_time='" + s.UpdateTime + "'"
	}

	if s.UpdateBy != 0 {
		where += " and update_by=" + fmt.Sprintf("%d", s.UpdateBy)
	}

	if s.Version != 0 {
		where += " and version=" + fmt.Sprintf("%d", s.Version)
	}

	qrySql := fmt.Sprintf("Select id,batch_no,sn,chip_id,cmd_type,type,purpose,frange,from_date,to_date,todo_count,done_count,info_msg,status,fail_msg,start_time,end_time,create_by,update_time,update_by,version from lk_device_mfiles where 1=1 %s ", where)
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

func (r MFilesList) Insert(p MFiles) error {
	l := time.Now()
	exeSql := fmt.Sprintf("Insert into  lk_device_mfiles(batch_no,sn,chip_id,cmd_type,type,purpose,frange,from_date,to_date,todo_count,done_count,info_msg,status,fail_msg,start_time,end_time,create_by,update_by,version)  values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
	if r.Level == DEBUG {
		log.Println(SQL_INSERT, exeSql)
	}
	_, err := r.DB.Exec(exeSql, p.BatchNo, p.Sn, p.ChipId, p.CmdType, p.Type, p.Purpose, p.Frange, p.FromDate, p.ToDate, p.TodoCount, p.DoneCount, p.InfoMsg, p.Status, p.FailMsg, p.StartTime, p.EndTime, p.CreateBy, p.UpdateBy, p.Version)
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

func (r MFilesList) InsertEntity(p MFiles, tr *sql.Tx) error {
	l := time.Now()
	var colNames, colTags string
	valSlice := make([]interface{}, 0)

	if p.BatchNo != "" {
		colNames += "batch_no,"
		colTags += "?,"
		valSlice = append(valSlice, p.BatchNo)
	}

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

	if p.CmdType != "" {
		colNames += "cmd_type,"
		colTags += "?,"
		valSlice = append(valSlice, p.CmdType)
	}

	if p.Type != "" {
		colNames += "type,"
		colTags += "?,"
		valSlice = append(valSlice, p.Type)
	}

	if p.Purpose != "" {
		colNames += "purpose,"
		colTags += "?,"
		valSlice = append(valSlice, p.Purpose)
	}

	if p.Frange != "" {
		colNames += "frange,"
		colTags += "?,"
		valSlice = append(valSlice, p.Frange)
	}

	if p.FromDate != "" {
		colNames += "from_date,"
		colTags += "?,"
		valSlice = append(valSlice, p.FromDate)
	}

	if p.ToDate != "" {
		colNames += "to_date,"
		colTags += "?,"
		valSlice = append(valSlice, p.ToDate)
	}

	if p.TodoCount != 0 {
		colNames += "todo_count,"
		colTags += "?,"
		valSlice = append(valSlice, p.TodoCount)
	}

	if p.DoneCount != 0 {
		colNames += "done_count,"
		colTags += "?,"
		valSlice = append(valSlice, p.DoneCount)
	}

	if p.InfoMsg != "" {
		colNames += "info_msg,"
		colTags += "?,"
		valSlice = append(valSlice, p.InfoMsg)
	}

	if p.Status != "" {
		colNames += "status,"
		colTags += "?,"
		valSlice = append(valSlice, p.Status)
	}

	if p.FailMsg != "" {
		colNames += "fail_msg,"
		colTags += "?,"
		valSlice = append(valSlice, p.FailMsg)
	}

	if p.StartTime != "" {
		colNames += "start_time,"
		colTags += "?,"
		valSlice = append(valSlice, p.StartTime)
	}

	if p.EndTime != "" {
		colNames += "end_time,"
		colTags += "?,"
		valSlice = append(valSlice, p.EndTime)
	}

	if p.CreateBy != 0 {
		colNames += "create_by,"
		colTags += "?,"
		valSlice = append(valSlice, p.CreateBy)
	}

	if p.UpdateBy != 0 {
		colNames += "update_by,"
		colTags += "?,"
		valSlice = append(valSlice, p.UpdateBy)
	}

	if p.Version != 0 {
		colNames += "version,"
		colTags += "?,"
		valSlice = append(valSlice, p.Version)
	}

	colNames = strings.TrimRight(colNames, ",")
	colTags = strings.TrimRight(colTags, ",")
	exeSql := fmt.Sprintf("Insert into  lk_device_mfiles(%s)  values(%s)", colNames, colTags)
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

func (r MFilesList) InsertMap(m map[string]interface{}, tr *sql.Tx) error {
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

	exeSql := fmt.Sprintf("Insert into  lk_device_mfiles(%s)  values(%s)", colNames, colTags)
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

func (r MFilesList) UpdataEntity(keyNo string, p MFiles, tr *sql.Tx) error {
	l := time.Now()
	var colNames string
	valSlice := make([]interface{}, 0)

	if p.Id != 0 {
		colNames += "id=?,"
		valSlice = append(valSlice, p.Id)
	}

	if p.BatchNo != "" {
		colNames += "batch_no=?,"

		valSlice = append(valSlice, p.BatchNo)
	}

	if p.Sn != "" {
		colNames += "sn=?,"

		valSlice = append(valSlice, p.Sn)
	}

	if p.ChipId != "" {
		colNames += "chip_id=?,"

		valSlice = append(valSlice, p.ChipId)
	}

	if p.CmdType != "" {
		colNames += "cmd_type=?,"

		valSlice = append(valSlice, p.CmdType)
	}

	if p.Type != "" {
		colNames += "type=?,"

		valSlice = append(valSlice, p.Type)
	}

	if p.Purpose != "" {
		colNames += "purpose=?,"

		valSlice = append(valSlice, p.Purpose)
	}

	if p.Frange != "" {
		colNames += "frange=?,"

		valSlice = append(valSlice, p.Frange)
	}

	if p.FromDate != "" {
		colNames += "from_date=?,"

		valSlice = append(valSlice, p.FromDate)
	}

	if p.ToDate != "" {
		colNames += "to_date=?,"

		valSlice = append(valSlice, p.ToDate)
	}

	if p.TodoCount != 0 {
		colNames += "todo_count=?,"
		valSlice = append(valSlice, p.TodoCount)
	}

	if p.DoneCount != 0 {
		colNames += "done_count=?,"
		valSlice = append(valSlice, p.DoneCount)
	}

	if p.InfoMsg != "" {
		colNames += "info_msg=?,"

		valSlice = append(valSlice, p.InfoMsg)
	}

	if p.Status != "" {
		colNames += "status=?,"

		valSlice = append(valSlice, p.Status)
	}

	if p.Percent != 0 {
		colNames += "percent=?,"
		valSlice = append(valSlice, p.Percent)
	}

	if p.FailMsg != "" {
		colNames += "fail_msg=?,"

		valSlice = append(valSlice, p.FailMsg)
	}

	if p.StartTime != "" {
		colNames += "start_time=?,"

		valSlice = append(valSlice, p.StartTime)
	}

	if p.EndTime != "" {
		colNames += "end_time=?,"

		valSlice = append(valSlice, p.EndTime)
	}

	if p.CreateBy != 0 {
		colNames += "create_by=?,"
		valSlice = append(valSlice, p.CreateBy)
	}

	if p.UpdateTime != "" {
		colNames += "update_time=?,"

		valSlice = append(valSlice, p.UpdateTime)
	}

	if p.UpdateBy != 0 {
		colNames += "update_by=?,"
		valSlice = append(valSlice, p.UpdateBy)
	}

	if p.Version != 0 {
		colNames += "version=?,"
		valSlice = append(valSlice, p.Version)
	}

	colNames = strings.TrimRight(colNames, ",")
	valSlice = append(valSlice, keyNo)

	exeSql := fmt.Sprintf("update  lk_device_mfiles  set %s  where batch_no=? ", colNames)
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

func (r MFilesList) UpdateMap(keyNo string, m map[string]interface{}, tr *sql.Tx) error {
	l := time.Now()

	var colNames string
	valSlice := make([]interface{}, 0)
	for k, v := range m {
		colNames += k + "=?,"
		valSlice = append(valSlice, v)
	}
	valSlice = append(valSlice, keyNo)
	colNames = strings.TrimRight(colNames, ",")
	updateSql := fmt.Sprintf("Update lk_device_mfiles set %s where id=?", colNames)
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
		if r.Level == DEBUG {
			log.Println(SQL_UPDATE, "LastInsertId:", LastInsertId)
		}
	}
	if RowsAffected, err := ret.RowsAffected(); nil == err {
		if r.Level == DEBUG {
			log.Println(SQL_UPDATE, "RowsAffected:", RowsAffected)
		}
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

func (r MFilesList) Delete(keyNo string, tr *sql.Tx) error {
	l := time.Now()
	delSql := fmt.Sprintf("Delete from  lk_device_mfiles  where id=?")
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
