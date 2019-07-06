package vers

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
	Id          int64  `json:"id"`
	Sn          string `json:"sn"`
	DeviceVer   string `json:"device_ver"`
	HwVer       string `json:"hw_ver"`
	SwVer       string `json:"sw_ver"`
	InstallDate string `json:"install_date"`
	UpNote      string `json:"up_note"`
	CreateTime  string `json:"create_time"`
	CreateBy    int64  `json:"create_by"`
	UpdateTime  string `json:"update_time"`
	UpdateBy    int64  `json:"update_by"`
	PageNo      int    `json:"page_no"`
	PageSize    int    `json:"page_size"`
	ExtraWhere  string `json:"extra_where"`
	SortFld     string `json:"sort_fld"`
}

type versList struct {
	DB    *sql.DB
	Level int
	Total int    `json:"total"`
	verss []vers `json:"vers"`
}

type vers struct {
	Id          int64  `json:"id"`
	Sn          string `json:"sn"`
	DeviceVer   string `json:"device_ver"`
	HwVer       string `json:"hw_ver"`
	SwVer       string `json:"sw_ver"`
	InstallDate string `json:"install_date"`
	UpNote      string `json:"up_note"`
	CreateTime  string `json:"create_time"`
	CreateBy    int64  `json:"create_by"`
	UpdateTime  string `json:"update_time"`
	UpdateBy    int64  `json:"update_by"`
}

type Form struct {
	Form vers `json:"vers"`
}

/*
	说明：创建实例对象
	入参：db:数据库sql.DB, 数据库已经连接, level:日志级别
	出参：实例对象
*/

func New(db *sql.DB, level int) *versList {
	if db == nil {
		log.Println(SQL_SELECT, "Database is nil")
		return nil
	}
	return &versList{DB: db, Total: 0, verss: make([]vers, 0), Level: level}
}

/*
	说明：创建实例对象
	入参：url:连接数据的url, 数据库还没有CONNECTED, level:日志级别
	出参：实例对象
*/

func NewUrl(url string, level int) *versList {
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
	return &versList{DB: db, Total: 0, verss: make([]vers, 0), Level: level}
}

/*
	说明：得到符合条件的总条数
	入参：s: 查询条件
	出参：参数1：返回符合条件的总条件, 参数2：如果错误返回错误对象
*/

func (r *versList) GetTotal(s Search) (int, error) {
	var where string
	l := time.Now()

	if s.Id != 0 {
		where += " and id=" + fmt.Sprintf("%d", s.Id)
	}

	if s.Sn != "" {
		where += " and sn='" + s.Sn + "'"
	}

	if s.DeviceVer != "" {
		where += " and device_ver='" + s.DeviceVer + "'"
	}

	if s.HwVer != "" {
		where += " and hw_ver='" + s.HwVer + "'"
	}

	if s.SwVer != "" {
		where += " and sw_ver='" + s.SwVer + "'"
	}

	if s.InstallDate != "" {
		where += " and install_date='" + s.InstallDate + "'"
	}

	if s.UpNote != "" {
		where += " and up_note='" + s.UpNote + "'"
	}

	if s.CreateTime != "" {
		where += " and create_time='" + s.CreateTime + "'"
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

	if s.ExtraWhere != "" {
		where += s.ExtraWhere
	}

	qrySql := fmt.Sprintf("Select count(1) as total from lk_device_ver   where 1=1 %s", where)
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

func (r versList) Get(s Search) (*vers, error) {
	var where string
	l := time.Now()

	if s.Id != 0 {
		where += " and id=" + fmt.Sprintf("%d", s.Id)
	}

	if s.Sn != "" {
		where += " and sn='" + s.Sn + "'"
	}

	if s.DeviceVer != "" {
		where += " and device_ver='" + s.DeviceVer + "'"
	}

	if s.HwVer != "" {
		where += " and hw_ver='" + s.HwVer + "'"
	}

	if s.SwVer != "" {
		where += " and sw_ver='" + s.SwVer + "'"
	}

	if s.InstallDate != "" {
		where += " and install_date='" + s.InstallDate + "'"
	}

	if s.UpNote != "" {
		where += " and up_note='" + s.UpNote + "'"
	}

	if s.CreateTime != "" {
		where += " and create_time='" + s.CreateTime + "'"
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

	if s.ExtraWhere != "" {
		where += s.ExtraWhere
	}

	qrySql := fmt.Sprintf("Select id,sn,device_ver,hw_ver,sw_ver,install_date,up_note,create_time,create_by,update_time,update_by, from lk_device_ver where 1=1 %s ", where)
	if r.Level == DEBUG {
		log.Println(SQL_SELECT, qrySql)
	}
	rows, err := r.DB.Query(qrySql)
	if err != nil {
		log.Println(SQL_ERROR, err.Error())
		return nil, err
	}
	defer rows.Close()

	var p vers
	if !rows.Next() {
		return nil, fmt.Errorf("Not Finded Record")
	} else {
		err := rows.Scan(&p.Id, &p.Sn, &p.DeviceVer, &p.HwVer, &p.SwVer, &p.InstallDate, &p.UpNote, &p.CreateTime, &p.CreateBy, &p.UpdateTime, &p.UpdateBy)
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

func (r *versList) GetList(s Search) ([]vers, error) {
	var where string
	l := time.Now()

	if s.Id != 0 {
		where += " and id=" + fmt.Sprintf("%d", s.Id)
	}

	if s.Sn != "" {
		where += " and sn='" + s.Sn + "'"
	}

	if s.DeviceVer != "" {
		where += " and device_ver='" + s.DeviceVer + "'"
	}

	if s.HwVer != "" {
		where += " and hw_ver='" + s.HwVer + "'"
	}

	if s.SwVer != "" {
		where += " and sw_ver='" + s.SwVer + "'"
	}

	if s.InstallDate != "" {
		where += " and install_date='" + s.InstallDate + "'"
	}

	if s.UpNote != "" {
		where += " and up_note='" + s.UpNote + "'"
	}

	if s.CreateTime != "" {
		where += " and create_time='" + s.CreateTime + "'"
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

	if s.ExtraWhere != "" {
		where += s.ExtraWhere
	}

	var qrySql string
	if s.PageSize == 0 && s.PageNo == 0 {
		qrySql = fmt.Sprintf("Select id,sn,device_ver,hw_ver,sw_ver,install_date,up_note,create_time,create_by,update_time,update_by, from lk_device_ver where 1=1 %s", where)
	} else {
		qrySql = fmt.Sprintf("Select id,sn,device_ver,hw_ver,sw_ver,install_date,up_note,create_time,create_by,update_time,update_by, from lk_device_ver where 1=1 %s Limit %d offset %d", where, s.PageSize, (s.PageNo-1)*s.PageSize)
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

	var p vers
	for rows.Next() {
		rows.Scan(&p.Id, &p.Sn, &p.DeviceVer, &p.HwVer, &p.SwVer, &p.InstallDate, &p.UpNote, &p.CreateTime, &p.CreateBy, &p.UpdateTime, &p.UpdateBy)
		r.verss = append(r.verss, p)
	}
	log.Println(SQL_ELAPSED, r)
	if r.Level == DEBUG {
		log.Println(SQL_ELAPSED, time.Since(l))
	}
	return r.verss, nil
}

/*
	说明：根据主键查询符合条件的记录，并保持成MAP
	入参：s: 查询条件
	出参：参数1：返回符合条件的对象, 参数2：如果错误返回错误对象
*/

func (r *versList) GetExt(s Search) (map[string]string, error) {
	var where string
	l := time.Now()

	if s.Id != 0 {
		where += " and id=" + fmt.Sprintf("%d", s.Id)
	}

	if s.Sn != "" {
		where += " and sn='" + s.Sn + "'"
	}

	if s.DeviceVer != "" {
		where += " and device_ver='" + s.DeviceVer + "'"
	}

	if s.HwVer != "" {
		where += " and hw_ver='" + s.HwVer + "'"
	}

	if s.SwVer != "" {
		where += " and sw_ver='" + s.SwVer + "'"
	}

	if s.InstallDate != "" {
		where += " and install_date='" + s.InstallDate + "'"
	}

	if s.UpNote != "" {
		where += " and up_note='" + s.UpNote + "'"
	}

	if s.CreateTime != "" {
		where += " and create_time='" + s.CreateTime + "'"
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

	qrySql := fmt.Sprintf("Select id,sn,device_ver,hw_ver,sw_ver,install_date,up_note,create_time,create_by,update_time,update_by, from lk_device_ver where 1=1 %s ", where)
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

func (r versList) Insert(p vers) error {
	l := time.Now()
	exeSql := fmt.Sprintf("Insert into  lk_device_ver(sn,device_ver,hw_ver,sw_ver,install_date,up_note,create_time,create_by,update_by,)  values(?,?,?,?,?,?,?,?,?,?,?,)")
	if r.Level == DEBUG {
		log.Println(SQL_INSERT, exeSql)
	}
	_, err := r.DB.Exec(exeSql, p.Sn, p.DeviceVer, p.HwVer, p.SwVer, p.InstallDate, p.UpNote, p.CreateTime, p.CreateBy, p.UpdateBy)
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

func (r versList) InsertEntity(p vers, tr *sql.Tx) error {
	l := time.Now()
	var colNames, colTags string
	valSlice := make([]interface{}, 0)

	if p.Sn != "" {
		colNames += "sn,"
		colTags += "?,"
		valSlice = append(valSlice, p.Sn)
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

	if p.SwVer != "" {
		colNames += "sw_ver,"
		colTags += "?,"
		valSlice = append(valSlice, p.SwVer)
	}

	if p.InstallDate != "" {
		colNames += "install_date,"
		colTags += "?,"
		valSlice = append(valSlice, p.InstallDate)
	}

	if p.UpNote != "" {
		colNames += "up_note,"
		colTags += "?,"
		valSlice = append(valSlice, p.UpNote)
	}

	if p.CreateTime != "" {
		colNames += "create_time,"
		colTags += "?,"
		valSlice = append(valSlice, p.CreateTime)
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

	colNames = strings.TrimRight(colNames, ",")
	colTags = strings.TrimRight(colTags, ",")
	exeSql := fmt.Sprintf("Insert into  lk_device_ver(%s)  values(%s)", colNames, colTags)
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

func (r versList) InsertMap(m map[string]interface{}, tr *sql.Tx) error {
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

	exeSql := fmt.Sprintf("Insert into  lk_device_ver(%s)  values(%s)", colNames, colTags)
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

func (r versList) UpdataEntity(keyNo string, p vers, tr *sql.Tx) error {
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

	if p.DeviceVer != "" {
		colNames += "device_ver=?,"

		valSlice = append(valSlice, p.DeviceVer)
	}

	if p.HwVer != "" {
		colNames += "hw_ver=?,"

		valSlice = append(valSlice, p.HwVer)
	}

	if p.SwVer != "" {
		colNames += "sw_ver=?,"

		valSlice = append(valSlice, p.SwVer)
	}

	if p.InstallDate != "" {
		colNames += "install_date=?,"

		valSlice = append(valSlice, p.InstallDate)
	}

	if p.UpNote != "" {
		colNames += "up_note=?,"

		valSlice = append(valSlice, p.UpNote)
	}

	if p.CreateTime != "" {
		colNames += "create_time=?,"

		valSlice = append(valSlice, p.CreateTime)
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

	colNames = strings.TrimRight(colNames, ",")
	valSlice = append(valSlice, keyNo)

	exeSql := fmt.Sprintf("update  lk_device_ver  set %s  where id=? ", colNames)
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

func (r versList) UpdateMap(keyNo string, m map[string]interface{}, tr *sql.Tx) error {
	l := time.Now()

	var colNames string
	valSlice := make([]interface{}, 0)
	for k, v := range m {
		colNames += k + "=?,"
		valSlice = append(valSlice, v)
	}
	valSlice = append(valSlice, keyNo)
	colNames = strings.TrimRight(colNames, ",")
	updateSql := fmt.Sprintf("Update lk_device_ver set %s where id=?", colNames)
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

func (r versList) Delete(keyNo string, tr *sql.Tx) error {
	l := time.Now()
	delSql := fmt.Sprintf("Delete from  lk_device_ver  where id=?")
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
