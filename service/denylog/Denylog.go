package denylog

import (
	"database/sql"
	"fmt"
	"log"
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
	Sn         string `json:"sn"`
	DeviceName string `json:"device_name"`
	OnlineTime string `json:"online_time"`
	Reason     string `json:"reason"`
	PageNo     int    `json:"page_no"`
	PageSize   int    `json:"page_size"`
	ExtraWhere string `json:"extra_where"`
	SortFld    string `json:"sort_fld"`
}

type DenyList struct {
	DB     *sql.DB
	Level  int
	Total  int    `json:"total"`
	Denies []Deny `json:"Deny"`
}

type Deny struct {
	Id         int64  `json:"id"`
	Sn         string `json:"sn"`
	Reason     string `json:"reason"`
	DeviceName string `json:"device_name"`
	OnlineTime string `json:"online_time"`
}

type Form struct {
	Form Deny `json:"Deny"`
}

/*
	说明：创建实例对象
	入参：db:数据库sql.DB, 数据库已经连接, level:日志级别
	出参：实例对象
*/

func New(db *sql.DB, level int) *DenyList {
	if db == nil {
		log.Println(SQL_SELECT, "Database is nil")
		return nil
	}
	return &DenyList{DB: db, Total: 0, Denies: make([]Deny, 0), Level: level}
}

/*
	说明：创建实例对象
	入参：url:连接数据的url, 数据库还没有CONNECTED, level:日志级别
	出参：实例对象
*/

func NewUrl(url string, level int) *DenyList {
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
	return &DenyList{DB: db, Total: 0, Denies: make([]Deny, 0), Level: level}
}

func (r DenyList) Insert(p Deny) error {
	l := time.Now()
	exeSql := fmt.Sprintf("Insert into  lk_deny_log(sn,device_name,online_time,reason)  values(?,?,?,?)")
	if r.Level == DEBUG {
		log.Println(SQL_INSERT, exeSql)
	}
	_, err := r.DB.Exec(exeSql, p.Sn, p.DeviceName, p.OnlineTime, p.Reason)
	if err != nil {
		log.Println(SQL_ERROR, err.Error())
		return err
	}
	if r.Level == DEBUG {
		log.Println(SQL_ELAPSED, time.Since(l))
	}
	return nil
}
