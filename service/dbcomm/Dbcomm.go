package dbcomm

import (
	"database/sql"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var (
	db   *sql.DB
	ccdb *sql.DB
)

func InitDB(dbUrl string, ccdbUrl string, idleConns int, openConns int) {
	var err error
	db, err = sql.Open("mysql", dbUrl)
	if err != nil {
		log.Println("Open database error:", err)
		return
	}
	if err = db.Ping(); err != nil {
		log.Println("Ping database error:", err)
		return
	}
	db.SetMaxIdleConns(idleConns)
	db.SetMaxOpenConns(openConns)
	db.SetConnMaxLifetime(time.Second * 3600 * 2)
	log.Println("Database Connected successful!")

}

func GetDB() *sql.DB {
	return db
}
