package db

import (
	"database/sql"
)

var db *sql.DB

func Configure(driverName, connStr string) (err error) {
	db, err = sql.Open(driverName, connStr)
	if err != nil {
		return
	}

	if err = db.Ping(); err != nil {
		return
	}

	return nil
}
