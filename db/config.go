package db

import (
	"database/sql"
)

var db *sql.DB

func Configure(connStr string) error {
	var err error
	db, err = sql.Open("pgx", connStr)
	if err != nil {
		return err
	}

	if err := db.Ping(); err != nil {
		return err
	}

	return nil
}
