package conn

import (
	"database/sql"
	"fmt"
	"github/DusanDjordjic/go-migrate/pkg/config"

	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
)

func Connect(conf config.AppConfig) (*sql.DB, error) {
	db, err := sql.Open(conf.Driver, conf.DSN)
	if err != nil {
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("failed to ping database, %s", err)
	}

	return db, nil
}
