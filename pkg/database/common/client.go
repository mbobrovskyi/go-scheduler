package common

import (
	"context"
	"database/sql"
)

func NewSQLClientWithDriver(ctx context.Context, dsn, driver string) (*sql.DB, error) {
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, err
	}
	if err = db.PingContext(ctx); err != nil {
		return nil, err
	}
	return db, nil
}
