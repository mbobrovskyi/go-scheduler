package postgres

import (
	"database/sql"
	"github.com/mbobrovskyi/go-scheduler/pkg/database/common"
	"golang.org/x/net/context"

	_ "github.com/lib/pq"
)

func NewPostgresClient(ctx context.Context, dsn string) (*sql.DB, error) {
	return common.NewSQLClientWithDriver(ctx, dsn, "postgres")
}
