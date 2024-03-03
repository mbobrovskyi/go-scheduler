package scheduler

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/lib/pq"
	"time"

	_ "github.com/lib/pq"
)

const DefaultPostgresTableName = "schedulers"

type PostgreSQLSchedulerEntityRepoOptions struct {
	TableName string
}

type PostgreSQLSchedulerEntityRepo struct {
	db *sql.DB

	tableName string
}

func (s *PostgreSQLSchedulerEntityRepo) Init(ctx context.Context, name string) error {
	ddl := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    		"name" 				VARCHAR  PRIMARY KEY,
			"last_run" 			TIMESTAMP DEFAULT to_timestamp(0),
			"last_finished_at"  TIMESTAMP DEFAULT to_timestamp(0),
			"last_success" 		TIMESTAMP DEFAULT to_timestamp(0),
			"last_error" 		VARCHAR
    )`, s.tableName)

	if _, err := s.db.ExecContext(ctx, ddl); err != nil {
		return err
	}

	query := fmt.Sprintf(`
		INSERT INTO %s (name)
			VALUES ($1)
		ON CONFLICT (name) DO NOTHING`,
		s.tableName)
	_, err := s.db.ExecContext(ctx, query, name)

	return err
}

func (s *PostgreSQLSchedulerEntityRepo) GetAndSetLastRun(ctx context.Context, name string, lastRunTo time.Time) (*SchedulerEntity, error) {
	var schedulerEntity SchedulerEntity

	query := fmt.Sprintf(`
		UPDATE %s
			SET last_run = $1
		WHERE name = $2 AND last_run <= $3
			RETURNING name, last_run, last_finished_at, last_success, last_error
	`, s.tableName)

	var lastRun, lastFinishedAt, lastSuccess pq.NullTime

	err := s.db.QueryRowContext(
		ctx,
		query,
		time.Now().UTC(),
		name,
		lastRunTo.UTC(),
	).
		Scan(
			&schedulerEntity.Name,
			&lastRun,
			&lastFinishedAt,
			&lastSuccess,
			&schedulerEntity.LastError,
		)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}

	schedulerEntity.LastRun = lastRun.Time
	schedulerEntity.LastFinishedAt = lastFinishedAt.Time
	schedulerEntity.LastSuccess = lastSuccess.Time

	return &schedulerEntity, nil
}

func (s *PostgreSQLSchedulerEntityRepo) Save(ctx context.Context, schedulerEntity SchedulerEntity) error {
	query := fmt.Sprintf(`
		INSERT INTO %s (name, last_run, last_finished_at, last_success, last_error)
			VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (name) DO UPDATE
			SET
				name = EXCLUDED.name,
				last_run = EXCLUDED.last_run,
				last_finished_at = EXCLUDED.last_finished_at,
				last_success = EXCLUDED.last_success,
				last_error = EXCLUDED.last_error`,
		s.tableName)
	_, err := s.db.ExecContext(ctx, query,
		schedulerEntity.Name,
		schedulerEntity.LastRun,
		schedulerEntity.LastFinishedAt,
		schedulerEntity.LastSuccess,
		schedulerEntity.LastError,
	)
	return err
}

func NewPostgreSQLSchedulerEntityRepo(
	db *sql.DB,
	options *PostgreSQLSchedulerEntityRepoOptions,
) *PostgreSQLSchedulerEntityRepo {
	repo := &PostgreSQLSchedulerEntityRepo{
		db: db,
	}

	if options != nil && options.TableName != "" {
		repo.tableName = options.TableName
	} else {
		repo.tableName = DefaultPostgresTableName
	}

	return repo
}
