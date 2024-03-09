package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/lib/pq"
	"github.com/mbobrovskyi/goscheduler/entity"
	"time"

	_ "github.com/lib/pq"
)

const DefaultPostgresTableName = "schedulers"

type PostgresSchedulerEntityRepoOptions struct {
	TableName string
}

type PostgresSchedulerEntityRepo struct {
	db *sql.DB

	tableName string
}

func (s *PostgresSchedulerEntityRepo) Init(ctx context.Context, name string) error {
	ddl := fmt.Sprintf(`
		BEGIN;
		SELECT PG_ADVISORY_XACT_LOCK(%d);		
		CREATE TABLE IF NOT EXISTS %s (
    		"name" 				VARCHAR   PRIMARY KEY,
			"last_run" 			TIMESTAMP DEFAULT to_timestamp(0),
			"last_finished_at"  TIMESTAMP DEFAULT to_timestamp(0),
			"last_success" 		TIMESTAMP DEFAULT to_timestamp(0),
			"last_error" 		VARCHAR
    	);
		COMMIT;
    `, time.Now().Unix(), s.tableName)

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

func (s *PostgresSchedulerEntityRepo) GetAndSetLastRun(ctx context.Context, name string, lastRunTo time.Time) (*entity.SchedulerEntity, error) {
	var schedulerEntity entity.SchedulerEntity

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

func (s *PostgresSchedulerEntityRepo) Save(ctx context.Context, schedulerEntity entity.SchedulerEntity) error {
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

func NewPostgresSchedulerEntity(db *sql.DB) *PostgresSchedulerEntityRepo {
	return NewPostgresSchedulerEntityWithOptions(db, PostgresSchedulerEntityRepoOptions{})
}

func NewPostgresSchedulerEntityWithOptions(
	db *sql.DB,
	options PostgresSchedulerEntityRepoOptions,
) *PostgresSchedulerEntityRepo {
	repo := &PostgresSchedulerEntityRepo{
		db:        db,
		tableName: DefaultPostgresTableName,
	}

	if options.TableName != "" {
		repo.tableName = options.TableName
	}

	return repo
}
