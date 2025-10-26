package driver

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lib/pq"
)

func createMigrationTableSql(schemaname, tablename string) string {
	return fmt.Sprintf(createMigrationsTable, schemaname, tablename)
}

func hasMigrationTableSql(schemaname, tablename string) string {
	return fmt.Sprintf(hasMigrationsTable, schemaname, tablename)
}

func getMigrationsSql(schemaname, tablename string, executed Executed, direction Direction) string {
	q := fmt.Sprintf(getMigrations, schemaname, tablename)

	if executed.Bool() {
		q += "WHERE executed = TRUE"
	} else {
		q += "WHERE executed = FALSE"
	}

	q += "\n"

	if direction == DirectionDesc {
		q += "ORDER BY created_at DESC"
	} else {
		q += "ORDER BY created_at ASC"
	}

	return q
}

func insertMigrationSql(schemaname, tablename string) string {
	return fmt.Sprintf(insertMigration, schemaname, tablename)
}

func updateMigrationSql(schemaname, tablename string) string {
	return fmt.Sprintf(updateMigration, schemaname, tablename)
}

const createMigrationsTable = `
CREATE TABLE IF NOT EXISTS %s.%s (
	id SERIAL PRIMARY KEY,
	created_at TIMESTAMP NOT NULL,
	name VARCHAR(128) NOT NULL,
	executed BOOLEAN NOT NULL,
	executed_at TIMESTAMP DEFAULT NULL,
	rolled_back_at TIMESTAMP DEFAULT NULL,
	CONSTRAINT name_unique UNIQUE (name)
);
`

const hasMigrationsTable = `
SELECT EXISTS (
	SELECT 1 
	FROM pg_tables 
	WHERE schemaname = '%s' AND tablename = '%s'
);
`

const getMigrations = `
SELECT id, created_at, name, executed
FROM %s.%s 
`

const insertMigration = `INSERT INTO %s.%s (name, created_at, executed) VALUES ($1, $2, FALSE)`

const updateMigration = `
UPDATE %s.%s SET executed = $2
WHERE name = $1
`

type PostgresqlDriver struct {
	config ConnectionConfig
}

func NewPostgresqlDriver() Driver {
	return new(PostgresqlDriver)
}

func (d *PostgresqlDriver) Conn(config ConnectionConfig) (*sql.DB, error) {
	d.config = config

	db, err := sql.Open("postgres", config.DSN)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database, %w", err)
	}

	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("failed to ping database, %w", err)
	}

	return db, nil
}

func (d *PostgresqlDriver) CreateMigrationsTable(ctx context.Context, exec Executor) error {
	q := createMigrationTableSql(d.config.Schema, d.config.Table)

	_, err := exec.ExecContext(ctx, q)
	if err != nil {
		return fmt.Errorf("cannot create migrations table, %w\nquery:\n%s\n,", err, q)
	}

	return nil
}

func (d *PostgresqlDriver) HasMigrationTable(ctx context.Context, exec Executor) (bool, error) {
	exists := false
	q := hasMigrationTableSql(d.config.Schema, d.config.Table)

	res := exec.QueryRowContext(ctx, q)
	err := res.Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check if %s.%s table exists, %w\nquery:\n%s\n", d.config.Schema, d.config.Table, err, q)
	}

	return exists, nil
}

func (d *PostgresqlDriver) GetMigrations(ctx context.Context, exec Executor, executed Executed, direction Direction) ([]Migration, error) {
	q := getMigrationsSql(d.config.Schema, d.config.Table, executed, direction)

	rows, err := exec.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("failed to get migrations from %s.%s, %w\nquery:\n%s\n", d.config.Schema, d.config.Table, err, q)
	}
	defer rows.Close()

	migrations, err := d.scanMigrations(rows)
	if err != nil {
		return nil, fmt.Errorf("failed to scan migrations from %s.%s, %w", d.config.Schema, d.config.Schema, err)
	}

	return migrations, nil
}

func (d *PostgresqlDriver) scanMigrations(rows *sql.Rows) ([]Migration, error) {
	var migrations []Migration = make([]Migration, 0, 16)

	for rows.Next() {
		var (
			m            Migration
			executedBool bool
		)

		err := rows.Scan(&m.ID, &m.CreatedAt, &m.Name, &executedBool)
		if err != nil {
			return nil, err
		}

		if executedBool {
			m.Executed = ExecutedYes
		} else {
			m.Executed = ExecutedNo
		}

		m.CreatedAt = m.CreatedAt.UTC()
		migrations = append(migrations, m)
	}

	return migrations, nil

}

func (d *PostgresqlDriver) updateMigration(ctx context.Context, exec Executor, name string, executed Executed) error {
	q := updateMigrationSql(d.config.Schema, d.config.Table)

	_, err := exec.ExecContext(ctx, q, name, executed)
	if err != nil {
		return fmt.Errorf("failed to update migration into %s.%s, %w\nquery:\n%s\n", d.config.Schema, d.config.Table, err, q)
	}

	return nil
}

func (d *PostgresqlDriver) executeMigration(ctx context.Context, exec Executor, name, sql string, executed Executed) error {
	_, err := exec.ExecContext(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to execute migration %s, %w\nquery:\n%s\n", name, err, sql)
	}

	err = d.updateMigration(ctx, exec, name, executed)
	if err != nil {
		return err
	}

	return nil
}

func (d *PostgresqlDriver) Up(ctx context.Context, exec Executor, name, sql string) error {
	return d.executeMigration(ctx, exec, name, sql, ExecutedYes)
}

func (d *PostgresqlDriver) Down(ctx context.Context, exec Executor, name, sql string) error {
	return d.executeMigration(ctx, exec, name, sql, ExecutedNo)
}

func (d *PostgresqlDriver) AddMigration(ctx context.Context, exec Executor, name string, ts time.Time) error {
	q := insertMigrationSql(d.config.Schema, d.config.Table)

	_, err := exec.ExecContext(ctx, q, name, ts)
	if err != nil {
		return fmt.Errorf("failed to insert migration into %s.%s, %w\nquery:\n%s\n", d.config.Schema, d.config.Table, err, q)
	}

	return nil
}
