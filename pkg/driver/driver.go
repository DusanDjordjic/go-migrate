package driver

import (
	"context"
	"database/sql"
	"strconv"
	"time"
)

type ConnectionConfig struct {
	DSN string
	// name of the migrations table
	Table string
	// schema name, used only by postgres
	Schema string
}

type Migration struct {
	ID        uint
	CreatedAt time.Time
	Name      string
	Executed  Executed
}

type (
	Executed  uint8
	Direction uint8
)

func (exe Executed) Bool() bool {
	switch exe {
	case ExecutedYes:
		return true
	case ExecutedNo:
		return false
	default:
		panic("invalid exeucted value " + strconv.FormatInt(int64(exe), 10))
	}
}

const (
	ExecutedYes   Executed  = 1
	ExecutedNo    Executed  = 0
	DirectionDesc Direction = 1
	DirectionAsc  Direction = 0
)

type Executor interface {
	Exec(query string, args ...any) (sql.Result, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	Query(query string, args ...any) (*sql.Rows, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRow(query string, args ...any) *sql.Row
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

type Driver interface {
	Conn(config ConnectionConfig) (*sql.DB, error)
	CreateMigrationsTable(ctx context.Context, exec Executor) error
	HasMigrationTable(ctx context.Context, exec Executor) (bool, error)
	// Gets migrations from database, sorted by time of creation
	GetMigrations(ctx context.Context, exec Executor, executed Executed, direction Direction) ([]Migration, error)
	// Adds a new migration to a database and sets it's executed flag to false by default
	AddMigration(ctx context.Context, exec Executor, name string, ts time.Time) error
	// Executed a migration and updates the migration setting executed to true
	Up(ctx context.Context, exec Executor, name, sql string) error
	// Executed a migration and updates the migration setting executed to false
	Down(ctx context.Context, exec Executor, name, sql string) error
}
