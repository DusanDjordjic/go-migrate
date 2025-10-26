package runner

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github/DusanDjordjic/go-migrate/pkg/driver"
	"io"
	"os"
	"path/filepath"
	"time"
	"unsafe"
)

const (
	UnlimitedSteps = -1
	up             = true
	down           = false
)

type Runner struct {
	driver driver.Driver
	db     *sql.DB
	config Config
}

type Config struct {
	MigrationsFolder string
}

func New(driver driver.Driver, config Config, connConfig driver.ConnectionConfig) (Runner, error) {
	db, err := driver.Conn(connConfig)
	if err != nil {
		return Runner{}, err
	}

	return Runner{
		driver: driver,
		db:     db,
		config: config,
	}, nil
}

func (r *Runner) Init(ctx context.Context) error {
	_, err := os.Stat(r.config.MigrationsFolder)
	if err == nil {
		goto InitTable
	}

	if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("cannot check \"%s\" migrations folder, %w", r.config.MigrationsFolder, err)
	}

	err = os.Mkdir(r.config.MigrationsFolder, 0755)
	if err != nil {
		return fmt.Errorf("failed to create \"%s\" migrations folder, %w", r.config.MigrationsFolder, err)
	}

InitTable:
	exists, err := r.driver.HasMigrationTable(ctx, r.db)
	if err != nil {
		return fmt.Errorf("failed to check if migrations table exists, %w", err)
	}

	if exists {
		return nil
	}

	return r.driver.CreateMigrationsTable(ctx, r.db)
}

func (r *Runner) New(ctx context.Context, name string) ([2]string, error) {
	timestamp := time.Now().UTC()
	out := [2]string{}

	upfile, err := createMigrationFile(r.config.MigrationsFolder, name, timestamp, true)
	if err != nil {
		return out, err
	}

	downfile, err := createMigrationFile(r.config.MigrationsFolder, name, timestamp, false)
	if err != nil {
		if rmerr := os.Remove(upfile); rmerr != nil {
			return out, errors.Join(err, rmerr)
		} else {
			return out, err
		}
	}

	out[0] = upfile
	out[1] = downfile

	r.driver.AddMigration(ctx, r.db, name, timestamp)
	return out, nil
}

func (r *Runner) Up(ctx context.Context, steps int) error {
	tx, err := r.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to start a transaction, %w", err)
	}

	defer tx.Rollback()

	migrations, err := r.driver.GetMigrations(ctx, tx, driver.ExecutedNo, driver.DirectionAsc)
	if err != nil {
		return err
	}

	// limit steps to number of migrations
	if steps == UnlimitedSteps || steps > len(migrations) {
		steps = len(migrations)
	}

	for i := range steps {
		migration := migrations[i]

		filename := migrationFilename(migration.Name, migration.CreatedAt, up)
		fullpath := filepath.Join(r.config.MigrationsFolder, filename)

		f, err := os.Open(fullpath)
		if err != nil {
			return fmt.Errorf("migration %d: failed to open \"%s\" migration file, %w", i+1, fullpath, err)
		}

		sql, err := io.ReadAll(f)

		f.Close()

		if err != nil {
			return fmt.Errorf("migration %d: failed to read migration from file \"%s\", %w", i+1, fullpath, err)
		}

		err = r.driver.Up(ctx, tx, migration.Name, bytesToString(sql))
		if err != nil {
			return fmt.Errorf("migration %d: failed to execute migration \"%s\", %w", i+1, fullpath, err)
		}
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit transaction, %w", err)
	}

	return nil
}

func (r *Runner) Down(ctx context.Context, steps int) error {
	tx, err := r.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to start a transaction, %w", err)
	}

	defer tx.Rollback()

	migrations, err := r.driver.GetMigrations(ctx, tx, driver.ExecutedYes, driver.DirectionDesc)
	if err != nil {
		return err
	}

	// limit steps to number of migrations
	if steps == UnlimitedSteps || steps > len(migrations) {
		steps = len(migrations)
	}

	for i := range steps {
		migration := migrations[i]

		filename := migrationFilename(migration.Name, migration.CreatedAt, down)
		fullpath := filepath.Join(r.config.MigrationsFolder, filename)

		f, err := os.Open(fullpath)
		if err != nil {
			return fmt.Errorf("migration %d: failed to open \"%s\" migration file, %w", i+1, fullpath, err)
		}

		sql, err := io.ReadAll(f)

		f.Close()

		if err != nil {
			return fmt.Errorf("migration %d: failed to read migration from file \"%s\", %w", i+1, fullpath, err)
		}

		err = r.driver.Down(ctx, tx, migration.Name, bytesToString(sql))
		if err != nil {
			return fmt.Errorf("migration %d: failed to execute migration \"%s\", %w", i+1, fullpath, err)
		}
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit transaction, %w", err)
	}

	return nil
}

func migrationFilename(name string, ts time.Time, up bool) string {
	if up {
		return fmt.Sprintf("%d_%s.up.sql", ts.UTC().Unix(), name)
	} else {
		return fmt.Sprintf("%d_%s.down.sql", ts.UTC().Unix(), name)
	}
}

func createMigrationFile(dir string, name string, ts time.Time, up bool) (string, error) {
	filename := migrationFilename(name, ts, up)
	fullpath := filepath.Join(dir, filename)

	f, err := os.Create(fullpath)
	if err != nil {
		return "", fmt.Errorf("failed to create migration file \"%s\", %w", fullpath, err)
	}
	f.Close()

	return fullpath, nil
}

func bytesToString(b []byte) string {
	return unsafe.String(unsafe.SliceData(b), len(b))
}
