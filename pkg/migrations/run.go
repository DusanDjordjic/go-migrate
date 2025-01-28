package migrations

import (
	"context"
	"database/sql"
	"fmt"
	"github/DusanDjordjic/go-migrate/pkg/models"
	"os"
	"path/filepath"
	"time"
)

type Direction int

const (
	ASC  Direction = 1
	DESC Direction = 2
)

func RunUpMigrations(db *sql.DB, dirName string, tableName string, steps int) error {
	migrations, err := loadMigrations(db, tableName, ASC)
	if err != nil {
		return err
	}

	fmt.Printf("Migrations %d\n", len(migrations))

	if steps == -1 {
		steps = len(migrations)
	}

	for _, migration := range migrations {
		if migration.Executed {
			continue
		}

		if steps == 0 {
			break
		}
		steps--

		upFile := fmt.Sprintf("%d_%s.up.sql", migration.CreatedAt.Unix(), migration.Name)
		upWholePath := filepath.Join(dirName, upFile)
		fmt.Printf("%v UP: %s\n", migration.CreatedAt, upWholePath)

		query, err := os.ReadFile(upWholePath)
		if err != nil {
			return fmt.Errorf("failed to read %s file, %s", upWholePath, err.Error())
		}
		// run migration
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err = runMigration(ctx, db, tableName, &migration, string(query))
		if err != nil {
			return fmt.Errorf("failed to execute %s migration, %s", migration.Name, err.Error())
		}

	}

	return nil

}

func RunDownMigrations(db *sql.DB, dirName string, tableName string, steps int) error {
	migrations, err := loadMigrations(db, tableName, DESC)
	if err != nil {
		return err
	}

	fmt.Printf("Migrations %d\n", len(migrations))

	if steps == -1 {
		steps = len(migrations)
	}

	for _, migration := range migrations {
		if !migration.Executed {
			continue
		}

		if steps == 0 {
			break
		}
		steps--

		downFile := fmt.Sprintf("%d_%s.down.sql", migration.CreatedAt.Unix(), migration.Name)
		downWholePath := filepath.Join(dirName, downFile)
		fmt.Printf("%v DOWN: %s\n", migration.CreatedAt, downWholePath)

		query, err := os.ReadFile(downWholePath)
		if err != nil {
			return fmt.Errorf("failed to read %s file, %s", downWholePath, err.Error())
		}
		// run migration
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err = runMigration(ctx, db, tableName, &migration, string(query))
		if err != nil {
			return fmt.Errorf("failed to execturte %s migration, %s", migration.Name, err.Error())
		}
	}

	return nil

}

func runMigration(ctx context.Context, db *sql.DB, tableName string, migration *models.Migration, query string) error {
	doneChan := make(chan error, 1)

	go func() {
		tx, err := db.BeginTx(ctx, nil)

		if err != nil {
			doneChan <- fmt.Errorf("failed to begin transaction, %s", err.Error())
			tx.Rollback()
			return
		}

		_, err = tx.Exec(query)
		if err != nil {
			doneChan <- fmt.Errorf("failed to execute query, %s", err.Error())
			tx.Rollback()
			return
		}

		var executed int
		if migration.Executed {
			executed = 0
		} else {
			executed = 1
		}

		res, err := tx.Exec(fmt.Sprintf("UPDATE %s SET executed = $1 WHERE id = $2", tableName), executed, migration.ID)
		if err != nil {
			doneChan <- fmt.Errorf("failed to update migration executed status, %s", err.Error())
			tx.Rollback()
			return
		}

		rowsAffected, err := res.RowsAffected()

		if err != nil {
			doneChan <- fmt.Errorf("failed to get rows affetced, %s", err.Error())
			tx.Rollback()
			return
		}

		if rowsAffected != 1 {
			doneChan <- fmt.Errorf("rows affected is not 1 but %d", rowsAffected)
			tx.Rollback()
			return
		}

		tx.Commit()
		migration.Executed = executed == 1
		doneChan <- nil
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-doneChan:
		return err
	}
}

func loadMigrations(db *sql.DB, tableName string, dir Direction) ([]models.Migration, error) {

	var migrations []models.Migration
	var directionString string
	if dir == ASC {
		directionString = "ASC"
	} else if dir == DESC {
		directionString = "DESC"
	} else {
		fmt.Fprintf(os.Stderr, "invalid direction parameter")
		os.Exit(1)
	}

	rows, err := db.Query(fmt.Sprintf(`
SELECT id, created_at, name, executed
FROM %s
ORDER BY created_at %s`, tableName, directionString))
	if err != nil {
		return migrations, fmt.Errorf("failed to get migrations from %s table, %s", tableName, err)
	}
	defer rows.Close()

	for rows.Next() {
		v := models.Migration{}
		err := rows.Scan(&v.ID, &v.CreatedAt, &v.Name, &v.Executed)
		if err != nil {
			return migrations, fmt.Errorf("failed to scan, %s", err)
		}

		migrations = append(migrations, v)
	}

	return migrations, nil
}
