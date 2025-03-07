package migrations

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
)

func Init(db *sql.DB, dirName string, tableName string) error {
	// Create migrations folder for migration files to be saved to
	err := InitMigrationsFolder(dirName)
	if err != nil {
		return fmt.Errorf("failed to create %s directory, %s", dirName, err.Error())
	}

	err = InitMigrationsTable(db, tableName)
	if err != nil {
		return fmt.Errorf("failed to initialize migrations table, %s", err.Error())
	}

	return nil
}

func InitMigrationsFolder(name string) error {
	exists, err := DoesMigrationsFolderExists(name)
	if err != nil {
		return fmt.Errorf("unknown error happened while checking does %s migrations folder exists, %s", name, err.Error())
	}

	if exists {
		return nil
	}

	err = os.Mkdir(name, 0755)
	if err != nil {
		return fmt.Errorf("failed to create %s migrations folder, %s", name, err.Error())
	}

	return nil
}

func DoesMigrationsFolderExists(name string) (bool, error) {
	_, err := os.Stat(name)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}

	return false, err

}

func InitMigrationsTable(db *sql.DB, tableName string) error {
	_, err := db.Exec(fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
id SERIAL PRIMARY KEY,
created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
name VARCHAR(128) NOT NULL,
executed INTEGER DEFAULT 0
);`, tableName))

	return err
}
