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
	exists, err := DoesMigrationsTableExist(db, tableName)
	if err != nil {
		return err
	}

	if exists {
		return fmt.Errorf("table %s already exists", tableName)
	}

	db.Exec(fmt.Sprintf(`
CREATE TABLE %s (
id INTEGER PRIMARY KEY AUTOINCREMENT,
created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
name VARCHAR(128) NOT NULL,
executed INTEGER DEFAULT 0
);`, tableName))

	return nil
}

func DoesMigrationsTableExist(db *sql.DB, tableName string) (bool, error) {
	rows, err := db.Query(`
SELECT name
FROM sqlite_master
WHERE type='table' AND name=$1;
`, tableName)
	if err != nil {
		return true, fmt.Errorf("failed to query existing tables, %s", err.Error())
	}

	defer rows.Close()

	res := rows.Next()
	if res {
		// There exists something so its the migrations table
		return true, nil
	}

	err = rows.Err()
	if err != nil {
		return true, fmt.Errorf("failed to get query results, %s", err.Error())
	}

	return false, nil
}
