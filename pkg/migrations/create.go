package migrations

import (
	"database/sql"
	"fmt"
)

func CreateMigration(db *sql.DB, table string, name string, id *uint, createdAt *string) error {
	if id == nil {
		return fmt.Errorf("out parameter id is nil")
	}
	if createdAt == nil {
		return fmt.Errorf("out parameter createdAt is nil")
	}

	result, err := db.Exec(fmt.Sprintf(`
INSERT INTO %s
(name, executed)
VALUES ($1, $2)
`, table), name, false)
	if err != nil {
		return fmt.Errorf("failed to insert \"%s\" migration, %s", name, err.Error())
	}
	lastInsertedId, err := result.LastInsertId()
	if err != nil {
		return fmt.Errorf("failed to get last inserted id, %s", err.Error())
	}

	err = db.QueryRow(fmt.Sprintf(`
SELECT id, created_at
FROM %s
WHERE id = $1
`, table), lastInsertedId).Scan(id, createdAt)

	if err != nil {
		return fmt.Errorf("failed to select last inserted row's id and created_at, %s", err.Error())
	}

	return nil
}
