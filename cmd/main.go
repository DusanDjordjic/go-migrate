package main

import (
	"flag"
	"fmt"
	"go-migrate/pkg/conn"
	"go-migrate/pkg/migrations"
	"os"
	"path/filepath"
	"time"
)

const DSN_ENV = "GO_MIGRATE_DSN"

func main() {
	dsn, err := conn.GetDsn(DSN_ENV)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
	}

	db, err := conn.Connect(dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
	}

	defer db.Close()

	if len(os.Args) < 2 {
		fmt.Println("Usage: [subcommand] [flags]")
		fmt.Println("Available subcommands: init, generate, up, down")
		os.Exit(1)
	}

	switch os.Args[1] {
	case "init":
		initCmd := flag.NewFlagSet("init", flag.ExitOnError)
		dirName := initCmd.String("dir", "migrations", "Migrations directory name (default \"migrations\"")
		tableName := initCmd.String("table", "migrations", "Migrations table name (default \"migrations\")")

		initCmd.Parse(os.Args[2:])

		err := migrations.Init(db, *dirName, *tableName)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		}

	case "generate":
		generateCmd := flag.NewFlagSet("generate", flag.ExitOnError)
		dirName := generateCmd.String("dir", "migrations", "Migrations directory name (default \"migrations\"")
		tableName := generateCmd.String("table", "migrations", "Migrations table name (default \"migrations\")")
		name := generateCmd.String("name", "", "name of migration (required)")

		generateCmd.Parse(os.Args[2:])

		if *name == "" {
			fmt.Fprintf(os.Stderr, "name is required\n")
			generateCmd.Usage()
			os.Exit(1)
		}

		var (
			id              uint
			createdAtString string
			createdAt       time.Time
		)

		err := migrations.CreateMigration(db, *tableName, *name, &id, &createdAtString)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s\n", err.Error())
			os.Exit(1)
		}

		createdAt, err = time.Parse("2006-01-02T15:04:05Z", createdAtString)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to parse created_at, %s\n", err.Error())
			os.Exit(1)
		}

		timestampMillis := createdAt.Unix()

		upFile := fmt.Sprintf("%d_%s.up.sql", timestampMillis, *name)
		downFile := fmt.Sprintf("%d_%s.down.sql", timestampMillis, *name)

		upFile = filepath.Join(*dirName, upFile)
		_, err = os.Create(upFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to create %s file, %s\n", upFile, err.Error())
			os.Exit(1)
		}

		downFile = filepath.Join(*dirName, downFile)
		_, err = os.Create(downFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to create %s file, %s\n", downFile, err.Error())
			os.Remove(upFile)
			os.Exit(1)
		}

	case "up":
		upCmd := flag.NewFlagSet("up", flag.ExitOnError)
		tableName := upCmd.String("table", "migrations", "Migrations table name (default \"migrations\")")
		dirName := upCmd.String("dir", "migrations", "Migrations directory name (default \"migrations\"")
		steps := upCmd.Int("steps", -1, "How many ups you want to do (default -1 meaning all)")
		upCmd.Parse(os.Args[2:])

		if *steps == 0 {
			fmt.Fprintf(os.Stderr, "steps is invalid, it can be -1, or some positive number, but its %d\n", *steps)
			os.Exit(1)
		}

		if *steps < 0 && *steps != -1 {
			fmt.Fprintf(os.Stderr, "steps is invalid, it can be -1, or some positive number, but its %d\n", *steps)
			os.Exit(1)
		}

		err := migrations.RunUpMigrations(db, *dirName, *tableName, *steps)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to execute up migrations, %s\n", err.Error())
			os.Exit(1)
		}

	case "down":
		upCmd := flag.NewFlagSet("up", flag.ExitOnError)
		tableName := upCmd.String("table", "migrations", "Migrations table name (default \"migrations\")")
		dirName := upCmd.String("dir", "migrations", "Migrations directory name (default \"migrations\"")
		steps := upCmd.Int("steps", -1, "How many ups you want to do (default -1 meaning all)")
		upCmd.Parse(os.Args[2:])

		if *steps == 0 {
			fmt.Fprintf(os.Stderr, "steps is invalid, it can be -1, or some positive number, but its %d\n", *steps)
			os.Exit(1)
		}

		if *steps < 0 && *steps != -1 {
			fmt.Fprintf(os.Stderr, "steps is invalid, it can be -1, or some positive number, but its %d\n", *steps)
			os.Exit(1)
		}

		err := migrations.RunDownMigrations(db, *dirName, *tableName, *steps)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to execute up migrations, %s\n", err.Error())
			os.Exit(1)
		}

	default:
		fmt.Printf("Unknown subcommand: %s\n", os.Args[1])
		fmt.Println("Available subcommands: add, list")
		os.Exit(1)
	}
}
