package main

import (
	"context"
	"flag"
	"fmt"
	"github/DusanDjordjic/go-migrate/pkg/config"
	"github/DusanDjordjic/go-migrate/pkg/driver"
	"github/DusanDjordjic/go-migrate/pkg/runner"
	"os"
)

func main() {
	conf, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}

	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: [subcommand] [flags]\n")
		fmt.Fprintf(os.Stderr, "Available subcommands: init, new, up, down")
		os.Exit(1)
	}

	var d driver.Driver

	switch conf.Driver {
	case "postgres":
		d = driver.NewPostgresqlDriver()
	default:
		fmt.Fprintf(os.Stderr, "unsupported driver %s. supported drivers %v", conf.Driver, config.AVAILABLE_DRIVERS)
		os.Exit(1)
	}

	r, err := runner.New(
		d,
		runner.Config{
			MigrationsFolder: "migrations",
		},
		driver.ConnectionConfig{
			DSN:    conf.DSN,
			Table:  "migrations",
			Schema: "public",
		},
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to connect to the database, %s", err.Error())
		os.Exit(1)
	}

	ctx := context.Background()

	switch os.Args[1] {
	case "init":
		initCmd := flag.NewFlagSet("init", flag.ExitOnError)

		initCmd.Parse(os.Args[2:])

		err := r.Init(ctx)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s\n", err.Error())
			os.Exit(1)
		}

	case "new":
		newCmd := flag.NewFlagSet("new", flag.ExitOnError)
		name := newCmd.String("name", "", "name of migration (required)")

		newCmd.Parse(os.Args[2:])

		if *name == "" {
			fmt.Fprintf(os.Stderr, "name is required\n")
			newCmd.Usage()
			os.Exit(1)
		}

		r.New(ctx, *name)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s\n", err.Error())
			os.Exit(1)
		}

	case "up":
		upCmd := flag.NewFlagSet("up", flag.ExitOnError)
		steps := upCmd.Int("steps", -1, "How many ups you want to do (-1 means all) (-1 default)")
		upCmd.Parse(os.Args[2:])

		if *steps == 0 {
			fmt.Fprintf(os.Stderr, "steps is invalid, it can be -1, or some positive number, but its %d\n", *steps)
			os.Exit(1)
		}

		if *steps < 0 && *steps != -1 {
			fmt.Fprintf(os.Stderr, "steps is invalid, it can be -1, or some positive number, but its %d\n", *steps)
			os.Exit(1)
		}

		err := r.Up(ctx, *steps)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to execute up migrations, %s\n", err.Error())
			os.Exit(1)
		}

	case "down":
		downCmd := flag.NewFlagSet("down", flag.ExitOnError)
		steps := downCmd.Int("steps", 1, "How many downs you want to do (-1 means all) (1 default)")
		downCmd.Parse(os.Args[2:])

		if *steps == 0 {
			fmt.Fprintf(os.Stderr, "steps is invalid, it can be -1, or some positive number, but its %d\n", *steps)
			os.Exit(1)
		}

		if *steps < 0 && *steps != -1 {
			fmt.Fprintf(os.Stderr, "steps is invalid, it can be -1, or some positive number, but its %d\n", *steps)
			os.Exit(1)
		}

		err := r.Down(ctx, *steps)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to execute down migrations, %s\n", err.Error())
			os.Exit(1)
		}

	default:
		fmt.Printf("Unknown subcommand: %s\n", os.Args[1])
		fmt.Println("Available subcommands: init, new, up, down")
		os.Exit(1)
	}
}
