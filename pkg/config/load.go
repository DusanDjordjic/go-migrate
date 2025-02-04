package config

import (
	"fmt"
	"os"
	"strings"
)

const (
	DSN_ENV               = "GO_MIGRATE_DSN"
	DRIVER_ENV            = "GO_MIGRATE_DRIVER"
	SQL_EXEC_ON_START_ENV = "GO_MIGRATE_EXEC_ON_START"
	CONFIG_FILE           = ".gomigrate"
)

func Load() (AppConfig, error) {
	conf := AppConfig{}
	dsn, _ := loadEnv(DSN_ENV)
	driver, _ := loadEnv(DRIVER_ENV)
	sql, _ := loadEnv(SQL_EXEC_ON_START_ENV)
	conf.DSN = dsn
	conf.Driver = driver
	conf.SQLToExecOnStart = sql

	fileContent, err := os.ReadFile(CONFIG_FILE)
	if err != nil {
		fmt.Fprintf(os.Stderr, "WARN: failed to read config file %s, %s", CONFIG_FILE, err.Error())
		if conf.Check() == nil {
			return conf, nil
		}

		return conf, err
	}

	lines := strings.Split(string(fileContent), "\n")
	for index, line := range lines {
		if len(line) == 0 {
			continue
		}

		key, val, found := strings.Cut(line, "=")
		if !found {
			fmt.Fprintf(os.Stderr, "WARN: invalid line %d, %s", index+1, line)
			continue
		}

		switch key {
		case DSN_ENV:
			conf.DSN = val
		case DRIVER_ENV:
			conf.Driver = val
		case SQL_EXEC_ON_START_ENV:
			conf.SQLToExecOnStart = val
		default:
			fmt.Fprintf(os.Stderr, "WARN: invalid variable at %d. line", index)
		}
	}

	if err := conf.Check(); err != nil {
		return conf, err
	}

	return conf, nil
}

func loadEnv(name string) (string, error) {
	s := os.Getenv(name)
	if len(s) == 0 {
		return "", fmt.Errorf("missing %s dsn env", name)
	}

	return s, nil
}
