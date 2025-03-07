package config

import "fmt"

var AVAILABLE_DRIVERS = [...]string{"sqlite3", "postgres", "mysql"}

type AppConfig struct {
	DSN              string
	Driver           string
	SQLToExecOnStart string
}

func (app *AppConfig) Check() error {
	if len(app.DSN) == 0 {
		return fmt.Errorf("failed to load %s", DSN_ENV)
	}

	if len(app.Driver) == 0 {
		return fmt.Errorf("failed to load %s", DRIVER_ENV)
	}

	found := false
	for _, d := range AVAILABLE_DRIVERS {
		if app.Driver == d {
			found = true
			break
		}
	}

	if !found {
		return fmt.Errorf("Driver \"%s\" is not supported yet", app.Driver)
	}

	return nil
}
