package conn

import (
	"fmt"
	"os"
)

func GetDsn(envName string) (string, error) {
	s := os.Getenv(envName)
	if len(s) == 0 {
		return "", fmt.Errorf("missing %s dsn env", envName)
	}

	return s, nil
}
