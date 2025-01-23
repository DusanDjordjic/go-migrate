package models

import "time"

type Migration struct {
	ID        uint
	CreatedAt time.Time
	Name      string
	Executed  bool
}
