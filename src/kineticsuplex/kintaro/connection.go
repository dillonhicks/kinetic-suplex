package kintaro

import (
	"time"
)


type Connection struct {
	Id        uint64
	DataChan  chan string
	Timestamp time.Time
	State     int
}
