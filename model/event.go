package model

import "time"

type Event struct {
	Client        string
	ClientVersion string
	DataCenter    string
	ProcessedTime time.Time
	Data          string
}

type EventInfo struct {
	EventType             string
	Data                  string
	RetryCount            int
	ProcessAfterTimeStamp time.Time
}
