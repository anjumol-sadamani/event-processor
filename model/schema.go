package model

type Schema struct {
	Id          int64
	EventSchema string
}

type SchemaColumn struct {
	Id    int64
	Query string
}
