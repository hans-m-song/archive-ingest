package io

import (
	"archive-ingest/util"
)

var logger = util.NewLogger()

type AnnouncerParameters struct {
	User, Pass, Host, Port string
}

type AnnouncerControls interface {
	Connect(name string) error
	Say(data interface{}) error
	Close() error
}
