package main

import (
	"archive-ingest/ingest"
	"archive-ingest/parse"
	"archive-ingest/util"
)

var logger = util.NewLogger()

func main() {
	logger.Info("hello world")
	ingest.Read(".", func(entity parse.Entity) {
		logger.Info(entity)
	})
}
