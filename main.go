package main

import (
	"archive-ingest/ingest"
	"archive-ingest/parse"
	"archive-ingest/util"
)

var logger = util.NewLogger()

func main() {
	err := ingest.Read(".", func(entity *parse.Entity) {
		logger.Info(entity)
	})

	if err != nil {
		logger.Fatal(err)
	}
}
