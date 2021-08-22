package main

import (
	"archive-ingest/announcer"
	"archive-ingest/ingest"
	"archive-ingest/parse"
	"archive-ingest/util"
	"os"
)

var logger = util.NewLogger()

func main() {
	state, err := announcer.Connect("amqp://guest:guest@localhost:5672", "queue")
	if err != nil {
		logger.Fatal(err)
	}

	defer state.Close()

	rootDir := os.Args[1]

	if len(rootDir) < 1 {
		rootDir = "."
	}

	logger.Debug("ingesting directory " + rootDir)

	err = ingest.Read(rootDir, func(entity *parse.Entity) {
		announcer.Announce(state, entity)
	})

	if err != nil {
		logger.Fatal(err)
	}
}
