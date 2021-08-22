package main

import (
	"archive-ingest/ingest"
	"archive-ingest/io"
	"archive-ingest/parse"
	"archive-ingest/util"
	"flag"
	"os"
)

var logger = util.NewLogger()

type ArchiveIngestArgs struct {
	announcer io.AnnouncerParameters
}

func parseArgs() *ArchiveIngestArgs {
	user := flag.String("user", "guest", "rabbitmq username")
	pass := flag.String("pass", "guest", "rabbitmq password")
	host := flag.String("host", "localhost", "rabbitmq hostname")
	port := flag.String("port", "5672", "rabbitmq port")
	queue := flag.String("queue", "queue", "rabbitmq queue")

	flag.Parse()

	return &ArchiveIngestArgs{
		announcer: io.AnnouncerParameters{
			User:  *user,
			Pass:  *pass,
			Host:  *host,
			Port:  *port,
			Queue: *queue,
		},
	}
}

func main() {
	args := parseArgs()

	announcer := io.New(&args.announcer)

	err := announcer.Connect()
	if err != nil {
		logger.Fatal(err)
	}

	defer announcer.Close()

	if len(os.Args) < 2 {
		logger.Fatal("Usage: ./archive-ingest /path/to/directory")
	}

	rootDir := os.Args[1]

	if len(rootDir) < 1 {
		rootDir = "."
	}

	logger.Debug("ingesting directory " + rootDir)

	err = ingest.Read(rootDir, func(entity *parse.Entity) {
		announcer.Say(entity)
	})

	if err != nil {
		logger.Fatal(err)
	}
}
