package main

import (
	"archive-ingest/ingest"
	"archive-ingest/io"
	"archive-ingest/parse"
	"archive-ingest/util"
	"errors"
	"flag"
)

var logger = util.NewLogger()

type ArchiveIngestArgs struct {
	announcer               io.AnnouncerParameters
	Location, Queue, DbName string
}

func parseArgs() (*ArchiveIngestArgs, error) {
	queue := flag.String("queue", "", "rabbitmq queue")
	dbName := flag.String("dbName", "", "postgres db name")

	flag.Parse()

	if *dbName != "" && *queue != "" {
		return nil, errors.New("specify exactly one of queue or dbName")
	}

	if *dbName == "" && *queue == "" {
		return nil, errors.New("specify one of queue or dbName")
	}

	var location, user, pass, host, port *string

	if *dbName != "" {
		location = flag.String("location", ".", "ingest directory or url (url currently unsupported)")
		user = flag.String("user", "postgres", "postgres username")
		pass = flag.String("pass", "postgres", "postgres password")
		host = flag.String("host", "localhost", "postgres hostname")
		port = flag.String("port", "5432", "postgres port")
	} else if *queue != "" {
		location = flag.String("dir", ".", "ingest directory")
		user = flag.String("user", "guest", "rabbitmq username")
		pass = flag.String("pass", "guest", "rabbitmq password")
		host = flag.String("host", "localhost", "rabbitmq hostname")
		port = flag.String("port", "5672", "rabbitmq port")
	}

	flag.Parse()

	args := &ArchiveIngestArgs{
		announcer: io.AnnouncerParameters{
			User: *user,
			Pass: *pass,
			Host: *host,
			Port: *port,
		},
		Location: *location,
		Queue:    *queue,
		DbName:   *dbName,
	}

	return args, nil
}

func selectAnnouncer(args *ArchiveIngestArgs) (string, io.AnnouncerControls, error) {
	if args.DbName != "" && args.Queue != "" {
		return "", nil, errors.New("specify exactly one of queue or dbName")
	}

	if args.Queue != "" {
		return args.Queue, &io.RabbitAnnouncer{Params: args.announcer}, nil
	}

	if args.DbName != "" {
		return args.DbName, &io.DbAnnouncer{Params: args.announcer}, nil
	}

	return "", nil, errors.New("specify one of queue or dbName")
}

func main() {
	args, err := parseArgs()
	if err != nil {
		logger.Fatal(err)
	}

	name, announcer, err := selectAnnouncer(args)

	if err != nil {
		logger.Fatal(err)
	}

	err = announcer.Connect(name)
	if err != nil {
		logger.Fatal(err)
	}

	defer announcer.Close()

	logger.WithField("dir", args.Location).Debug("ingesting directory")

	err = ingest.Read(args.Location, func(entity *parse.Entity) {
		err = announcer.Say(entity)
		if err != nil {
			logger.Fatal(err)
		}
	})

	if err != nil {
		logger.Fatal(err)
	}
}
