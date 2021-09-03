package main

import (
	"fmt"
	"os"

	"github.com/hans-m-song/archive-ingest/cmd"
	"github.com/hans-m-song/archive-ingest/pkg/config"

	"github.com/sirupsen/logrus"
)

const (
	DiscoverCmd = "discover"
	IngestCmd   = "ingest"
)

var (
	ErrorCmdRequired = fmt.Errorf(
		`not enough arguments, must be one of "%s" or "%s"`,
		DiscoverCmd,
		IngestCmd,
	)
	ErrorInvalidCmd = fmt.Errorf(
		`invalid command, must be one of "%s" or "%s"`,
		DiscoverCmd,
		IngestCmd,
	)
)

func main() {
	config.Setup()

	if len(os.Args) < 2 {
		logrus.Fatal(ErrorCmdRequired)
	}

	command := os.Args[1]

	switch command {

	case DiscoverCmd:
		cmd.StartDiscover()

	case IngestCmd:
		cmd.StartIngest()

	default:
		logrus.Fatal(ErrorInvalidCmd)

	}
}
