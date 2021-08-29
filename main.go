package main

import (
	"archive-ingest/pkg/broker"
	"archive-ingest/pkg/config"
	"archive-ingest/pkg/discover"
	"archive-ingest/pkg/parse"
	"errors"
	"fmt"
	"os"

	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
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
	ErrorDirRequired = errors.New(
		"not enough arguments, please provide a directory",
	)
)

func startDiscover() {
	if len(os.Args) < 3 {
		logrus.Fatal(ErrorDirRequired)
	}

	dir := os.Args[2]
	logrus.WithField("dir", dir).Info("beginning discovery of directory")

	announcer, err := broker.NewBroker()
	if err != nil {
		logrus.Fatal(err)
	}

	queue := viper.GetString(config.RabbitmqQueue)
	listener := func(entity *parse.Entity) {
		if entity == nil {
			return
		}

		message := broker.Message{Body: entity}
		if err := announcer.SendMessage(queue, message); err != nil {
			logrus.Warn(err)
		}
	}

	if err := discover.Read(dir, listener); err != nil {
		logrus.Fatal(err)
	}

	if err := announcer.Disconnect(); err != nil {
		logrus.Fatal(err)
	}
}

func startIngest() {
	logrus.Info("beginning ingest")

	// announcer, err := broker.NewBroker()
	// if err != nil {
	// 	logrusger.Fatal(err)
	// }
}

func main() {
	config.Setup()

	if len(os.Args) < 2 {
		logrus.Fatal(ErrorCmdRequired)
	}

	command := os.Args[1]

	switch command {
	case DiscoverCmd:
		startDiscover()
	case IngestCmd:
		startIngest()
	default:
		logrus.Fatal(ErrorInvalidCmd)
	}
}
