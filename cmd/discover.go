package cmd

import (
	"archive-ingest/pkg/broker"
	"archive-ingest/pkg/config"
	"archive-ingest/pkg/discover"
	"archive-ingest/pkg/parse"
	"archive-ingest/pkg/util"
	"errors"
	"os"

	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

var (
	ErrorDirRequired = errors.New(
		"not enough arguments, please provide a directory",
	)
)

func startAnnouncer() *broker.Broker {
	announcer, err := broker.NewBroker()
	if err != nil {
		logrus.WithField("err", err).Fatal("error creating broker")
	}

	return announcer
}

func StartDiscover() {
	if len(os.Args) < 3 {
		logrus.Fatal(ErrorDirRequired)
	}

	dir := os.Args[2]
	logrus.WithField("dir", dir).Info("beginning discovery of directory")

	announcer := startAnnouncer()

	cleanup := func() {
		util.FatalOnErr(announcer.Disconnect, "error disconnecting announcer")
	}

	util.CatchSignal(cleanup)
	defer cleanup()

	queue := viper.GetString(config.RabbitmqQueue)
	listener := func(entity *parse.Entity) {
		if entity == nil {
			return
		}

		message := broker.Message{Body: entity}
		if err := announcer.SendMessage(queue, message); err != nil {
			logrus.
				WithFields(logrus.Fields{"message": message, "err": err}).
				Warn("error sending message")
		}
	}

	if err := discover.Read(dir, listener); err != nil {
		logrus.
			WithFields(logrus.Fields{"err": err, "dir": dir}).
			Fatal("error walking directory")
	}
}
