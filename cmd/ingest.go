package cmd

import (
	"archive-ingest/pkg/broker"
	"archive-ingest/pkg/config"
	"archive-ingest/pkg/ingest"
	"archive-ingest/pkg/parse"
	"archive-ingest/pkg/util"

	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/streadway/amqp"
)

func startListener() *broker.Broker {
	listener, err := broker.NewBroker()
	if err != nil {
		logrus.WithField("err", err).Fatal("error creating broker")
	}

	return listener
}

func startIngester() *ingest.Ingester {
	ingester, err := ingest.NewIngester()
	if err != nil {
		logrus.WithField("err", err).Fatal("error creating ingester")
	}

	return ingester
}

func StartIngest() {
	logrus.Info("beginning ingest")

	listener := startListener()
	ingester := startIngester()

	cleaner := util.CreateCleaner(func() {
		logrus.Debug("cleaning up")
		util.FatalFunc(listener.Disconnect)
		util.FatalFunc(ingester.Disconnect)
	})

	defer cleaner()

	if err := ingester.Init(); err != nil {
		logrus.WithField("err", err).Fatal("error initialising ingester")
	}

	queue := viper.GetString(config.RabbitmqQueue)
	callback := func(message *broker.Message, delivery amqp.Delivery) {
		entity, err := parse.ParseObject(message.Body)
		if err != nil {
			logrus.WithField("body", message.Body).Warn("ignoring invalid message contents")
			return
		}

		if err := ingester.Digest(*entity); err != nil {
			logrus.WithField("err", err).Warn("error digesting message contents, requeued")
			return
		}
	}

	util.FatalFunc(func() error { return listener.Listen(queue, callback) })
}
