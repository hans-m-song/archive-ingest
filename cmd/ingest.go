package cmd

import (
	"github.com/hans-m-song/archive-ingest/pkg/broker"
	"github.com/hans-m-song/archive-ingest/pkg/config"
	"github.com/hans-m-song/archive-ingest/pkg/ingest"
	"github.com/hans-m-song/archive-ingest/pkg/parse"
	"github.com/hans-m-song/archive-ingest/pkg/util"

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

	cleanup := func() {
		util.WarnOnErr(listener.Disconnect, "error disconnecting listener")
		util.WarnOnErr(ingester.Disconnect, "error disconnecting ingester")
	}

	util.CatchSignal(cleanup)
	defer cleanup()

	if err := ingester.Init(); err != nil {
		logrus.WithField("err", err).Fatal("error initialising ingester")
	}

	queue := viper.GetString(config.RabbitmqQueue)
	callback := func(message *broker.Message, delivery amqp.Delivery) {
		resolve := func() error { return delivery.Ack(false) }
		reject := func() error { return delivery.Nack(false, true) }

		entity, err := parse.ParseObject(message.Body)

		if err != nil {
			logrus.WithField("body", message.Body).Warn("ignoring invalid message contents")
			util.FatalOnErr(reject, "error nacking message")
			return
		}

		if err := ingester.Digest(*entity); err != nil {
			logrus.WithField("err", err).Warn("error digesting message contents, requeued")
			util.FatalOnErr(reject, "error nacking message")
			return
		}

		util.FatalOnErr(resolve, "error acking message")
	}

	if err := listener.Listen(queue, callback); err != nil {
		logrus.WithField("err", err).Fatal("error listening to queue")
	}

}
