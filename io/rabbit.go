package io

import (
	"archive-ingest/util"
	"encoding/json"

	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

type RabbitAnnouncer struct {
	Params     AnnouncerParameters
	connection *amqp.Connection
	channel    *amqp.Channel
	queue      *amqp.Queue
}

func (a *RabbitAnnouncer) Connect(name string) error {
	url, obscured := util.CreateConnectionUrl(util.UrlParams{
		Protocol: "amqp",
		User:     a.Params.User,
		Pass:     a.Params.Pass,
		Host:     a.Params.Host,
		Port:     a.Params.Port,
	})

	logger.WithField("url", obscured).Debug("attempting to connect to rabbitmq")

	connection, err := amqp.Dial(url)
	if err != nil {
		return err
	}

	a.connection = connection

	channel, err := a.connection.Channel()
	if err != nil {
		return err
	}

	a.channel = channel

	q, err := a.channel.QueueDeclare(name, false, false, false, false, nil)

	if err != nil {
		return err
	}

	a.queue = &q

	logger.WithField("queue", a.queue.Name).Debug("connection successful")

	return nil
}

func (a *RabbitAnnouncer) Say(data interface{}) error {
	logger.WithFields(logrus.Fields{"data": data, "queue": a.queue.Name}).Info("announcing")
	serialised, err := json.Marshal(data)

	if err != nil {
		return err
	}

	return a.channel.Publish(
		"",
		a.queue.Name,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        serialised,
		},
	)
}

func (a *RabbitAnnouncer) Close() error {
	logger.Debug("disconnecting announcer")

	err := a.channel.Close()
	if err != nil {
		return err
	}

	return a.connection.Close()
}
