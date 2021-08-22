package io

import (
	"archive-ingest/util"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/streadway/amqp"
)

var logger = util.NewLogger()

type AnnouncerParameters struct {
	User, Pass, Host, Port, Queue string
}

type Announcer struct {
	params     AnnouncerParameters
	connection *amqp.Connection
	channel    *amqp.Channel
	queue      *amqp.Queue
}

type AnnouncerControls interface {
	Connect()
	Say(data interface{}) error
	Close()
}

func (a *Announcer) Connect() error {
	url := fmt.Sprintf(
		"amqp://%s:%s@%s:%s",
		a.params.User,
		a.params.Pass,
		a.params.Host,
		a.params.Port,
	)

	logger.WithField("url", url).Debug("attempting to connect to rabbitmq")

	connection, err := amqp.Dial(url)
	if err != nil {
		return errors.New("failed to connect to rabbitmq")
	}

	a.connection = connection

	channel, err := a.connection.Channel()
	if err != nil {
		return errors.New("failed to open channel")
	}

	a.channel = channel

	q, err := a.channel.QueueDeclare(a.params.Queue, false, false, false, false, nil)

	if err != nil {
		return errors.New("failed to declare queue")
	}

	a.queue = &q

	return nil
}

func (a *Announcer) Say(data interface{}) error {
	serialised, err := json.Marshal(data)

	if err != nil {
		return err
	}

	logger.WithField("data", data).Info("announcing")

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

func (a *Announcer) Close() {
	a.channel.Close()
	a.connection.Close()
}

func New(params *AnnouncerParameters) *Announcer {
	announcer := Announcer{params: *params}

	return &announcer
}
