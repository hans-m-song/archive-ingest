package broker

import (
	"github.com/hans-m-song/archive-ingest/pkg/util"

	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

func connect(params util.UrlParams) (*amqp.Connection, *amqp.Channel, error) {
	url, obscured := util.CreateConnectionUrl(util.UrlParams{
		Protocol: params.Protocol,
		User:     params.User,
		Pass:     params.Pass,
		Host:     params.Host,
		Port:     params.Port,
	})

	logrus.WithField("url", obscured).Debug("attempting to connect to rabbitmq")

	connection, err := amqp.Dial(url)
	if err != nil {
		return nil, nil, err
	}

	channel, err := connection.Channel()
	if err != nil {
		return nil, nil, err
	}

	return connection, channel, nil
}

func assertQueue(channel *amqp.Channel, queue string) error {
	q, err := channel.QueueDeclare(queue, false, false, false, false, nil)
	if err != nil {
		return err
	}

	logrus.
		WithFields(logrus.Fields{"name": q.Name, "messages": q.Messages}).
		Debug("asserted queue")

	return nil
}
