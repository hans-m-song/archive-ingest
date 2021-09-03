package broker

import (
	"encoding/json"

	"github.com/hans-m-song/archive-ingest/pkg/config"
	"github.com/hans-m-song/archive-ingest/pkg/util"

	"github.com/gofrs/uuid"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/streadway/amqp"
)

type Message struct {
	Body interface{} `json:"body"`
}

type Broker struct {
	ready      bool
	params     util.UrlParams
	connection *amqp.Connection
	channel    *amqp.Channel
	consumers  []string
}

type OnMessageCallback func(*Message, amqp.Delivery)

func (broker *Broker) Connect(params util.UrlParams) error {
	if broker.ready {
		logrus.Warn("broker attempted to connect when already connected")
		return nil
	}

	connection, channel, err := connect(params)
	if err != nil {
		return err
	}

	broker.params = params
	broker.connection = connection
	broker.channel = channel
	broker.ready = true

	logrus.Info("broker connected")

	return nil
}

func (broker *Broker) SendMessage(queue string, message Message) error {
	if err := assertQueue(broker.channel, queue); err != nil {
		return err
	}

	serialised, err := json.Marshal(message)

	if err != nil {
		return err
	}

	logrus.
		WithFields(logrus.Fields{"queue": queue, "size": len(string(serialised))}).
		Debug("sending message")

	return broker.channel.Publish("", queue, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        serialised,
	})
}

func (broker *Broker) Listen(queue string, callback OnMessageCallback) error {
	if err := assertQueue(broker.channel, queue); err != nil {
		return err
	}

	tag, err := uuid.NewV4()
	if err != nil {
		return err
	}

	broker.consumers = append(broker.consumers, tag.String())

	msgs, err := broker.channel.Consume(queue, tag.String(), false, false, false, false, nil)
	if err != nil {
		return err
	}

	logrus.
		WithFields(logrus.Fields{"consumer": tag.String(), "queue": queue}).
		Info("broker consuming from queue")

	for delivery := range msgs {
		logrus.Debug("begin for loop body")
		if len(delivery.Body) < 1 {
			continue
		}

		logrus.
			WithFields(logrus.Fields{
				"size":    len(delivery.Body),
				"pending": delivery.MessageCount,
			}).
			Debug("received message")

		message := &Message{}
		if err := json.Unmarshal(delivery.Body, message); err != nil {
			logrus.WithField("err", err).Warn("error parsing delivery body")
			continue
		}

		callback(message, delivery)
	}

	return nil
}

func (broker *Broker) Disconnect() error {
	if !broker.ready {
		logrus.Warn("broker attempted to disconnect when already disconnected")
	}

	for _, consumer := range broker.consumers {
		if err := broker.channel.Cancel(consumer, false); err != nil {
			return err
		}
	}

	if err := broker.channel.Close(); err != nil {
		return err
	}

	if err := broker.connection.Close(); err != nil {
		return err
	}

	broker.ready = false

	logrus.Info("broker disconnected")

	return nil
}

func NewBroker() (*Broker, error) {
	params := util.UrlParams{
		Protocol: "amqp",
		User:     viper.GetString(config.RabbitmqUser),
		Pass:     viper.GetString(config.RabbitmqPass),
		Host:     viper.GetString(config.RabbitmqHost),
		Port:     viper.GetString(config.RabbitmqPort),
	}

	broker := Broker{}

	return &broker, broker.Connect(params)
}
