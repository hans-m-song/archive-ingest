package broker

import (
	"archive-ingest/pkg/config"
	"archive-ingest/pkg/util"
	"encoding/json"

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

type OnMessageCallback func(Message)

type BrokerControl interface {
	Connect(params util.UrlParams) error
	SendMessage(queue string, message Message) error
	Listen(queue string, callback OnMessageCallback) error
	Disconnect() error
}

func (broker *Broker) Connect(params util.UrlParams) error {
	if broker.ready {
		logrus.Warn("Broker attempted to connect when already connected")
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

	consumerTag, err := uuid.NewV4()
	if err != nil {
		return err
	}

	broker.consumers = append(broker.consumers, consumerTag.String())

	msgs, err := broker.channel.Consume(queue, consumerTag.String(), true, false, false, false, nil)
	if err != nil {
		return err
	}

	go func() {
		for msg := range msgs {
			logrus.WithField("msg", msg).Debug("received message")
		}
	}()

	// listen forever
	<-make(chan bool)

	return nil
}

func (broker *Broker) Disconnect() error {
	if !broker.ready {
		logrus.Warn("Broker attempted to disconnect when already disconnected")
	}

	var err error

	if err = broker.channel.Close(); err != nil {
		return err
	}

	if err := broker.connection.Close(); err != nil {
		return err
	}

	for _, consumer := range broker.consumers {
		if err = broker.channel.Cancel(consumer, false); err != nil {
			return err
		}
	}

	broker.ready = false

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

	// typecheck interface implementation
	var _ BrokerControl = (*Broker)(nil)

	return &broker, broker.Connect(params)
}
