package announcer

import (
	"archive-ingest/util"
	"encoding/json"
	"errors"

	"github.com/streadway/amqp"
)

var logger = util.NewLogger()

type ConnectionState struct {
	Connection *amqp.Connection
	Channel    *amqp.Channel
	Queue      *amqp.Queue
	Close      func()
}

func Connect(url, queueName string) (*ConnectionState, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, errors.New("failed to connect to rabbitmq")
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, errors.New("failed to open channel")
	}

	q, err := ch.QueueDeclare(queueName, false, false, false, false, nil)

	if err != nil {
		return nil, errors.New("failed to declare queue")
	}

	state := &ConnectionState{
		Connection: conn,
		Channel:    ch,
		Queue:      &q,
		Close: func() {
			conn.Close()
			ch.Close()
		},
	}

	return state, nil
}

func Announce(state *ConnectionState, data interface{}) error {
	serialised, err := json.Marshal(data)

	if err != nil {
		return err
	}

	logger.WithField("data", data).Info("announcing")

	return state.Channel.Publish(
		"",
		state.Queue.Name,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        serialised,
		},
	)
}
