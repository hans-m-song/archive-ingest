package cmd

import (
	"archive-ingest/pkg/broker"
	"archive-ingest/pkg/config"
	"archive-ingest/pkg/ingest"
	"encoding/json"
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

func StartIngest() {
	logrus.Info("beginning ingest")

	listener, err := broker.NewBroker()
	if err != nil {
		logrus.Fatal(err)
	}

	defer listener.Disconnect()

	ingester, err := ingest.NewIngester()
	if err != nil {
		logrus.Fatal(err)
	}

	defer ingester.Disconnect()

	queue := viper.GetString(config.RabbitmqQueue)
	callback := func(message *broker.Message) {
		fmt.Println(message)
		content, _ := json.MarshalIndent(message.Body, "", "  ")
		fmt.Println(string(content))
	}
	if err := listener.Listen(queue, callback); err != nil {
		logrus.Fatal(err)
	}
}
