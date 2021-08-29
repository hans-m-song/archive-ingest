package config

import (
	"encoding/json"
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type EnvSpec struct {
	path, defaultValue string
}

const (
	PostgresUser     = "postgres.user"
	PostgresPass     = "postgres.pass"
	PostgresHost     = "postgres.host"
	PostgresPort     = "postgres.port"
	PostgresDatabase = "postgres.database"

	RabbitmqUser  = "rabbitmq.user"
	RabbitmqPass  = "rabbitmq.pass"
	RabbitmqHost  = "rabbitmq.host"
	RabbitmqPort  = "rabbitmq.port"
	RabbitmqQueue = "rabbitmq.queue"

	DebugLogLevel   = "debug.loglevel"
	DebugShowCaller = "debug.showcaller"
)

var (
	ErrorInvalidLogLevel = fmt.Errorf("invalid log level")
)

func setupEnv() {
	debug, _ := logrus.DebugLevel.MarshalText()

	defaults := []EnvSpec{
		{path: PostgresUser, defaultValue: "postgres"},
		{path: PostgresPass, defaultValue: "postgres"},
		{path: PostgresHost, defaultValue: "localhost"},
		{path: PostgresPort, defaultValue: "5432"},
		{path: PostgresDatabase, defaultValue: "postgres"},

		{path: RabbitmqUser, defaultValue: "guest"},
		{path: RabbitmqPass, defaultValue: "guest"},
		{path: RabbitmqHost, defaultValue: "localhost"},
		{path: RabbitmqPort, defaultValue: "5672"},
		{path: RabbitmqQueue, defaultValue: "queue"},

		{path: DebugLogLevel, defaultValue: string(debug)},
		{path: DebugShowCaller, defaultValue: "true"},
	}

	viper.SetConfigType("yaml")
	viper.SetConfigName("config")
	viper.AddConfigPath(".")

	for _, spec := range defaults {
		viper.SetDefault(spec.path, spec.defaultValue)
	}

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			fmt.Println(err)
			panic(err)
		}
	}
}

func setupLogger() {
	rawLevel := viper.GetString(DebugLogLevel)
	level, err := logrus.ParseLevel(rawLevel)
	if err != nil {
		fmt.Println(ErrorInvalidLogLevel)
		logrus.WithField("level", rawLevel).Panic(ErrorInvalidLogLevel)
	}

	logrus.SetReportCaller(viper.GetBool(DebugShowCaller))
	logrus.SetLevel(level)
	logrus.WithField("level", rawLevel).Debug("log level set")
}

func Setup() {
	setupEnv()
	setupLogger()
}

func Expose() {
	data, _ := json.MarshalIndent(viper.AllSettings(), "", "  ")
	fmt.Println(string(data))
}
