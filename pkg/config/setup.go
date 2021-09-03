package config

import (
	"encoding/json"
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type EnvSpec struct {
	Path, DefaultValue string
}

const (
	PostgresUser     = "POSTGRES_USER"
	PostgresPass     = "POSTGRES_PASS"
	PostgresHost     = "POSTGRES_HOST"
	PostgresPort     = "POSTGRES_PORT"
	PostgresDatabase = "POSTGRES_DATABASE"

	RabbitmqUser  = "RABBITMQ_USER"
	RabbitmqPass  = "RABBITMQ_PASS"
	RabbitmqHost  = "RABBITMQ_HOST"
	RabbitmqPort  = "RABBITMQ_PORT"
	RabbitmqQueue = "RABBITMQ_QUEUE"

	DebugLogLevel    = "DEBUG_LOG_LEVEL"
	DebugShowCaller  = "DEBUG_SHOW_CALLER"
	DebugShowQueries = "DEBUG_SHOW_QUERIES"
)

var (
	ErrorInvalidLogLevel = fmt.Errorf("invalid log level")
)

func setupEnv() {
	debug, _ := logrus.DebugLevel.MarshalText()

	defaults := []EnvSpec{
		{Path: PostgresUser, DefaultValue: "postgres"},
		{Path: PostgresPass, DefaultValue: "postgres"},
		{Path: PostgresHost, DefaultValue: "localhost"},
		{Path: PostgresPort, DefaultValue: "5432"},
		{Path: PostgresDatabase, DefaultValue: "postgres"},

		{Path: RabbitmqUser, DefaultValue: "guest"},
		{Path: RabbitmqPass, DefaultValue: "guest"},
		{Path: RabbitmqHost, DefaultValue: "localhost"},
		{Path: RabbitmqPort, DefaultValue: "5672"},
		{Path: RabbitmqQueue, DefaultValue: "queue"},

		{Path: DebugLogLevel, DefaultValue: string(debug)},
		{Path: DebugShowCaller, DefaultValue: "false"},
		{Path: DebugShowQueries, DefaultValue: "false"},
	}

	viper.AutomaticEnv()
	viper.SetConfigType("yaml")
	viper.SetConfigName("config")
	viper.AddConfigPath(".")

	for _, spec := range defaults {
		viper.SetDefault(spec.Path, spec.DefaultValue)
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
