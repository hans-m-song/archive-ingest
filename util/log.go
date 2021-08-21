package util

import (
	logrus "github.com/sirupsen/logrus"
)

func NewLogger() *logrus.Logger {
	var logger = logrus.New()

	logger.SetLevel(logrus.DebugLevel)

	return logger
}
