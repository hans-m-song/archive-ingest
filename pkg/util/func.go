package util

import (
	"github.com/sirupsen/logrus"
)

func FatalOnErr(callback func() error, message string) {
	if err := callback(); err != nil {
		logrus.WithField("err", err).Fatal(message)
	}
}

func WarnOnErr(callback func() error, message string) {
	if err := callback(); err != nil {
		logrus.WithField("err", err).Warn(message)
	}
}
