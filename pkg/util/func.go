package util

import "github.com/sirupsen/logrus"

func FatalFunc(callback func() error) {
	if err := callback(); err != nil {
		logrus.WithField("err", err).Fatal("callback resulted in error")
	}
}

func WarnFunc(callback func() error) {
	if err := callback(); err != nil {
		logrus.WithField("err", err).Warn("callback resulted in error")
	}
}
