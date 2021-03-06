package util

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/sirupsen/logrus"
)

func CatchSignal(callback func()) {
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		signal := <-sigc
		logrus.WithField("signal", signal).Debug("caught signal")
		callback()
	}()
}
