package ingest

import (
	"archive-ingest/parse"
	"archive-ingest/util"
	"io/fs"
	"os"

	"github.com/sirupsen/logrus"
)

var logger = util.NewLogger()

func Read(rootDir string, callback func(parse.Entity)) error {
	logger.WithField("rootDir", rootDir).Info("Read dir")

	var dir = os.DirFS(rootDir)
	fs.WalkDir(dir, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			logger.Fatal(err)
		}

		// TODO call parse on entry
		logger.WithFields(logrus.Fields{"path": path, "dir": d}).Info("executing callback on entry")

		return nil
	})

	return nil
}
