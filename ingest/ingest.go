package ingest

import (
	"archive-ingest/parse"
	"archive-ingest/util"
	"io/fs"
	"os"
	"regexp"

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

		if d.IsDir() {
			return nil
		}

		valid, error := regexp.MatchString(`\.git`, d.Name())
		if !valid || error != nil {
			return nil
		}

		// TODO call parse on entry
		logger.WithFields(logrus.Fields{"path": path}).Info("executing callback on entry")

		return nil
	})

	return nil
}
