package ingest

import (
	"archive-ingest/parse"
	"archive-ingest/util"
	"io/fs"
	"os"
	"strings"

	"github.com/sirupsen/logrus"
)

var logger = util.NewLogger()

func Read(rootDir string, callback func(*parse.Entity)) error {
	logger.WithField("dir", rootDir).Info("reading dir")

	dir := os.DirFS(rootDir)

	return fs.WalkDir(dir, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() || strings.HasPrefix(path, ".") {
			return nil
		}

		logger.WithFields(logrus.Fields{"path": path, "dir": d.IsDir()}).Debug("checking path")

		entity, _ := parse.ParseFilename(d.Name())

		if entity == nil {
			return nil
		}

		callback(entity)

		return nil
	})
}
