package discover

import (
	"archive-ingest/pkg/parse"
	"io/fs"
	"os"
	"strings"

	"github.com/sirupsen/logrus"
)

func Read(rootDir string, callback func(*parse.Entity)) error {
	logrus.WithField("dir", rootDir).Info("reading dir")

	dir := os.DirFS(rootDir)

	return fs.WalkDir(dir, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() && strings.HasPrefix(path, ".") && path != "." {
			logrus.
				WithFields(logrus.Fields{"path": path, "dir": d.IsDir()}).
				Debug("skipping file")
			return fs.SkipDir
		}

		entity, err := parse.ParseFilename(path, d.Name())

		if err != nil {
			logrus.
				WithFields(logrus.Fields{"err": err, "file": d.Name()}).
				Warn("error parsing filename")
		}

		if entity == nil {
			return nil
		}

		callback(entity)

		return nil
	})
}
