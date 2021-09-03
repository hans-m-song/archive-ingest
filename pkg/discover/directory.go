package discover

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/hans-m-song/archive-ingest/pkg/parse"

	"github.com/sirupsen/logrus"
)

func pathToFile(root, path, name string) string {
	fullPath := filepath.Join(root, path)
	pathTo := strings.Replace(fullPath, name, "", 1)
	return filepath.Clean(pathTo)
}

func Read(root string, callback func(*parse.Entity)) error {
	logrus.WithField("dir", root).Info("reading dir")

	dir := os.DirFS(root)

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

		entity, err := parse.ParseFilename(d.Name())

		if err != nil {
			logrus.
				WithFields(logrus.Fields{"err": err, "file": d.Name()}).
				Warn("error parsing filename")
			return nil
		}

		if entity != nil {
			entity.Filepath = pathToFile(root, path, d.Name())
			logrus.
				WithFields(logrus.Fields{"path": entity.Filepath, "name": entity.Title, "authors": entity.Authors}).
				Debug("parsed entity")
			callback(entity)
		}

		return nil
	})
}
