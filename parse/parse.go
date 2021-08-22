package parse

import (
	"archive-ingest/util"
	"errors"
	"regexp"
	"strings"
)

type Entity struct {
	Filename, Author, Title, Publisher string
	Tags                               []string
}

var logger = util.NewLogger()

func ParseFilename(filename string) (*Entity, error) {
	re := regexp.MustCompile(`\[(.+)\]\s(.+)\s\((.+)\)(\s{(.+)})?\.zip`)
	matches := re.FindStringSubmatch(filename)

	if matches == nil || len(matches) < 3 {
		return nil, errors.New("filename did not match known format")
	}

	entity := Entity{
		Filename:  filename,
		Author:    matches[1],
		Title:     matches[2],
		Publisher: matches[3],
	}

	if len(matches) > 4 {
		entity.Tags = strings.Split(matches[5], " ")
	}

	logger.WithField("entity", entity).Info("parsed filename")

	return &entity, nil
}
