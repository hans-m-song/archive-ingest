package parse

import (
	"archive-ingest/util"
	"errors"
	"regexp"
	"strings"
)

type EntityType = int

const (
	FileType EntityType = iota
	DirectoryType
)

type Entity struct {
	Filename, Author, Title, Publisher string
	Tags                               []string
}

var logger = util.NewLogger()

func ParseFilename(filename string) (*Entity, error) {
	re := regexp.MustCompile(`\[(?P<Author>.+)\]\s(?P<Title>.+)\s\((?P<Publisher>.+)\)(\s{(?P<Tags>.+)})?\.zip`)
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
