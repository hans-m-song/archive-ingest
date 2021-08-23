package parse

import (
	"archive-ingest/util"
	"strings"

	"github.com/oriser/regroup"
)

type Entity struct {
	Filename  string
	Author    string `regroup:"Author"`
	Title     string `regroup:"Title"`
	Publisher string `regroup:"Publisher"`
	Source    string `regroup:"Source"`
	TagsRaw   string `regroup:"TagsRaw"`
	Tags      []string
}

var logger = util.NewLogger()

const rawRe = `\[(?P<Author>.*)\]\s` +
	`(?P<Title>.*)\s` +
	`\((?P<Publisher>.*)\)` +
	`(\s⁅(?P<Source>.*)⁆)?` +
	`(\s{(?P<TagsRaw>.*)})?` +
	`\.zip`

var re = regroup.MustCompile(rawRe)

func ParseFilename(filename string) (*Entity, error) {
	entity := &Entity{Filename: filename}

	err := re.MatchToTarget(filename, entity)
	if err != nil {
		return nil, err
	}

	entity.Filename = filename
	if entity.TagsRaw != "" {
		entity.Tags = strings.Split(entity.TagsRaw, " ")
	}

	logger.WithField("entity", entity).Debug("successfully parsed")

	return entity, nil
}
