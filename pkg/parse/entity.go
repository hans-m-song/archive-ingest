package parse

import (
	"strings"

	"github.com/oriser/regroup"
	"github.com/sirupsen/logrus"
)

type RawEntity struct {
	RawAuthors string `regroup:"RawAuthors"`
	Title      string `regroup:"Title"`
	Publisher  string `regroup:"Publisher"`
	Source     string `regroup:"Source"`
	RawTags    string `regroup:"RawTags"`
}

type Entity struct {
	Filepath, Filename, Title, Publisher, Source string
	Authors                                      []string
	Tags                                         []string
}

const rawRe = `\[(?P<RawAuthors>.*)\]\s` +
	`(?P<Title>.*)\s` +
	`\((?P<Publisher>.*)\)` +
	`(\s⁅(?P<Source>.*)⁆)?` +
	`(\s{(?P<RawTags>.*)})?` +
	`\.zip`

var re = regroup.MustCompile(rawRe)

func ParseFilename(filepath, filename string) (*Entity, error) {
	rawEntity := RawEntity{}
	err := re.MatchToTarget(filename, &rawEntity)
	if err != nil {
		return nil, err
	}

	logrus.WithField("title", rawEntity.Title).Debug("successfully parsed entity")

	entity := Entity{
		Filepath:  filepath,
		Filename:  filename,
		Title:     rawEntity.Title,
		Publisher: rawEntity.Publisher,
		Source:    rawEntity.Source,
		Authors:   strings.Split(rawEntity.RawAuthors, ", "),
		Tags:      strings.Split(rawEntity.RawTags, " "),
	}

	return &entity, nil
}
