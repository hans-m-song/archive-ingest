package parse

import (
	"strings"

	"github.com/oriser/regroup"
)

const rawRe = `\[(?P<RawAuthors>.*)\]\s` +
	`(?P<Title>.*)\s` +
	`\((?P<Publisher>.*)\)` +
	`(\s⁅(?P<Collection>.*)⁆)?` +
	`(\s{(?P<RawTags>.*)})?`

var re = regroup.MustCompile(rawRe)

func ParseFilename(filename string) (*Entity, error) {
	rawEntity := RawEntity{}
	err := re.MatchToTarget(filename, &rawEntity)
	if err != nil {
		return nil, err
	}

	entity := Entity{
		Filename:   filename,
		Title:      rawEntity.Title,
		Publisher:  rawEntity.Publisher,
		Collection: rawEntity.Collection,
		Authors:    strings.Split(rawEntity.RawAuthors, ", "),
		Tags:       strings.Split(rawEntity.RawTags, " "),
	}

	return &entity, nil
}
