package ingest

import (
	"archive-ingest/pkg/parse"
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"text/template"

	"github.com/sirupsen/logrus"
)

func createTemplate(name, text string) *template.Template {
	template, err := template.New(name).Parse(text)

	if err != nil {
		logrus.WithFields(logrus.Fields{"name": name}).Fatal("error creating template")
	}

	return template
}

var (
	templateInsertName = createTemplate("InsertName", `{{"" -}}
WITH tmp AS (
	INSERT INTO {{.Table}}(name)
	VALUES('{{.Value}}')
	ON CONFLICT ON CONSTRAINT {{.Table}}_name_key DO NOTHING
	RETURNING {{.Table}}_id
)
SELECT {{.Table}}_id FROM tmp
UNION ALL
SELECT {{.Table}}_id FROM {{.Table}}
WHERE name = {{.Value}}`,
	)

	templateCreateTable = createTemplate("CreateTable", `{{"" -}}
CREATE TABLE IF NOT EXISTS {{.Table}} (
	{{.Table}}_id integer PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
	{{- range $i, $element := .Columns}}
		{{- if $i}}, {{end}}
		{{$element}}{{end}}
)`,
	)

	templateInsertEntity = createTemplate("InsertEntity", `{{"" -}}
INSERT INTO entity 
(filepath, filename, title, authors, tags, publisher_id, collection_id)
VALUES (
	'{{.Filepath}}',
	'{{.Filename}}',
	'{{.Title}}',
	'{{.Authors}}',
	'{{.Tags}}',
	{{.PublisherId}},
	{{.CollectionId}},
)
ON CONFLICT (filepath, filename) DO NOTHING
RETURNING entity_id`,
	)
)

func templateToString(template *template.Template, data interface{}) string {
	buf := &bytes.Buffer{}

	if err := templateCreateTable.Execute(buf, data); err != nil {
		logrus.WithField("err", err).Fatal("error executing template")
	}

	return buf.String()
}

func createTableQuery(name string, columns ...string) string {
	data := map[string]interface{}{"Table": name, "Columns": columns}
	return templateToString(templateCreateTable, data)
}

func insertNameQuery(table, value string) string {
	data := map[string]interface{}{"Table": table, "Value": value}
	return templateToString(templateInsertName, data)
}

func constraintUnique(columns ...string) string {
	return fmt.Sprintf(
		"UNIQUE (%s)",
		strings.Join(columns, ", "),
	)
}

func constraintFk(name string) string {
	return fmt.Sprintf(
		`CONSTRAINT fk_%s_id FOREIGN KEY(%s_id) REFERENCES %s(%s_id)`,
		name, name, name, name,
	)
}

func arrayValue(values []int) string {
	builder := strings.Builder{}
	for i, value := range values {
		if i > 0 {
			builder.WriteString(",")
		}
		builder.WriteString(strconv.Itoa(value))
	}

	return fmt.Sprintf(`{%s}`, builder.String())
}

type InsertEntityParams struct {
	Authors, Tags             []int
	PublisherId, CollectionId int
}

func insertEntityQuery(entity parse.Entity, params InsertEntityParams) string {
	data := map[string]interface{}{
		"Filepath":     entity.Filepath,
		"Filename":     entity.Filename,
		"Title":        entity.Title,
		"AuthorId":     arrayValue(params.Authors),
		"Tags":         arrayValue(params.Tags),
		"PublisherId":  params.PublisherId,
		"CollectionId": params.CollectionId,
	}
	return templateToString(templateInsertEntity, data)
}
