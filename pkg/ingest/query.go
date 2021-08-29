package ingest

import (
	"archive-ingest/pkg/parse"
	"fmt"
	"strings"
)

type NameRow struct {
	Id   int    `json:"id"`
	Name string `json:"name"`
}

type EntityRow struct {
	Filepath    string `json:"filepath"`
	Title       string `json:"title"`
	AuthorId    int    `json:"author_id"`
	PublisherId int    `json:"publisher_id"`
	SourceId    int    `json:"source_id"`
	Tags        string `json:"tags"`
}

func createFk(tableName, columnName string) string {
	return fmt.Sprintf(`
		CONSTRAINT fk_%s
			FOREIGN KEY(%s)
				REFERENCES %s(%s)
	`, columnName, columnName, tableName, columnName)
}

func createTable(name string, columns ...string) string {
	header := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s", name)
	id := fmt.Sprintf("%s_id GENERATED ALWAYS AS IDENTITY", name)
	spec := strings.Join(columns, ", ")
	return fmt.Sprintf(`%s (%s, %s)`, header, id, spec)
}

func createNameTable(name string) string {
	return createTable(name, "name text")
}

func createIngestDbTables() []string {
	return []string{
		createNameTable("author"),
		createNameTable("tag"),
		createNameTable("publisher"),
		createNameTable("source"),
		createTable("entity",
			"filepath text",
			"title text",
			"author_id integer",
			"publisher_id integer",
			"source_id integer",
			"tags integer[]",
			createFk("author", "author_id"),
			createFk("publisher", "publisher_id"),
			createFk("source", "source_id"),
		),
	}
}

func createEntityInsert(entity parse.Entity) string {
	authorId, publisherId, sourceId := 1, 1, 1
	serialisedTags := fmt.Sprintf("{%s}", strings.Join(entity.Tags, ", "))
	return fmt.Sprintf(`
		INSERT INTO entity
		(filepath, title, author_id, publisher_id, source_id, tags)
		VALUES (%v, %v, %v, %v, %v, %v, )
		ON CONFLICT UPDATE
	`,
		entity.Filepath,
		entity.Title,
		authorId,
		publisherId,
		sourceId,
		serialisedTags,
	)
}
