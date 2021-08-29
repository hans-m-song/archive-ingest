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
	Filepath     string `json:"filepath"`
	Filename     string `json:"filename"`
	Title        string `json:"title"`
	AuthorId     int    `json:"author_id"`
	PublisherId  int    `json:"publisher_id"`
	CollectionId int    `json:"collection_id"`
	Tags         string `json:"tags"`
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
	id := fmt.Sprintf("%s_id integer PRIMARY KEY GENERATED ALWAYS AS IDENTITY", name)
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
		createNameTable("collection"),
		createTable("entity",
			"filepath text",
			"filename text",
			"title text",
			"author_id integer",
			"publisher_id integer",
			"collection_id integer",
			"tags integer[]",
			createFk("author", "author_id"),
			createFk("publisher", "publisher_id"),
			createFk("collection", "collection_id"),
		),
	}
}

func createNameInsert(name, value string) string {
	return fmt.Sprintf(`
		INSERT INTO %s
		(name) VALUES (%s)
		ON CONFLICT UPDATE
		RETURNING %s_id
	`, name, value, name)
}

func createEntityInsert(entity parse.Entity) string {
	authorId, publisherId, collectionId := 1, 1, 1
	serialisedTags := fmt.Sprintf("{%s}", strings.Join(entity.Tags, ", "))
	return fmt.Sprintf(`
		INSERT INTO entity
		(filepath, title, author_id, publisher_id, source_id, tags)
		VALUES (%v, %v, %v, %v, %v, %v, )
		ON CONFLICT UPDATE
		RETURNING entity_id
	`,
		entity.Filepath,
		entity.Title,
		authorId,
		publisherId,
		collectionId,
		serialisedTags,
	)
}
