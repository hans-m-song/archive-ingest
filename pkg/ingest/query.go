package ingest

import (
	"archive-ingest/pkg/parse"
	"context"
	"errors"

	"github.com/jackc/pgx/v4"
	"github.com/sirupsen/logrus"
)

func createNameTable(name string) string {
	return createTableQuery(name, "name text UNIQUE NOT NULL")
}

func createIngestDbTables() []string {
	return []string{
		createNameTable("author"),
		createNameTable("tag"),
		createNameTable("publisher"),
		createNameTable("collection"),
		createTableQuery("entity",
			"filepath text NOT NULL",
			"filename text NOT NULL",
			"title text NOT NULL",
			"authors integer[] NOT NULL",
			"tags integer[] NOT NULL",
			"publisher_id integer",
			"collection_id integer",
			constraintUnique("filepath", "filename"),
			constraintFk("publisher"),
			constraintFk("collection"),
		),
	}
}

func insertIfNotExist(connection *pgx.Conn, name string, value string) (*int, error) {
	var id *int
	query := insertNameQuery(name, value)
	logrus.WithField("query", query).Debug("executing query")

	if err := connection.QueryRow(context.Background(), query).Scan(id); err != nil {
		return nil, err
	}

	if id == nil {
		return nil, errors.New("id not returned")
	}

	return id, nil
}

func insertMultiple(connection *pgx.Conn, name string, values []string) ([]int, error) {
	ids := make([]int, len(values))

	for i, value := range values {
		query := insertNameQuery(name, value)
		logrus.WithField("query", query).Debug("executing query")

		id, err := insertIfNotExist(connection, name, value)
		if err != nil {
			return ids, err
		}

		ids[i] = *id
	}

	return ids, nil
}

func insertEntity(
	connection *pgx.Conn,
	entity parse.Entity,
	publisherId, collectionId int,
	authorIds, tagIds []int,
) (*int, error) {
	query := insertEntityQuery(
		entity,
		InsertEntityParams{
			Authors: authorIds, Tags: tagIds,
			PublisherId:  publisherId,
			CollectionId: collectionId,
		},
	)

	var id *int
	if err := connection.QueryRow(context.Background(), query).Scan(id); err != nil {
		return nil, err
	}

	if id == nil {
		return nil, errors.New("id not returned")
	}

	return id, nil
}
