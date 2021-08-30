package ingest

import (
	"archive-ingest/pkg/config"
	"archive-ingest/pkg/parse"
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"golang.org/x/sync/errgroup"
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

func insertIfNotExist(connection *pgxpool.Pool, name string, value string) (*int, error) {
	id := -1
	query := insertNameQuery(name, value)

	if viper.GetViper().GetBool(config.DebugShowQueries) {
		fmt.Println(query)
	}

	row := connection.QueryRow(context.Background(), query)

	if err := row.Scan(&id); err != nil {
		return nil, err
	}

	if id < 0 {
		return nil, errors.New("id not returned")
	}

	logrus.WithFields(logrus.Fields{"table": name, "value": value}).Debug("inserted row")
	return &id, nil
}

func insertMultiple(connection *pgxpool.Pool, name string, values []string) ([]int, error) {
	ids := make([]int, len(values))

	for i, value := range values {
		id, err := insertIfNotExist(connection, name, value)
		if err != nil {
			return nil, err
		}

		ids[i] = *id
	}

	return ids, nil
}

func insertEntityDependencies(connection *pgxpool.Pool, entity *parse.Entity) (*InsertEntityParams, error) {
	params := InsertEntityParams{}

	eg, _ := errgroup.WithContext(context.Background())

	eg.Go(func() (err error) {
		authorIds, err := insertMultiple(connection, "author", entity.Authors)
		params.Authors = authorIds
		return
	})

	eg.Go(func() (err error) {
		tagIds, err := insertMultiple(connection, "tag", entity.Tags)
		params.Tags = tagIds
		return
	})

	eg.Go(func() (err error) {
		collectionId, err := insertIfNotExist(connection, "collection", entity.Collection)
		params.CollectionId = *collectionId
		return
	})

	eg.Go(func() (err error) {
		publisherId, err := insertIfNotExist(connection, "publisher", entity.Publisher)
		params.PublisherId = *publisherId
		return
	})

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	return &params, nil
}

func insertEntity(
	connection *pgxpool.Pool,
	entity parse.Entity,
	params InsertEntityParams,
) (*int, error) {
	query := insertEntityQuery(entity, params)

	if viper.GetViper().GetBool(config.DebugShowQueries) {
		fmt.Println(query)
	}

	var id int
	if err := connection.QueryRow(context.Background(), query).Scan(&id); err != nil {
		return nil, err
	}

	if id < 1 {
		return nil, errors.New("id not returned")
	}

	logrus.WithFields(logrus.Fields{"title": entity.Title}).Debug("inserted entity")
	return &id, nil
}
