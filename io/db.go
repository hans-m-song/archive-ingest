package io

import (
	"archive-ingest/util"
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v4"
)

type DbAnnouncer struct {
	Params     AnnouncerParameters
	connection *pgx.Conn
	batch      *pgx.Batch
}

func (a *DbAnnouncer) Connect(name string) error {
	url, obscured := util.CreateConnectionUrl(util.UrlParams{
		Protocol: "postgres",
		User:     a.Params.User,
		Pass:     a.Params.Pass,
		Host:     a.Params.Host,
		Port:     a.Params.Port,
	})

	dbName := "/" + name

	logger.WithField("url", obscured+dbName).Debug("attempting to connect to postgres")

	connection, err := pgx.Connect(context.Background(), url+dbName)
	if err != nil {
		logger.Fatal(err)
	}

	a.connection = connection

	logger.WithField("db", name).Debug("connected to db")

	return nil
}

type DbSayPayload struct{}

func (a *DbAnnouncer) Say(data interface{}) error {
	logger.WithField("data", data).Debug("announcing")

	payload, ok := data.(DbSayPayload)
	if !ok {
		return errors.New("invalid payload format, not a DbSayPayload")
	}

	logger.WithField("payload", payload).Debug("saving")

	return nil
}

func (a *DbAnnouncer) Flush() error {
	if a.batch != nil {
		logger.WithField("actions", a.batch.Len()).Debug("flushing batch")

		result := a.connection.SendBatch(context.Background(), a.batch)
		return result.Close()
	}

	return nil
}

func (a *DbAnnouncer) Close() error {
	logger.Debug("disconnecting announcer")

	err := a.Flush()
	if err != nil {
		return err
	}

	return a.connection.Close(context.Background())
}

func createNameTable(name string) string {
	return fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s 
		(
			%s_id GENERATED ALWAYS AS IDENTITY
			name  text
		)
	`, name, name)
}

func (a *DbAnnouncer) Init() error {
	a.batch.Queue(createNameTable("author"))
	a.batch.Queue(createNameTable("tag"))
	a.batch.Queue(createNameTable("publisher"))
	a.batch.Queue(createNameTable("source"))
	a.batch.Queue(`
		CREATE TABLE IF NOT EXISTS entity
		(
			id           GENERATED ALWAYS AS IDENTITY
			filepath     text
			title        text
			author_id    integer
			publisher_id integer
			source_id    integer
			tags         integer[]
			CONSTRAINT fk_author_id
				FOREIGN KEY(author_id)
					REFERENCES author(author_id)
			CONSTRAINT fk_publisher_id
				FOREIGN KEY(publisher_id)
					REFERENCES publisher(publisher_id)
			CONSTRAINT fk_source_id
				FOREIGN KEY(source_id)
					REFERENCES source(source_id)
		)
	`)

	return a.Flush()
}
