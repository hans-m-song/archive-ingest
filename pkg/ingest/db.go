package ingest

import (
	"archive-ingest/pkg/parse"
	"archive-ingest/pkg/util"
	"context"

	"github.com/jackc/pgx/v4"
	"github.com/sirupsen/logrus"
)

type ConnectionParams struct {
	User, Pass, Host, Port, Name string
}

type Ingester struct {
	connection *pgx.Conn
	batch      *pgx.Batch
}

type IngesterControl interface {
	Connect(params ConnectionParams) error
	Flush() error
	Init() error
	Digest(entity parse.Entity) error
	Disconnect() error
}

func (i *Ingester) Connect(params ConnectionParams) error {
	extra := "/" + params.Name
	url, obscured := util.CreateConnectionUrl(util.UrlParams{
		Protocol: "postgres",
		User:     params.User,
		Pass:     params.Pass,
		Host:     params.Host,
		Port:     params.Port,
		Extra:    &extra,
	})

	logrus.WithField("url", obscured).Debug("attempting to connect to postgres")

	connection, err := pgx.Connect(context.Background(), url)
	if err != nil {
		logrus.Fatal(err)
	}

	i.connection = connection

	logrus.WithField("db", params.Name).Info("connected to db")

	return nil
}

func (i *Ingester) Flush() error {
	if i.batch != nil {
		logrus.WithField("actions", i.batch.Len()).Debug("flushing batch")

		result := i.connection.SendBatch(context.Background(), i.batch)
		return result.Close()
	}

	return nil
}

func (i *Ingester) Init() error {
	logrus.Info("initialising ingest tables")
	queries := createIngestDbTables()

	for _, query := range queries {
		i.batch.Queue(query)
	}

	return i.Flush()
}

func (i *Ingester) Digest(entity parse.Entity) error {
	logrus.WithField("entity", entity).Debug("digesting new entity")
	query := createEntityInsert(entity)
	logrus.Debug(query)

	return nil
}

func (a *Ingester) Close() error {
	logrus.Debug("disconnecting ingester")

	err := a.Flush()
	if err != nil {
		return err
	}

	return a.connection.Close(context.Background())
}
