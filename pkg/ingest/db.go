package ingest

import (
	"archive-ingest/pkg/config"
	"archive-ingest/pkg/parse"
	"archive-ingest/pkg/util"
	"context"
	"fmt"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type ConnectionParams struct {
	User, Pass, Host, Port, Name string
}

type Ingester struct {
	ready      bool
	connection *pgxpool.Pool
	batch      *pgx.Batch
}

func (i *Ingester) Connect(params ConnectionParams) error {
	if i.ready {
		logrus.Warn("attempting to connect when already connected")
		return nil
	}

	url, obscured := util.CreateConnectionUrl(util.UrlParams{
		Protocol: "postgres",
		User:     params.User,
		Pass:     params.Pass,
		Host:     params.Host,
		Port:     params.Port,
		Extra:    &params.Name,
	})

	logrus.WithField("url", obscured).Debug("attempting to connect to postgres")

	connection, err := pgxpool.Connect(context.Background(), url)
	if err != nil {
		logrus.WithField("err", err).Fatal("error connecting to database")
	}

	i.connection = connection

	logrus.WithField("database", params.Name).Info("connected to database")

	if i.batch == nil {
		i.batch = &pgx.Batch{}
	}

	i.ready = true
	return nil
}

func (i *Ingester) Flush() error {
	if i.batch == nil {
		i.batch = &pgx.Batch{}
		return nil
	}

	result := i.connection.SendBatch(context.Background(), i.batch)
	logrus.WithField("actions", i.batch.Len()).Debug("batch flushed")

	return result.Close()
}

func (i *Ingester) Init() error {
	if i.batch == nil {
		i.batch = &pgx.Batch{}
	}

	logrus.Info("initialising ingest tables")
	queries := createIngestDbTables()

	for _, query := range queries {
		if viper.GetViper().GetBool(config.DebugShowQueries) {
			fmt.Println(query)
		}

		i.batch.Queue(query)
	}

	return i.Flush()
}

func (i *Ingester) Digest(entity parse.Entity) error {
	logrus.WithField("entity", entity).Debug("digesting entity")

	dependencies, err := insertEntityDependencies(i.connection, &entity)
	if err != nil {
		return err
	}

	logrus.WithField("filename", entity.Filename).Debug("inserting entity")
	id, err := insertEntity(i.connection, entity, *dependencies)
	if err != nil && err != pgx.ErrNoRows {
		return err
	}

	if id != nil {
		logrus.WithField("id", id).Debug("inserted entity")
	}

	return nil
}

func (i *Ingester) Disconnect() error {
	if !i.ready {
		logrus.Warn("attempting to disconnect when already disconnected")
		return nil
	}

	logrus.Debug("disconnecting ingester")

	err := i.Flush()
	if err != nil {
		return err
	}

	i.connection.Close()

	i.ready = false
	return nil
}

func NewIngester() (*Ingester, error) {
	params := ConnectionParams{
		User: viper.GetString(config.PostgresUser),
		Pass: viper.GetString(config.PostgresPass),
		Host: viper.GetString(config.PostgresHost),
		Port: viper.GetString(config.PostgresPort),
		Name: viper.GetString(config.PostgresDatabase),
	}

	ingester := Ingester{batch: &pgx.Batch{}}
	if err := ingester.Connect(params); err != nil {
		return nil, err
	}

	return &ingester, nil
}
