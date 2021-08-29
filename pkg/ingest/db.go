package ingest

import (
	"archive-ingest/pkg/config"
	"archive-ingest/pkg/parse"
	"archive-ingest/pkg/util"
	"context"

	"github.com/jackc/pgx/v4"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
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
	url, obscured := util.CreateConnectionUrl(util.UrlParams{
		Protocol: "postgres",
		User:     params.User,
		Pass:     params.Pass,
		Host:     params.Host,
		Port:     params.Port,
		Extra:    &params.Name,
	})

	logrus.WithField("url", obscured).Debug("attempting to connect to postgres")

	connection, err := pgx.Connect(context.Background(), url)
	if err != nil {
		logrus.Fatal(err)
	}

	i.connection = connection

	logrus.WithField("database", params.Name).Info("connected to database")

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

func (a *Ingester) Disconnect() error {
	logrus.Debug("disconnecting ingester")

	err := a.Flush()
	if err != nil {
		return err
	}

	return a.connection.Close(context.Background())
}

func NewIngester() (*Ingester, error) {
	// type check interface implementation
	var _ IngesterControl = (*Ingester)(nil)

	params := ConnectionParams{
		User: viper.GetString(config.PostgresUser),
		Pass: viper.GetString(config.PostgresPass),
		Host: viper.GetString(config.PostgresHost),
		Port: viper.GetString(config.PostgresPort),
		Name: viper.GetString(config.PostgresDatabase),
	}

	ingester := Ingester{}
	if err := ingester.Connect(params); err != nil {
		return nil, err
	}

	return &ingester, nil
}
