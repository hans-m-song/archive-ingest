package ingest

import (
	"archive-ingest/pkg/config"
	"archive-ingest/pkg/parse"
	"archive-ingest/pkg/util"
	"context"
	"fmt"

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
		logrus.WithField("err", err).Fatal("error connecting to database")
	}

	i.connection = connection

	logrus.WithField("database", params.Name).Info("connected to database")

	if i.batch == nil {
		i.batch = &pgx.Batch{}
	}

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
	if i.batch == nil {
		i.batch = &pgx.Batch{}
	}

	logrus.Info("initialising ingest tables")
	queries := createIngestDbTables()

	for _, query := range queries {
		fmt.Println(query)
		i.batch.Queue(query)
	}

	return i.Flush()
}

func (i *Ingester) Digest(entity parse.Entity) error {
	logrus.WithField("entity", entity).Debug("digesting entity")

	logrus.WithField("authors", entity.Authors).Debug("inserting authors")
	authorIds, err := insertMultiple(i.connection, "author", entity.Authors)
	if err != nil {
		return err
	}

	logrus.WithField("tags", entity.Tags).Debug("inserting tags")
	tagIds, err := insertMultiple(i.connection, "tag", entity.Tags)
	if err != nil {
		return err
	}

	logrus.WithField("collection", entity.Collection).Debug("inserting collection")
	collectionId, err := insertIfNotExist(i.connection, "collection", entity.Collection)
	if err != nil {
		return err
	}

	logrus.WithField("publisher", entity.Publisher).Debug("inserting publisher")
	publisherId, err := insertIfNotExist(i.connection, "publisher", entity.Publisher)
	if err != nil {
		return err
	}

	logrus.WithField("filename", entity.Filename).Debug("inserting entity")
	entityId, err := insertEntity(
		i.connection,
		entity,
		*publisherId,
		*collectionId,
		authorIds,
		tagIds,
	)

	if err != nil {
		return err
	}

	logrus.WithFields(logrus.Fields{
		"authorIds":    authorIds,
		"tagIds":       tagIds,
		"collectionId": collectionId,
		"publisherId":  publisherId,
		"entityId":     entityId,
	}).Debug("inserted entity")

	return nil
}

func (i *Ingester) Disconnect() error {
	logrus.Debug("disconnecting ingester")

	err := i.Flush()
	if err != nil {
		return err
	}

	return i.connection.Close(context.Background())
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

	ingester := Ingester{batch: &pgx.Batch{}}
	if err := ingester.Connect(params); err != nil {
		return nil, err
	}

	return &ingester, nil
}
