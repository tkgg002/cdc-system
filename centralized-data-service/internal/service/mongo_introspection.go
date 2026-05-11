package service

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/bson"
)

type MongoIntrospectionService struct {
}

func NewMongoIntrospectionService() *MongoIntrospectionService {
	return &MongoIntrospectionService{}
}

func (s *MongoIntrospectionService) DiscoverDatabases(host, port string) ([]string, error) {
	uri := fmt.Sprintf("mongodb://%s:%s", host, port)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri).SetDirect(true))
	if err != nil {
		return nil, err
	}
	defer client.Disconnect(ctx)

	databases, err := client.ListDatabaseNames(ctx, bson.M{})
	if err != nil {
		return nil, err
	}

	return databases, nil
}

func (s *MongoIntrospectionService) DiscoverCollections(host, port, dbName string) ([]string, error) {
	uri := fmt.Sprintf("mongodb://%s:%s", host, port)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri).SetDirect(true))
	if err != nil {
		return nil, err
	}
	defer client.Disconnect(ctx)

	db := client.Database(dbName)
	collections, err := db.ListCollectionNames(ctx, bson.M{})
	if err != nil {
		return nil, err
	}

	return collections, nil
}
