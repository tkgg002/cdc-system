package service

import (
	"context"
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

func (s *MongoIntrospectionService) DiscoverDatabases(uri string) ([]string, error) {
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

func (s *MongoIntrospectionService) DiscoverCollections(uri, dbName string) ([]string, error) {
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

func (s *MongoIntrospectionService) IntrospectCollection(uri string, dbName, collectionName string, sampleSize int) (map[string]interface{}, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri).SetDirect(true))
	if err != nil {
		return nil, err
	}
	defer client.Disconnect(ctx)

	collection := client.Database(dbName).Collection(collectionName)

	// Sample documents
	cursor, err := collection.Find(ctx, bson.M{}, options.Find().SetLimit(int64(sampleSize)))
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	fieldMap := make(map[string]interface{})
	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			continue
		}
		for k, v := range doc {
			if k == "_id" {
				continue
			}
			// Simple type inference or just keep a sample value
			fieldMap[k] = v
		}
	}

	return fieldMap, nil
}
