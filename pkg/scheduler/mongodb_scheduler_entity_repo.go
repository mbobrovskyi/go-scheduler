package scheduler

import (
	"context"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

const DefaultMongoDBCollectionName = "schedulers"

type MongoDBSchedulerEntityRepoOptions struct {
	CollectionName string
}

type MongoDBSchedulerEntityRepo struct {
	db *mongo.Database

	collectionName string
}

func (s *MongoDBSchedulerEntityRepo) Init(ctx context.Context, name string) error {
	filter := bson.D{{"name", name}}
	update := bson.D{{"$setOnInsert", NewSchedulerEntity(name)}}
	opts := options.FindOneAndUpdate().SetUpsert(true)

	if err := s.db.Collection(s.collectionName).
		FindOneAndUpdate(ctx, filter, update, opts).Err(); err != nil {
		if err == mongo.ErrNoDocuments {
			return nil
		}
		return err
	}

	return nil
}

func (s *MongoDBSchedulerEntityRepo) GetAndSetLastRun(ctx context.Context, name string, lastRunTo time.Time) (*SchedulerEntity, error) {
	var schedulerEntity SchedulerEntity

	filter := bson.D{
		{"name", name},
		{"lastRun", bson.D{{
			"$lte", lastRunTo,
		}}},
	}
	update := bson.D{
		{"$set", bson.M{"lastRun": time.Now().UTC()}},
	}
	opts := options.FindOneAndUpdate().SetReturnDocument(options.After)

	if err := s.db.Collection(s.collectionName).
		FindOneAndUpdate(ctx, filter, update, opts).
		Decode(&schedulerEntity); err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, err
	}
	return &schedulerEntity, nil
}

func (s *MongoDBSchedulerEntityRepo) Save(ctx context.Context, schedulerEntity SchedulerEntity) error {
	filter := bson.D{{"name", schedulerEntity.Name}}
	update := bson.D{{"$set", schedulerEntity}}

	if _, err := s.db.Collection(s.collectionName).
		UpdateOne(ctx, filter, update); err != nil {
		return err
	}
	return nil
}

func NewMongoDBSchedulerEntityRepoImpl(
	db *mongo.Database,
	options *MongoDBSchedulerEntityRepoOptions,
) *MongoDBSchedulerEntityRepo {
	repo := &MongoDBSchedulerEntityRepo{
		db: db,
	}

	if options != nil && options.CollectionName != "" {
		repo.collectionName = options.CollectionName
	} else {
		repo.collectionName = DefaultMongoDBCollectionName
	}

	return repo
}
