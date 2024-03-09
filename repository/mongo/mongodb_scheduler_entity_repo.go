package mongo

import (
	"context"
	"github.com/mbobrovskyi/goscheduler/entity"
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
	filter := bson.D{{Key: "name", Value: name}}
	update := bson.D{{Key: "$setOnInsert", Value: entity.NewSchedulerEntity(name)}}
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

func (s *MongoDBSchedulerEntityRepo) GetAndSetLastRun(ctx context.Context, name string, lastRunTo time.Time) (*entity.SchedulerEntity, error) {
	var schedulerEntity entity.SchedulerEntity

	filter := bson.D{
		{Key: "name", Value: name},
		{Key: "lastRun", Value: bson.D{{
			Key: "$lte", Value: lastRunTo,
		}}},
	}
	update := bson.D{
		{Key: "$set", Value: bson.M{"lastRun": time.Now().UTC()}},
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

func (s *MongoDBSchedulerEntityRepo) Save(ctx context.Context, schedulerEntity entity.SchedulerEntity) error {
	filter := bson.D{{Key: "name", Value: schedulerEntity.Name}}
	update := bson.D{{Key: "$set", Value: schedulerEntity}}

	if _, err := s.db.Collection(s.collectionName).
		UpdateOne(ctx, filter, update); err != nil {
		return err
	}
	return nil
}

func NewMongoDBSchedulerEntityRepo(db *mongo.Database) *MongoDBSchedulerEntityRepo {
	return NewMongoDBSchedulerEntityRepoWithOptions(db, MongoDBSchedulerEntityRepoOptions{})
}

func NewMongoDBSchedulerEntityRepoWithOptions(
	db *mongo.Database,
	options MongoDBSchedulerEntityRepoOptions,
) *MongoDBSchedulerEntityRepo {
	repo := &MongoDBSchedulerEntityRepo{
		db:             db,
		collectionName: DefaultMongoDBCollectionName,
	}

	if options.CollectionName != "" {
		repo.collectionName = options.CollectionName
	}

	return repo
}
