package taskoutput

import (
	"context"

	"github.com/evergreen-ci/evergreen"
	"github.com/evergreen-ci/pail"
	"github.com/evergreen-ci/utility"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/mongo"
)

func newBucket(ctx context.Context, bucketName, bucketType string) (pail.Bucket, error) {
	switch bucketType {
	case evergreen.BucketTypeS3:
		return pail.NewS3Bucket(pail.S3Options{
			Name:        bucketName,
			Region:      evergreen.DefaultEC2Region,
			Permissions: pail.S3PermissionsPrivate,
			MaxRetries:  utility.ToIntPtr(10),
			Compress:    true,
		})
	case evergreen.BucketTypeGridFS:
		client, err := mongo.Connect(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "connecting to the GridFS DB")
		}

		return pail.NewGridFSBucketWithClient(ctx, client, pail.GridFSOptions{
			Name:     bucketName,
			Database: "taskoutput",
		})
	case evergreen.BucketTypeLocal:
		return pail.NewLocalBucket(pail.LocalOptions{Path: bucketName})
	default:
		return nil, errors.Errorf("unrecognized bucket type '%s'", bucketType)
	}
}
