package apimodels

import (
	"context"

	"github.com/julianedwards/cedar/logger"
	"github.com/julianedwards/cedar/options"
	"github.com/pkg/errors"
)

type CedarConfig struct {
	BaseURL    string `json:"base_url"`
	RPCPort    string `json:"rpc_port"`
	Username   string `json:"username"`
	APIKey     string `json:"api_key,omitempty"`
	AWSKey     string `json:"aws_key,omitempty"`
	AWSSecret  string `json:"aws_secret,omitempty"`
	LogsBucket string `json:"logs_bucket"`
	// TODO: add ability to select bucket type.
}

type CedarTestResultsTaskInfo struct {
	Failed bool `json:"failed"`
}

func (c CedarConfig) CreateBucketLogger(ctx context.Context) (logger.Logger, error) {
	bl, err := logger.NewBucketLogger(ctx, options.Bucket{
		Type: options.PailS3,
		Name: c.LogsBucket,
		S3: &options.S3Bucket{
			Key:    c.AWSKey,
			Secret: c.AWSSecret,
		},
	})

	return bl, errors.Wrap(err, "creating bucket logger")
}
