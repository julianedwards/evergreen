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

func (c CedarConfig) CreateBucketLogger(ctx context.Context, prefix string) (logger.Logger, error) {
	bl, err := logger.NewBucketLogger(ctx, options.Bucket{
		Type:   options.PailS3,
		Name:   c.LogsBucket,
		Prefix: prefix,
		S3: &options.S3Bucket{
			Key:    c.AWSKey,
			Secret: c.AWSSecret,
		},
	})

	return bl, errors.Wrap(err, "creating bucket logger")
}

type CedarTestResultsTaskInfo struct {
	Failed bool `json:"failed"`
}

type CedarTaskMetadata struct {
	Project   string   `json:"project"`
	Version   string   `json:"version"`
	Variant   string   `json:"variant"`
	TaskName  string   `json:"task_name"`
	TaskId    string   `json:"task_id"`
	Execution int      `json:"execution"`
	Mainline  bool     `json:"mainline"`
	Tags      []string `json:"tags"`
	Status    string   `json:"status"`
	Schema    int      `json:"schema"`
}
