package apimodels

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/evergreen-ci/evergreen"
	"github.com/evergreen-ci/gimlet"
	"github.com/evergreen-ci/timber"
	"github.com/evergreen-ci/timber/buildlogger"
	"github.com/julianedwards/cedar/logger"
	"github.com/julianedwards/cedar/options"
	"github.com/mongodb/grip"
	"github.com/mongodb/grip/level"
	"github.com/mongodb/grip/message"
	"github.com/mongodb/grip/recovery"
	"github.com/pkg/errors"
)

// for the different types of remote logging
const (
	SystemLogPrefix  = "S"
	AgentLogPrefix   = "E"
	TaskLogPrefix    = "T"
	AllTaskLevelLogs = "ALL"

	LogErrorPrefix = "E"
	LogWarnPrefix  = "W"
	LogDebugPrefix = "D"
	LogInfoPrefix  = "I"
)

// Also used in the task_logg collection in the database.
// The LogMessage type is used by the models package and is stored in
// the database (inside in the model.TaskLog structure.)
type LogMessage struct {
	Type      string    `bson:"t" json:"t"`
	Severity  string    `bson:"s" json:"s"`
	Message   string    `bson:"m" json:"m"`
	Timestamp time.Time `bson:"ts" json:"ts"`
	Version   int       `bson:"v" json:"v"`
}

// TaskLog is a group of LogMessages, and mirrors the model.TaskLog
// type, sans the ObjectID field.
type TaskLog struct {
	TaskId       string       `json:"t_id"`
	Execution    int          `json:"e"`
	Timestamp    time.Time    `json:"ts"`
	MessageCount int          `json:"c"`
	Messages     []LogMessage `json:"m"`
}

func GetSeverityMapping(s int) string {
	switch {
	case s >= int(level.Error):
		return LogErrorPrefix
	case s >= int(level.Warning):
		return LogWarnPrefix
	case s >= int(level.Info):
		return LogInfoPrefix
	case s < int(level.Info):
		return LogDebugPrefix
	default:
		return LogInfoPrefix
	}
}

// GetBuildloggerLogsOptions represents the arguments passed into
// GetBuildloggerLogs function.
type GetBuildloggerLogsOptions struct {
	BaseURL       string `json:"_"`
	TaskID        string `json:"-"`
	TestName      string `json:"-"`
	GroupID       string `json:"-"`
	Execution     *int   `json:"-"`
	PrintPriority bool   `json:"-"`
	Tail          int    `json:"-"`
	LogType       string `json:"-"`
}

// GetBuildloggerLogs makes request to Cedar for a specifc log and returns an
// io.ReadCloser.
func GetBuildloggerLogs(ctx context.Context, opts GetBuildloggerLogsOptions) (io.ReadCloser, error) {
	usr := gimlet.GetUser(ctx)
	if usr == nil {
		return nil, errors.New("error getting user from context")
	}
	getOpts := buildlogger.GetOptions{
		Cedar: timber.GetOptions{
			BaseURL:  fmt.Sprintf("https://%s", opts.BaseURL),
			UserKey:  usr.GetAPIKey(),
			UserName: usr.Username(),
		},
		TaskID:        opts.TaskID,
		TestName:      opts.TestName,
		GroupID:       opts.GroupID,
		Execution:     opts.Execution,
		PrintTime:     true,
		PrintPriority: opts.PrintPriority,
		Tail:          opts.Tail,
	}

	switch opts.LogType {
	case TaskLogPrefix:
		getOpts.Tags = []string{evergreen.LogTypeTask}
	case SystemLogPrefix:
		getOpts.Tags = []string{evergreen.LogTypeSystem}
	case AgentLogPrefix:
		getOpts.Tags = []string{evergreen.LogTypeAgent}
	case AllTaskLevelLogs:
		getOpts.Tags = []string{
			evergreen.LogTypeTask,
			evergreen.LogTypeSystem,
			evergreen.LogTypeAgent,
		}
	}
	logReader, err := buildlogger.Get(ctx, getOpts)

	return logReader, errors.Wrapf(err, "failed to get logs for '%s' from buildlogger, using evergreen logger", opts.TaskID)
}

// ReadBuildloggerToChan parses Cedar buildlogger log lines by message and
// severity and reads into a channel.
func ReadBuildloggerToChan(ctx context.Context, taskID string, r io.ReadCloser, lines chan<- LogMessage) {
	var (
		line string
		err  error
	)

	defer func() {
		if err := recovery.HandlePanicWithError(recover(), nil, "read buildlogger to chan"); err != nil {
			grip.Error(message.WrapError(err, message.Fields{
				"task_id": taskID,
				"message": "reading buildlogger log lines to LogMessage channel",
			}))
		}
	}()

	defer close(lines)
	if r == nil {
		return
	}

	reader := bufio.NewReader(r)
	for err == nil {
		line, err = reader.ReadString('\n')
		if err != nil && err != io.EOF {
			grip.Warning(message.WrapError(err, message.Fields{
				"task_id": taskID,
				"message": "problem reading buildlogger log lines",
			}))
			return
		}

		severity := int(level.Info)
		if strings.HasPrefix(line, "[P: ") {
			severity, err = strconv.Atoi(strings.TrimSpace(line[3:6]))
			if err != nil {
				grip.Error(message.WrapError(err, message.Fields{
					"task_id": taskID,
					"message": "problem reading buildlogger log line severity",
				}))
				err = nil
			}
			line = line[8:]
		}

		select {
		case <-ctx.Done():
			grip.Error(message.WrapError(ctx.Err(), message.Fields{
				"task_id": taskID,
				"message": "context error while reading buildlogger log lines",
			}))
		case lines <- LogMessage{
			Message:  strings.TrimSuffix(line, "\n"),
			Severity: GetSeverityMapping(severity),
		}:
		}
	}
}

// ReadBuildloggerToSlice returns a slice of LogMessages from an io.ReadCloser.
func ReadBuildloggerToSlice(ctx context.Context, taskID string, r io.ReadCloser) []LogMessage {
	lines := []LogMessage{}
	lineChan := make(chan LogMessage, 1024)
	go ReadBuildloggerToChan(ctx, taskID, r, lineChan)

	for {
		line, more := <-lineChan
		if !more {
			break
		}

		lines = append(lines, line)
	}

	return lines
}

type LogMessageFilter func(LogMessage) bool

func FilterByLogType(logType string) LogMessageFilter {
	switch logType {
	case TaskLogPrefix:
		logType = evergreen.LogTypeTask
	case SystemLogPrefix:
		logType = evergreen.LogTypeSystem
	case AgentLogPrefix:
		logType = evergreen.LogTypeAgent
	case AllTaskLevelLogs, "":
		return func(_ LogMessage) bool {
			return true
		}
	}

	return func(log LogMessage) bool {
		if log.Type == logType {
			return true
		}

		return false
	}
}

type GetBucketLogsOptions struct {
	Key     string `json:"-"`
	Reverse bool   `json:"-"`
}

func GetBucketLogs(ctx context.Context, opts GetBucketLogsOptions) (logger.ReadCloser, error) {
	dbConf := evergreen.GetEnvironment().Settings().Cedar
	apiConf := CedarConfig{
		AWSKey:     dbConf.AWSKey,
		AWSSecret:  dbConf.AWSSecret,
		LogsBucket: dbConf.LogsBucket,
	}
	bucketLogger, err := apiConf.CreateBucketLogger(ctx)
	if err != nil {
		return nil, err
	}

	var r logger.ReadCloser
	readerOpts := options.Read{Key: opts.Key}
	if opts.Reverse {
		r, err = bucketLogger.NewReverseReadCloser(ctx, readerOpts)
	} else {
		r, err = bucketLogger.NewReadCloser(ctx, readerOpts)
	}

	return r, errors.Wrap(err, "getting bucket logger read closer")
}

type ReadBucketLogsOptions struct {
	TaskId     string
	ReadCloser logger.ReadCloser
	Filter     LogMessageFilter
	Limit      int
	Lines      chan LogMessage
}

func ReadBucketLogsToChan(ctx context.Context, opts ReadBucketLogsOptions) {
	defer func() {
		if err := recovery.HandlePanicWithError(recover(), nil, "read bucket logs to chan"); err != nil {
			grip.Error(message.WrapError(err, message.Fields{
				"task_id": opts.TaskId,
				"message": "reading bucket log lines to chan",
			}))
		}
	}()

	defer close(opts.Lines)
	if opts.ReadCloser == nil {
		return
	}

	var (
		lineCount int
		err       error
	)
	for err == nil {
		data, err := opts.ReadCloser.ReadPage()
		if err != nil && err != io.EOF {
			grip.Warning(message.WrapError(err, message.Fields{
				"task_id": opts.TaskId,
				"message": "reading bucket log lines",
			}))
			return
		}

		page := []LogMessage{}
		if err := json.Unmarshal(data, page); err != nil {
			grip.Warning(message.WrapError(err, message.Fields{
				"task_id": opts.TaskId,
				"message": "unmarshaling bucket log lines",
			}))
			return
		}

		for _, line := range page {
			if opts.Limit > 0 && lineCount > opts.Limit {
				return
			}
			if opts.Filter != nil && !opts.Filter(line) {
				continue
			}

			lineCount++

			select {
			case <-ctx.Done():
				grip.Error(message.WrapError(ctx.Err(), message.Fields{
					"task_id": opts.TaskId,
					"message": "context error while reading buildlogger log lines",
				}))
			case opts.Lines <- line:
			}
		}
	}
}

func ReadBucketLogsToSlice(ctx context.Context, opts ReadBucketLogsOptions) []LogMessage {
	lines := []LogMessage{}
	lineChan := opts.Lines
	if lineChan == nil {
		lineChan = make(chan LogMessage, 1024)
	}
	go ReadBucketLogsToChan(ctx, opts)

	for {
		line, more := <-lineChan
		if !more {
			break
		}

		lines = append(lines, line)
	}

	return lines
}
