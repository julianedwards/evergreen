package client

import (
	"context"
	"sync"
	"time"

	"github.com/evergreen-ci/evergreen/apimodels"
	"github.com/julianedwards/cedar/encode"
	"github.com/julianedwards/cedar/logger"
	"github.com/julianedwards/cedar/options"
	"github.com/mongodb/grip/level"
	"github.com/mongodb/grip/message"
	"github.com/mongodb/grip/send"
	"github.com/pkg/errors"
)

const (
	defaultMaxBufferSize int = 1e7
	defaultFlushInterval     = time.Minute
)

type bucketSender struct {
	logType string
	*bucketSenderBase
}

func newBucketSender(base *bucketSenderBase, logType string) *bucketSender {
	return &bucketSender{
		logType:          logType,
		bucketSenderBase: base,
	}
}

func (s *bucketSender) Send(m message.Composer) {
	s.send(s.logType, m)
}

type bucketSenderBase struct {
	mu         sync.Mutex
	ctx        context.Context
	cancel     context.CancelFunc
	buffer     []apimodels.LogMessage
	bufferSize int
	lastFlush  time.Time
	timer      *time.Timer
	closed     bool

	opts         bucketSenderOptions
	bucketLogger logger.Logger

	local send.Sender
	*send.Base
}

type bucketSenderOptions struct {
	key           string
	levelInfo     send.LevelInfo
	maxBufferSize int
	flushInterval time.Duration
}

func newBucketSenderBase(ctx context.Context, bucketLogger logger.Logger, opts bucketSenderOptions) (*bucketSenderBase, error) {
	s := &bucketSenderBase{
		opts:         opts,
		bucketLogger: bucketLogger,
		Base:         send.NewBase(opts.key),
	}

	s.local = send.MakeNative()
	s.local.SetName("local")
	if err := s.SetErrorHandler(send.ErrorHandlerFromSender(s.local)); err != nil {
		return nil, errors.Wrap(err, "setting default error handler")
	}

	if err := s.SetLevel(opts.levelInfo); err != nil {
		return nil, errors.Wrap(err, "setting level")
	}

	ctx, cancel := context.WithCancel(ctx)
	s.ctx = ctx
	s.cancel = cancel

	if s.opts.maxBufferSize <= 0 {
		s.opts.maxBufferSize = defaultMaxBufferSize
	}
	if s.opts.flushInterval > 0 {
		go s.timedFlush()
	}

	return s, nil
}

func (s *bucketSenderBase) send(logType string, m message.Composer) {
	if !s.Level().ShouldLog(m) {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		s.local.Send(message.NewErrorMessage(level.Error, errors.New("cannot call Send on a closed bucket logger Sender")))
		return
	}

	s.buffer = append(s.buffer, apimodels.LogMessage{
		Type:      logType,
		Timestamp: time.Now(),
		Severity:  m.Priority().String(),
		Message:   m.String(),
	})
	s.bufferSize += len(m.String())
	if s.bufferSize >= s.opts.maxBufferSize {
		if err := s.flush(s.ctx); err != nil {
			s.local.Send(message.NewErrorMessage(level.Error, err))
			return
		}
	}
}

// Flush flushes anything data that may be in the buffer to bucket storage.
func (s *bucketSenderBase) Flush(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	return s.flush(ctx)
}

// Close flushes anything that may be left in the underlying buffer and cleans
// up resources as necessary. Close is thread safe but should only be called
// once no more calls to Send are needed; after Close has been called any
// subsequent calls to Send will error. After the first call to Close
// subsequent calls will no-op.
func (s *bucketSenderBase) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	defer s.cancel()

	if s.closed {
		return nil
	}
	s.closed = true

	if len(s.buffer) > 0 {
		if err := s.flush(s.ctx); err != nil {
			s.local.Send(message.NewErrorMessage(level.Error, err))
			return errors.Wrap(err, "flushing buffer")
		}
	}

	return nil
}

func (s *bucketSenderBase) timedFlush() {
	s.mu.Lock()
	s.timer = time.NewTimer(s.opts.flushInterval)
	s.mu.Unlock()
	defer s.timer.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.timer.C:
			s.mu.Lock()
			if len(s.buffer) > 0 && time.Since(s.lastFlush) >= s.opts.flushInterval {
				if err := s.flush(s.ctx); err != nil {
					s.local.Send(message.NewErrorMessage(level.Error, err))
				}
			}
			_ = s.timer.Reset(s.opts.flushInterval)
			s.mu.Unlock()
		}
	}
}

func (s *bucketSenderBase) flush(ctx context.Context) error {
	err := s.bucketLogger.Write(s.ctx, options.Write{
		Key:      s.opts.key,
		Data:     s.buffer,
		Encoding: encode.JSON,
	})
	if err != nil {
		return err
	}

	s.buffer = nil
	s.bufferSize = 0
	s.lastFlush = time.Now()

	return nil
}
