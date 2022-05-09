package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

type KafkaConsumer interface {
	Read(ctx context.Context) (*kafka.Message, error)
	CommitMessage(ctx context.Context, m kafka.Message) error
	GetRetryTimeInterval() time.Duration
}

// KafkaReader implements the KafkaConsumer interface.
type KafkaReader struct {
	Reader            *kafka.Reader
	RetryTimeInterval int
}

// Read reads the message from the Kafka Topic
func (r *KafkaReader) Read(ctx context.Context) (*kafka.Message, error) {
	m, err := r.Reader.FetchMessage(ctx)
	if err != nil {
		return nil, fmt.Errorf("Error while reading the message from topic %s, error: %s", m.Topic, err.Error())
	}

	return &m, err
}

// CommitMessage commits the message after consumed by the worker.
func (r *KafkaReader) CommitMessage(ctx context.Context, m kafka.Message) error {
	err := r.Reader.CommitMessages(ctx, m)
	if err != nil {
		return fmt.Errorf(
			"Error while committing the message in topic: %s, partition: %d, key: %s - %v",
			m.Topic,
			m.Partition,
			m.Key,
			err,
		)
	}

	return nil
}

// GetRetryTimeInterval is used for getting the time at the which the message
// is processed at consumer side.
func (r *KafkaReader) GetRetryTimeInterval() time.Duration {
	return time.Duration(r.RetryTimeInterval) * time.Minute
}
