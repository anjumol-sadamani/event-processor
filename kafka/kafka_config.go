package kafka

import (
	"context"
	"eventprocessor/config"
	"eventprocessor/model"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
)

const (
	//placeholder {environment}
	ConsumerTopic      = "event.processor.consumer.%s"
	ConsumerRetryTopic = "event.processor.consumer.retry.%s"
	ConsumerDlq        = "event.processor.consumer.dlq.%s"
)

type KafkaConfig struct {
	Conn        *kafka.Conn   // Conn connects to the kafka brokers
	Reader      KafkaConsumer // Reader reading from the topic defined in the Environment variable.
	RetryWriter *kafka.Writer
	DlqWriter   *kafka.Writer
}

//Split the brokers string into slice.
//Example kafkaBrokerStr = "localhost:8080,localhost:8081"
// will return ["localhost:8080", "localhost:8081"]
func brokersSlice(kafkaBrokerStr string) []string {
	return strings.Split(kafkaBrokerStr, ",")
}

func CreateKafkaConnection() *kafka.Conn {
	config := config.GetConfig()
	kafkaBrokerStr := config.KafkaServerHost
	kafkaBrokers := strings.Split(kafkaBrokerStr, ",")

	var connLeader *kafka.Conn
	for _, broker := range kafkaBrokers {
		conn, err := kafka.Dial("tcp", broker)
		if err != nil {
			log.Fatalf("Error While Dialing to the kafka Broker: %s", err.Error())
		}

		controller, err := conn.Controller()
		if err != nil {
			log.Fatalf("error while creating the connection with the broker: %s", err.Error())
		}
		connLeader, err = kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
		if err != nil {
			log.Fatalf(err.Error())
		}
	}
	return connLeader
}

func CreateKafkaReader(topic string) *KafkaReader {
	config := config.GetConfig()

	kafkaBrokers := brokersSlice(config.KafkaServerHost)
	groupId := fmt.Sprintf("consumer-group-%s", topic)

	pr := &KafkaReader{
		Reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:  kafkaBrokers,
			GroupID:  groupId,
			Topic:    topic,
			MinBytes: 10e3, // 10KB
			MaxBytes: 10e6, // 10MB
			// In case the consumer restarted with a different
			//consumer group then it will not retry all the committed messages.
			StartOffset: kafka.FirstOffset,
		}),
		RetryTimeInterval: config.RetryTimeInterval,
	}

	return pr
}

func CreateKafkaConfig() *KafkaConfig {
	config := config.GetConfig()
	k := &KafkaConfig{
		Conn:        CreateKafkaConnection(),
		Reader:      CreateKafkaReader(config.KafkaTopic),
		RetryWriter: CreateWriter(config.KafkaRetryTopic),
		DlqWriter:   CreateWriter(config.KafkaDlqTopic),
	}

	return k
}

func CreateWriter(topic string) *kafka.Writer {
	kafkaBrokerStr := config.GetConfig().KafkaServerHost
	kafkaBrokers := strings.Split(kafkaBrokerStr, ",")
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: kafkaBrokers,
		Topic:   topic,
	})
	return w
}

func newTopic(topic string, numPartitions, replicationFactor int) kafka.TopicConfig {
	return kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     numPartitions,
		ReplicationFactor: replicationFactor,
	}
}

func CreateKafkaTopic(k *KafkaConfig) {
	// Creating the topic by using the topic constructor.
	config := config.GetConfig()

	topic := func(topic string) string {
		return fmt.Sprintf(topic, config.Env)
	}
	replicationFactor := config.ReplicationFactor

	topicConfigs := []kafka.TopicConfig{
		newTopic(topic(ConsumerTopic), 100, replicationFactor),
		newTopic(topic(ConsumerRetryTopic), 100, replicationFactor),
		newTopic(topic(ConsumerDlq), 100, replicationFactor),
	}
	// creates the topics
	if err := k.Conn.CreateTopics(topicConfigs...); err != nil {
		//the application is existed if there is returned in creating the topics
		log.Fatalf("Error while creating the topics %v", err)
	}
}

func (k *KafkaConfig) RetryEvent(ei *model.EventInfo, ctx context.Context, retryTopic string) error {
	ei.RetryCount = ei.RetryCount + 1
	ei.ProcessAfterTimeStamp = time.Now().Add(k.Reader.GetRetryTimeInterval())
	if err := k.WriteMessage(ctx, ei); err != nil {
		log.Errorf("Error while write messages to kafka %s", err.Error())
		return err
	}
	return nil
}

func (k *KafkaConfig) WriteMessage(ctx context.Context, ei *model.EventInfo) error {
	if err := k.RetryWriter.WriteMessages(ctx, kafka.Message{
		Key:   []byte(strconv.Itoa(rand.Int())),
		Value: []byte(ei.Data),
	}); err != nil {
		log.Errorf("RetryEvent, Not able to write the message to topic")
		return err
	}
	return nil
}
