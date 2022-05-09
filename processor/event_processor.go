package processor

import (
	"context"
	"errors"
	"eventprocessor/config"
	"eventprocessor/kafka"
	"eventprocessor/model"
	"eventprocessor/service"
	"fmt"
	"os"
	"sync"

	kf "github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
)

const (
	SCHEMA_TYPE     = "Schema"
	EVENT_TYPE      = "Query"
	MAX_RETRY_COUNT = 10
)

type EventListener struct {
	Ctx           context.Context
	AppConfig     *config.Config
	KafkaConfig   *kafka.KafkaConfig
	EventService  service.EventHandlerService
	SchemaService service.SchemaHandlerService
	EventChannel  chan model.EventInfo
	Wg            *sync.WaitGroup
	Stop          chan os.Signal
}

func (el *EventListener) ProcessEvents() {
	defer el.Wg.Done()
	for {
		el.processPushEvents()
	}
}

func (el *EventListener) processPushEvents() {
	log.Info("process events started")
	m, err := el.KafkaConfig.Reader.Read(el.Ctx)
	if err != nil {
		log.Errorf("Event has failed to get consumed: %v", err.Error())
		return
	}

	//committing the message on the kafka topic.
	defer func() {
		if err := el.KafkaConfig.Reader.CommitMessage(el.Ctx, *m); err != nil {
			log.Error(err)
		}
	}()

	//event is successfully retrieved.
	log.Infof("message at eventid/topic/partition/offset/time %s/%v/%v/%v/%s: %s\n", string(m.Key), m.Topic, m.Partition, m.Offset, m.Time.String(), string(m.Key))

	data := string(m.Value)
	eventType, err := getEventHeader(m.Headers)

	if err != nil {
		log.Errorf("Event type error %s", err.Error())
		return
	}

	eventInfo := &model.EventInfo{
		Data:      data,
		EventType: eventType,
	}

	switch eventInfo.EventType {
	case SCHEMA_TYPE:
		el.SchemaService.SaveSchema(data)
	case EVENT_TYPE:
		el.EventChannel <- *eventInfo
	default:
		log.Warnf("invalid Event type received %s", eventInfo.EventType)
	}

}

func (el *EventListener) PersistEvents() {
	defer el.Wg.Done()
	for {
		select {
		case eventInfo := <-el.EventChannel:
			if dbError := el.EventService.SaveEvent(eventInfo.Data); dbError != nil {
				el.handleRetry(eventInfo)
			}
		case sig := <-el.Stop:
			log.Errorf("Got %s signal. Aborting ..!", sig)
			return
		}
	}
}

func (el *EventListener) handleRetry(ei model.EventInfo) {
	if ei.RetryCount < MAX_RETRY_COUNT {
		eventRetryTopic := fmt.Sprintf(kafka.ConsumerRetryTopic, el.AppConfig.Env)
		el.KafkaConfig.RetryEvent(&ei, el.Ctx, eventRetryTopic)
	} else {
		// Events are pushed in the DLQ after all the retries
		el.KafkaConfig.WriteMessage(el.Ctx, &ei)
	}
}

func getEventHeader(headers []kf.Header) (string, error) {

	for _, header := range headers {
		if header.Key == "type" {
			return string(header.Value), nil
		}
	}
	return "", errors.New("Event type not found in the event")
}
