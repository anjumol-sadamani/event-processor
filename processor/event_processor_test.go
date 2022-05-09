package processor

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"eventprocessor/config"
	kf "eventprocessor/kafka"
	"eventprocessor/model"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func Test_ProcessPushEvents(t *testing.T) {
	ctx := context.Background()
	event := sampleEvent()
	schema := sampleSchemaEvent()
	data := string(schema.Value)
	type testData struct {
		name        string
		kafkaMock   func(mock *MockKafkaConsumer)
		mockClosure func(mock *MockService)
		eventsCount int
	}

	tests := []testData{
		{
			name: "Process events",
			mockClosure: func(mock *MockService) {
				mock.AssertNumberOfCalls(t, "SaveSchema", 0)
			},
			kafkaMock: func(mock *MockKafkaConsumer) {
				mock.On("Read").Return(event, nil).Once()
				mock.On("CommitMessage", *event).Once().Return(nil)

			},
			eventsCount: 1,
		},
		{
			name: "Process schema",
			kafkaMock: func(mock *MockKafkaConsumer) {
				mock.On("Read").Return(schema, nil).Once()
				mock.On("CommitMessage", *schema).Once().Return(nil)
			},
			mockClosure: func(mockService *MockService) {
				mockService.On("SaveSchema", data).Return(([]model.SchemaColumn)(nil), nil).Once()
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mockKafkaConsumer := &MockKafkaConsumer{}
			defer mockKafkaConsumer.AssertExpectations(t)

			mockService := &MockService{}
			defer mockService.AssertExpectations(t)

			test.mockClosure(mockService)
			test.kafkaMock(mockKafkaConsumer)

			el := &EventListener{
				KafkaConfig: &kf.KafkaConfig{
					Reader: mockKafkaConsumer,
				},
				SchemaService: mockService,
				EventChannel:  make(chan model.EventInfo, 1),
				Ctx:           ctx,
			}
			el.processPushEvents()
			assert.Equal(t, test.eventsCount, len(el.EventChannel))
		})
	}

}

func Test_PersistEvents(t *testing.T) {
	ctx := context.Background()

	eventInfo := model.EventInfo{
		Data:      string(sampleEvent().Value),
		EventType: "Query",
	}

	type testData struct {
		name        string
		mockClosure func(mock *MockService)
		kafkaMock   func(mock *MockKafkaConfig)
		eventsCount int
		retryCount  int
	}

	tests := []testData{
		{
			name: "Save Event",
			mockClosure: func(mockService *MockService) {
				mockService.On("SaveEvent", mock.Anything).Return(nil).Once()
			},
			eventsCount: 0,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			mockService := &MockService{}
			defer mockService.AssertExpectations(t)

			test.mockClosure(mockService)
			el := &EventListener{
				EventService: mockService,
				EventChannel: make(chan model.EventInfo, 1),
				Ctx:          ctx,
				Wg:           new(sync.WaitGroup),
				AppConfig: &config.Config{
					Env: "Dev",
				},
			}

			el.Wg.Add(1)
			go el.PersistEvents()
			el.EventChannel <- eventInfo
			time.Sleep(3 * time.Second)

			assert.Equal(t, test.eventsCount, len(el.EventChannel))
		})
	}

}

type MockKafkaConsumer struct {
	mock.Mock
}

func (m *MockKafkaConsumer) Read(ctx context.Context) (*kafka.Message, error) {
	args := m.Called()
	return args.Get(0).(*kafka.Message), args.Error(1)
}

func (m *MockKafkaConsumer) CommitMessage(ctx context.Context, msg kafka.Message) error {
	args := m.Called(msg)
	return args.Error(0)
}

func (m *MockKafkaConsumer) GetRetryTimeInterval() time.Duration {
	return 1 * time.Minute
}

type MockService struct {
	mock.Mock
}

type MockKafkaConfig struct {
	mock.Mock
}

func (m *MockKafkaConfig) RetryEvent(ei *model.EventInfo, ctx context.Context, retryTopic string) error {
	args := m.Called(ei, retryTopic)
	return args.Error(0)
}

func (m *MockKafkaConfig) WriteMessage(ctx context.Context, ei *model.EventInfo) error {
	args := m.Called(ei)
	return args.Error(0)
}

func (m *MockService) SaveSchema(data string) ([]model.SchemaColumn, error) {
	args := m.Called(data)
	return args.Get(0).([]model.SchemaColumn), args.Error(1)
}

func (m *MockService) SaveEvent(data string) error {
	args := m.Called(data)
	return args.Error(0)
}

func sampleSchemaEvent() *kafka.Message {
	json, _ := json.Marshal(`{
		"Query":{
		"deal":"Deal",
		"user":"User"
		},
		"Deal":{
		"title":"String",
		"price":"Float",
		"user":"User"
		},
		"User":{
		"name":"String",
		"deals":"[Deal]"
		}
		}`)
	return &kafka.Message{Value: json,
		Headers: []kafka.Header{
			{
				Key:   "type",
				Value: []byte("Schema"),
			},
		}}
}

func sampleEvent() *kafka.Message {
	json, _ := json.Marshal(`{
	"deal":{
	"price":true
	},
	"user":{
	"id":true,
	"name":true
	}
	}`)
	return &kafka.Message{Value: json,
		Headers: []kafka.Header{
			{
				Key:   "type",
				Value: []byte("Query"),
			},
		}}
}
