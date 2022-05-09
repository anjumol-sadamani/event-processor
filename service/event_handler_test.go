package service

import (
	"errors"
	"eventprocessor/model"
	"testing"
	"time"

	"bou.ke/monkey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func Test_SaveEvent(t *testing.T) {

	type testData struct {
		name        string
		data 		string
		result 		interface{}
		mockClosure func(mock *MockRepo)
	}

	mockTime := time.Now()

	tests := []testData{
		{
			name:        "Success",
			data: `{"deal":{"price":true},"user":{"id":true,"name":true}}`,
			mockClosure: func(mock *MockRepo) {
				mock.On("InsertEvent", 
				model.Event{
					Client:        "client_id",
					ClientVersion: "v1",
					DataCenter:    "Google",
					ProcessedTime: mockTime,
					Data:         `{"deal":{"price":true},"user":{"id":true,"name":true}}`,
				}).Return(nil).Once()
			},
			result: nil,
		},
		{
			name:        "DB-Fail",
			data: `{"deal":{"price":true},"user":{"id":true,"name":true}}`,
			mockClosure: func(mock *MockRepo) {
				mock.On("InsertEvent", 
				model.Event{
					Client:        "client_id",
					ClientVersion: "v1",
					DataCenter:    "Google",
					ProcessedTime: mockTime,
					Data:         `{"deal":{"price":true},"user":{"id":true,"name":true}}`,
				}).Return(errors.New("")).Once()
			},
			result: errors.New("DB Save failed"),
		},
		{
			name:        "InValidJSON",
			data: ``,
			mockClosure: func(mockRepo *MockRepo) {
				mockRepo.On("InsertEvent", mock.Anything).Return(nil).Times(0)
			},
			result: errors.New("Invalid JSON"),
		},
	}

	patchTime := monkey.Patch(time.Now, func() time.Time { return mockTime })
	defer patchTime.Unpatch()

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			
			mockRepo := &MockRepo{}
			test.mockClosure(mockRepo)			

			ec := &EventHandlerServiceImpl{
				EventRepo: mockRepo,
			}

			er := ec.SaveEvent(test.data)

			assert.Equal(t, test.result, er)
		})
	}
}

type MockRepo struct {
	mock.Mock
}

func (m *MockRepo) InsertEvent(data model.Event) error {
	args := m.Called(data)
	return args.Error(0)
}

func (m *MockRepo) BulkInsertEvent(dataList []*model.Event) error {
	return nil
}

func (m *MockRepo) GetEventCount(paths []model.SchemaColumn) (map[string]interface{}, error) {
	return nil, nil
}

func (m *MockRepo) GetEventCountByDay(paths []model.SchemaColumn) ([]map[string]interface{}, error) {
	return nil, nil
}

func (m *MockRepo) CountEventsByClient(paths []model.SchemaColumn, groupBy []string) ([]map[string]interface{}, error) {
	return nil, nil
}