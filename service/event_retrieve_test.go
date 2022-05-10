package service

import (
	"errors"
	"eventprocessor/model"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func Test_CountEvents(t *testing.T) {
	type testData struct {
		name        string
		result      interface{}
		mockClosure func(mock *MockRepo)
		statusCode  int
	}

	events := getSampleEvent()

	deal := make(map[string]interface{}, 0)
	deal["price"] = 30
	deal["title"] = 20

	query := make(map[string]interface{}, 0)
	query["deal"] = 10

	exp := make(map[string]interface{}, 0)
	exp["deal"] = deal
	exp["query"] = query

	tests := []testData{
		{
			name: "Success",
			mockClosure: func(mockRepo *MockRepo) {
				mockRepo.On("GetEventCount", mock.Anything).Return(events, nil).Once()
			},
			result:     model.SuccessResponse(exp),
			statusCode: http.StatusOK,
		},
		{
			name: "DB Error",
			mockClosure: func(mockRepo *MockRepo) {
				mockRepo.On("GetEventCount", mock.Anything).Return((map[string]interface{})(nil), errors.New("DB connection error")).Once()
			},
			result:     model.FailureResponse("Failed to get count", http.StatusInternalServerError),
			statusCode: http.StatusInternalServerError,
		},

		{
			name: "No events",
			mockClosure: func(mockRepo *MockRepo) {
				mockRepo.On("GetEventCount", mock.Anything).Return((map[string]interface{})(nil), nil).Once()
			},
			result:     model.FailureResponse("Data not available", http.StatusNotFound),
			statusCode: http.StatusNotFound,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			mockRepo := &MockRepo{}
			test.mockClosure(mockRepo)
			defer mockRepo.AssertExpectations(t)

			es := &EventRetrieveServiceImpl{
				EventRepo: mockRepo,
			}

			actualResponse := es.CountEvents()
			assert.Equal(t, test.statusCode, actualResponse.StatusCode)
			assert.Equal(t, test.result, actualResponse)

		})
	}

}

func Test_CountEventsByDay(t *testing.T) {
	type testData struct {
		name        string
		result      interface{}
		mockClosure func(mock *MockRepo)
		statusCode  int
	}

	list := getSampleEventByDay()
	deal := make(map[string]interface{}, 0)
	deal["price"] = 30
	deal["title"] = 20

	query := make(map[string]interface{}, 0)
	query["deal"] = 10

	exp := make(map[string]interface{}, 0)
	exp["deal"] = deal
	exp["query"] = query
	exp["processed_time"] = "2022-05-10T00:00:00Z"

	expList := make([]interface{}, 0)

	expList = append(expList, exp)

	tests := []testData{
		{
			name: "Success",
			mockClosure: func(mockRepo *MockRepo) {
				mockRepo.On("GetEventCountByDay", mock.Anything).Return(list, nil).Once()
			},
			result:     model.SuccessResponse(expList),
			statusCode: http.StatusOK,
		},
		{
			name: "DB Error",
			mockClosure: func(mockRepo *MockRepo) {
				mockRepo.On("GetEventCountByDay", mock.Anything).Return(([]map[string]interface{})(nil), errors.New("DB connection error")).Once()
			},
			result:     model.FailureResponse("Failed to get count", http.StatusInternalServerError),
			statusCode: http.StatusInternalServerError,
		},

		{
			name: "No events",
			mockClosure: func(mockRepo *MockRepo) {
				mockRepo.On("GetEventCountByDay", mock.Anything).Return(([]map[string]interface{})(nil), nil).Once()
			},
			result:     model.FailureResponse("Data not available", http.StatusNotFound),
			statusCode: http.StatusNotFound,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			mockRepo := &MockRepo{}
			test.mockClosure(mockRepo)
			defer mockRepo.AssertExpectations(t)

			es := &EventRetrieveServiceImpl{
				EventRepo: mockRepo,
			}

			actualResponse := es.CountEventsByDay()
			assert.Equal(t, test.statusCode, actualResponse.StatusCode)
			assert.Equal(t, test.result, actualResponse)
		})
	}

}

func Test_CountEventsByMetaData(t *testing.T) {
	type testData struct {
		name        string
		result      interface{}
		mockClosure func(mock *MockRepo)
		statusCode  int
	}
	list := getSampleEventByMetadata()
	deal := make(map[string]interface{}, 0)
	deal["price"] = 30
	deal["title"] = 20

	query := make(map[string]interface{}, 0)
	query["deal"] = 10

	exp := make(map[string]interface{}, 0)
	exp["deal"] = deal
	exp["query"] = query
	exp["client"] = "client_id"

	expList := make([]interface{}, 0)

	expList = append(expList, exp)
	tests := []testData{
		{
			name: "Success",
			mockClosure: func(mockRepo *MockRepo) {
				mockRepo.On("CountEventsByMetadata", mock.Anything, mock.Anything).Return(list, nil).Once()
			},
			result:     model.SuccessResponse(expList),
			statusCode: http.StatusOK,
		},
		{
			name: "DB Error",
			mockClosure: func(mockRepo *MockRepo) {
				mockRepo.On("CountEventsByMetadata", mock.Anything, mock.Anything).Return(([]map[string]interface{})(nil), errors.New("DB connection error")).Once()
			},
			result:     model.FailureResponse("Failed to get count", http.StatusInternalServerError),
			statusCode: http.StatusInternalServerError,
		},

		{
			name: "No events",
			mockClosure: func(mockRepo *MockRepo) {
				mockRepo.On("CountEventsByMetadata", mock.Anything, mock.Anything).Return(([]map[string]interface{})(nil), nil).Once()
			},
			result:     model.FailureResponse("Data not available", http.StatusNotFound),
			statusCode: http.StatusNotFound,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			mockRepo := &MockRepo{}
			test.mockClosure(mockRepo)
			defer mockRepo.AssertExpectations(t)

			es := &EventRetrieveServiceImpl{
				EventRepo: mockRepo,
			}

			actualResponse := es.CountEventsByMetadata([]string{"Client"})
			assert.Equal(t, test.statusCode, actualResponse.StatusCode)
			assert.Equal(t, test.result, actualResponse)

		})
	}

}

func getSampleEvent() map[string]interface{} {
	events := make(map[string]interface{}, 0)
	events["data.'query'.'deal'"] = 10
	events["data.'deal'.'title'"] = 20
	events["data.'deal'.'price'"] = 30
	return events
}

func getSampleEventByDay() []map[string]interface{} {
	events := make(map[string]interface{}, 0)
	events["data.'query'.'deal'"] = 10
	events["data.'deal'.'title'"] = 20
	events["data.'deal'.'price'"] = 30
	events["processed_time"] = "2022-05-10T00:00:00Z"
	list := make([]map[string]interface{}, 0)
	list = append(list, events)

	return list
}

func getSampleEventByMetadata() []map[string]interface{} {
	events := make(map[string]interface{}, 0)
	events["data.'query'.'deal'"] = 10
	events["data.'deal'.'title'"] = 20
	events["data.'deal'.'price'"] = 30
	events["client"] = "client_id"
	list := make([]map[string]interface{}, 0)
	list = append(list, events)

	return list
}
