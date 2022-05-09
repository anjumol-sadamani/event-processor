package service

import (
	"errors"
	"eventprocessor/model"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func Test_SaveSchema(t *testing.T) {

	type testData struct {
		name        string
		data 		string
		result 		interface{}
		mockClosure func(mock *MockSchemaRepo)
	}

	tests := []testData{
		{
			name:        "Success",
			data: `{ "Query":{ "deal":"Deal" }, "Deal":{ "title":"String", "price":"Float"}} `,
			mockClosure: func(mockRepo *MockSchemaRepo) {
				mockRepo.On("InsertSchema", 
				model.Schema{
					EventSchema: `{ "Query":{ "deal":"Deal" }, "Deal":{ "title":"String", "price":"Float"}} `,
					}).Return(nil).Once()

				mockRepo.On("InsertSchemaColumn", mock.Anything).Return(nil).Once()
			},
			result: nil,
		},
		{
			name:        "InValidJSON",
			data: ``,
			mockClosure: func(mockRepo *MockSchemaRepo) {
				mockRepo.On("InsertSchema", mock.Anything).Return(nil).Once()
				mockRepo.On("InsertSchemaColumn", mock.Anything).Return(nil).Once()
			},
			result: errors.New("Invalid JSON"),
		},
	}

	expectedSchemaList := []model.SchemaColumn{
				 	{	Query: "data.'query'.'deal'"	},
				 	{	Query: "data.'deal'.'title'"	},
				 	{	Query: "data.'deal'.'price'"	},
				}				

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			
			mockRepo := &MockSchemaRepo{}
			test.mockClosure(mockRepo)			

			ec := &SchemaHandlerServiceImpl{
				SchemaRepo: mockRepo,
			}

			res, er := ec.SaveSchema(test.data)

			assert.Equal(t, test.result, er)

			if test.name == "Success" {
				assert.True(t, reflect.DeepEqual(expectedSchemaList, res))
			}
		})
	}
}

type MockSchemaRepo struct {
	mock.Mock
}

func (sr *MockSchemaRepo) InsertSchema(schema model.Schema) error {
	args := sr.Called(schema)
	return args.Error(0)
}

func (sr *MockSchemaRepo) InsertSchemaColumn(schemaColumns []model.SchemaColumn) error {
	args := sr.Called(schemaColumns)
	return args.Error(0)
}