package service

import (
	"eventprocessor/model"
	repo "eventprocessor/repository"
	"net/http"
	"strings"

	"github.com/Jeffail/gabs"
	"gorm.io/gorm"
)

type EventRetrieveService interface {
	CountEvents() *model.APIResponse
	CountEventsByDay() *model.APIResponse
	CountEventsByMetadata(groupBy []string) *model.APIResponse
}

type EventRetrieveServiceImpl struct {
	EventRepo repo.EventRepository
}

func NewServiceCreate(db *gorm.DB) *EventRetrieveServiceImpl {
	return &EventRetrieveServiceImpl{EventRepo: repo.CreateEventRepository(db)}
}

func (e *EventRetrieveServiceImpl) CountEvents() *model.APIResponse {
	res, err := e.EventRepo.GetEventCount(SchemaList)

	if err != nil {
		return model.FailureResponse("Failed to get count", http.StatusInternalServerError)
	}

	if len(res) <= 0 {
		return model.FailureResponse("Data not available", http.StatusNotFound)
	}

	return model.SuccessResponse(generateJSONObject(res))
}

func (e *EventRetrieveServiceImpl) CountEventsByDay() *model.APIResponse {
	res, err := e.EventRepo.GetEventCountByDay(SchemaList)

	if err != nil {
		return model.FailureResponse("Failed to get count", http.StatusInternalServerError)
	}
	if len(res) <= 0 {
		return model.FailureResponse("Data not available", http.StatusNotFound)
	}

	return model.SuccessResponse(generateJSONArray(res))
}

func (e *EventRetrieveServiceImpl) CountEventsByMetadata(groupBy []string) *model.APIResponse {
	res, err := e.EventRepo.CountEventsByMetadata(SchemaList, groupBy)

	if err != nil {
		return model.FailureResponse("Failed to get count", http.StatusInternalServerError)
	}
	if len(res) <= 0 {
		return model.FailureResponse("Data not available", http.StatusNotFound)
	}

	return model.SuccessResponse(generateJSONArray(res))
}

func generateJSONArray(dataMapArray []map[string]interface{}) interface{} {
	jsonArray, _ := gabs.New().Array()
	for i := 0; i < len(dataMapArray); i++ {
		jsonArray.ArrayAppend(generateJSONObject(dataMapArray[i]))
	}

	return jsonArray.Data()
}

func generateJSONObject(dataMap map[string]interface{}) interface{} {
	jsonObj := gabs.New()
	for key, value := range dataMap {
		key = strings.ReplaceAll(key, "'", "")
		key = strings.ReplaceAll(key, "data.", "")
		jsonObj.SetP(value, key)
	}

	return jsonObj.Data()
}
