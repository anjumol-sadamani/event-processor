package service

import (
	"encoding/json"
	"errors"
	"eventprocessor/model"
	repo "eventprocessor/repository"
	"fmt"
	"reflect"
	"strings"

	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

var SchemaList []model.SchemaColumn

type SchemaHandlerService interface {
	SaveSchema(data string) ([]model.SchemaColumn, error)
}

type SchemaHandlerServiceImpl struct {
	SchemaRepo repo.SchemaRepository
}

func SchemaHandlerServiceCreate(db *gorm.DB) *SchemaHandlerServiceImpl {
	SchemaList = make([]model.SchemaColumn, 0)
	return &SchemaHandlerServiceImpl{SchemaRepo: repo.CreateSchemaRepository(db)}
}

func (sh *SchemaHandlerServiceImpl) SaveSchema(inputSchema string) ([]model.SchemaColumn, error) {

	var data interface{}
	err := json.Unmarshal([]byte(inputSchema), &data)
	if err != nil {
		log.Errorf("Schema event unmarshal error %s", err.Error())
		return nil, errors.New("Invalid JSON")
	}
	parseSchema(data, "data", &SchemaList)

	if err := sh.SchemaRepo.InsertSchema(model.Schema{EventSchema: inputSchema}); err != nil {
		log.Errorf("Save schema error %v", err)
		return nil, err
	}

	if err := sh.SchemaRepo.InsertSchemaColumn(SchemaList); err != nil {
		log.Errorf("Save schema column error %v", err)
		return nil, err
	}
	log.Info("schema saved successfully")
	return SchemaList, nil
}

// parse schema recursively to form build query for API
func parseSchema(schema interface{}, root string, schemaList *[]model.SchemaColumn) {
	for key, element := range schema.(map[string]interface{}) {
		newPath := root + "." + strings.ToLower("'"+key+"'")

		if reflect.TypeOf(element).Kind() == reflect.Map {
			parseSchema(element, newPath, schemaList)
		} else {
			// fmt.Println("Key:", key, "Element:", element, "Path:", newPath)
			*schemaList = append(*schemaList, model.SchemaColumn{Query: newPath})
		}
	}
}
