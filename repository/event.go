package repository

import (
	"eventprocessor/model"
	"fmt"
	"strings"

	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

type EventRepository interface {
	InsertEvent(data model.Event) error
	BulkInsertEvent(dataList []*model.Event) error
	GetEventCount(paths []model.SchemaColumn) (map[string]interface{}, error)
	GetEventCountByDay(paths []model.SchemaColumn) ([]map[string]interface{}, error)
	CountEventsByMetadata(paths []model.SchemaColumn, groupBy []string) ([]map[string]interface{}, error)
}

type EventRepositoryImpl struct {
	DB *gorm.DB
}

func CreateEventRepository(db *gorm.DB) *EventRepositoryImpl {
	return &EventRepositoryImpl{DB: db}
}

func (er *EventRepositoryImpl) InsertEvent(md model.Event) error {
	err := er.DB.Table("event").Create(&md).Error
	return err

}

// function to batch inset data, to make processing faster
func (er *EventRepositoryImpl) BulkInsertEvent(dataList []*model.Event) error {

	err := er.DB.Table("event").Create(&dataList).Error
	return err
}

func (er *EventRepositoryImpl) GetEventCount(paths []model.SchemaColumn) (map[string]interface{}, error) {
	countQrys := genrateCountAggregateQuery(paths)

	var results map[string]interface{}
	err := er.DB.Table("event").Select(strings.Join(countQrys, ",")).Find(&results).Error

	if err != nil {
		log.Errorf("Error occurred while fetch the event count from DB")
		return nil, err
	}

	return results, nil
}

func (er *EventRepositoryImpl) GetEventCountByDay(paths []model.SchemaColumn) ([]map[string]interface{}, error) {
	countQrys := genrateCountAggregateQuery(paths)

	var results []map[string]interface{}
	er.DB.Table("event").Select("processed_time::date," + strings.Join(countQrys, ",")).Group("processed_time::date").Find(&results)

	return results, nil
}

func (er *EventRepositoryImpl) CountEventsByMetadata(paths []model.SchemaColumn, groupBy []string) ([]map[string]interface{}, error) {
	countQrys := genrateCountAggregateQuery(paths)
	groupQry := strings.Join(groupBy, ",")
	var results []map[string]interface{}
	er.DB.Table("event").Select(groupQry + "," + strings.Join(countQrys, ",")).Group(groupQry).Find(&results)

	return results, nil
}

func genrateCountAggregateQuery(paths []model.SchemaColumn) []string {
	countQrys := make([]string, 0, len(paths))
	for _, v := range paths {
		q := strings.Replace(v.Query, ".query", "", 1)
		q = strings.ReplaceAll(q, ".", " -> ")
		q = fmt.Sprintf("COUNT(%s) as \"%s\"", q, v.Query)
		countQrys = append(countQrys, q)
	}
	return countQrys
}
