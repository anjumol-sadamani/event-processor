package service

import (
	"encoding/json"
	"errors"
	"eventprocessor/model"
	repo "eventprocessor/repository"
	"time"

	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

type EventHandlerService interface {
	SaveEvent(data string) error
}

type EventHandlerServiceImpl struct {
	EventRepo repo.EventRepository
}

func EventHandlerServiceCreate(db *gorm.DB) *EventHandlerServiceImpl {
	return &EventHandlerServiceImpl{EventRepo: repo.CreateEventRepository(db)}
}

func (eh *EventHandlerServiceImpl) SaveEvent(data string) error {
	if !json.Valid([]byte(data)) {
		log.Errorf("Save event error - Invalid JSON : %s", data)
		return errors.New("Invalid JSON")
	}

	event := model.Event{
		Client:        "client_id",
		ClientVersion: "v1",
		DataCenter:    "Google",
		ProcessedTime: time.Now(),
		Data:          data,
	}
	if err := eh.EventRepo.InsertEvent(event); err != nil {
		log.Errorf("Save event error %v", err)
		return errors.New("DB Save failed")
	}
	log.Info("event saved successfully")
	return nil
}
