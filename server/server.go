package server

import (
	"context"
	"eventprocessor/config"
	"eventprocessor/model"
	"eventprocessor/processor"
	"eventprocessor/service"
	"os"
	"os/signal"
	"syscall"

	"sync"

	log "github.com/sirupsen/logrus"

	"eventprocessor/data"
	kf "eventprocessor/kafka"

	"github.com/gin-gonic/gin"
)

func Start() {

	db, err := data.Connection()

	if err != nil {
		log.Fatalf("DB connection error..Exiting the process")
	}

	router := gin.Default()
	processorConfig := config.GetConfig()

	switch processorConfig.Env {
	case "dev":
		gin.SetMode(gin.DebugMode)
	case "test":
		gin.SetMode(gin.TestMode)
	default:
		gin.SetMode(gin.ReleaseMode)
	}

	InitEventProcessorRoutes(db, router)
	kafkaConfig := kf.CreateKafkaConfig()
	kf.CreateKafkaTopic(kafkaConfig)
	wg := new(sync.WaitGroup)
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	el := processor.EventListener{
		AppConfig:     processorConfig,
		KafkaConfig:   kafkaConfig,
		EventService:  service.EventHandlerServiceCreate(db),
		SchemaService: service.SchemaHandlerServiceCreate(db),
		EventChannel:  make(chan model.EventInfo, 1000),
		Ctx:           context.Background(),
		Wg:            wg,
		Stop:          stop,
	}

	wg.Add(3)
	go el.ProcessEvents()
	go el.PersistEvents()
	go devMessageProducer(context.Background())
	router.Run(":" + processorConfig.Port)
	wg.Wait()

	close(el.EventChannel)
	close(stop)
}
