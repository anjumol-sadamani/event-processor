package server

import (
	controller "eventprocessor/controller"
	"eventprocessor/service"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

func InitEventProcessorRoutes(db *gorm.DB, route *gin.Engine) {

	eventController := &controller.EventRetrieveController{
		EventRetrieveService: service.NewServiceCreate(db),
	}
	health := new(controller.HealthController)
	groupRoute := route.Group("/event-processor/api/v1")
	groupRoute.GET("/", health.GetHealth)
	groupRoute.GET("/count", eventController.GetEventsCount)
	groupRoute.GET("/countByDay", eventController.GetEventsCountByDay)
	groupRoute.GET("/countByMetadata", eventController.GetEventsCountByMetadata)
}
