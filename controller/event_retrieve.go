package contoller

import (
	"eventprocessor/model"
	"eventprocessor/service"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

type EventRetrieveController struct {
	EventRetrieveService service.EventRetrieveService
}

func (ec *EventRetrieveController) GetEventsCount(c *gin.Context) {
	response := ec.EventRetrieveService.CountEvents()
	c.JSON(response.StatusCode, response.Data)
}

func (ec *EventRetrieveController) GetEventsCountByDay(c *gin.Context) {
	response := ec.EventRetrieveService.CountEventsByDay()
	c.JSON(response.StatusCode, response.Data)
}

func (ec *EventRetrieveController) GetEventsCountByClient(c *gin.Context) {

	groupByParams := c.Request.URL.Query().Get("group_by_tag")
	if groupByParams == "" {
		response := model.FailureResponse("Groupby params are mandatory", http.StatusBadRequest)
		c.JSON(response.StatusCode, response.Data)
		return
	}

	groupByTags := strings.Split(groupByParams, ",")

	response := ec.EventRetrieveService.CountEventsByClient(groupByTags)
	c.JSON(response.StatusCode, response.Data)
}
