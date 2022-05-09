package contoller

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

type HealthController struct {
}

func (controller *HealthController) GetHealth(c *gin.Context) {
	message := "Health is UP"
	c.String(http.StatusOK, message)
}
