package main

import (
	"eventprocessor/server"
)

func main() {
	// r := gin.Default()

	// r.GET("/", func(c *gin.Context) {
	//   c.JSON(http.StatusOK, gin.H{"data": "hello world"})
	// })

	// r.Run()

	server.Start()
}
