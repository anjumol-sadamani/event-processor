package server

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

func devMessageProducer(ctx context.Context) {
	fmt.Println("Running message producer")
	messageKey := 0

	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"localhost:29092"},
		Topic:   "event.processor.consumer.DEV",
	})

	// send schema as first event
	err := w.WriteMessages(ctx, kafka.Message{
			Key: []byte(strconv.Itoa(messageKey)),
			Value: []byte(`{
				"Query":{
				"deal":"Deal",
				"user":"User"
				},
				"Deal":{
				"title":"String",
				"price":"Float",
				"user":"User"
				},
				"User":{
				"name":"String",
				"deals":"[Deal]"
				}
				}
				`),
			Headers: []kafka.Header{
				{
					Key:   "type",
					Value: []byte("Schema"),
				},
			},
		})

		checkError(err)
		messageKey++;

		// send random query as events
	for {
		err := w.WriteMessages(ctx, kafka.Message{
			Key: []byte(strconv.Itoa(messageKey)),
			Value: getMessage(),
			Headers: []kafka.Header{
				{
					Key:   "type",
					Value: []byte("Query"),
				},
			},
		})
		checkError(err)
		messageKey++

		time.Sleep(time.Second)
	}
}

func getMessage() []byte {
	messageList := [][]byte{
		[]byte(`{
				"deal":{
				"price":true
				},
				"user":{
				"id":true,
				"name":true
				}
				}`),
		[]byte(`
				{
				"deal":{
					"title":true,
				"price":true,
				"user":{
				"name":true
				}
				}
				}`),
		[]byte(`
				{
				"deal":{
					"title":true,
				"user":{
				"name":true
				}
				}
				}`),
		[]byte(`
				{
				"deal":{
					"title":true,
					"price":true,
				"user":{
				"id":true,
				"name":true
				}
				}
				}`),
	}

	return messageList[rand.Intn(len(messageList)-1)]
}

func checkError(err error) {
	if err != nil {
			panic("could not write message " + err.Error())
		}
}