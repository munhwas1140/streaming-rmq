package main

import (
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Event struct {
	Name string
}

func main() {
	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s/%s", "guest", "guest", "localhost:5672", ""))
	if err != nil {
		log.Fatal(err)
	}

	ch, err := conn.Channel()
	if err != nil {
		log.Fatal(err)
	}

	if err := ch.Qos(50, 0, false); err != nil {
		log.Fatal(err)
	}

	// Auto ACk has to be FALSE
	// It will not work with streams
	stream, err := ch.Consume("events", "event_consumer", false, false, false, false, amqp.Table{
		"x-stream-offset": "10m", // STARTING POINT
	})
	if err != nil {
		log.Fatal(err)
	}

	// Loop forever and just read the messages
	fmt.Println("Starting to consume stream")
	for event := range stream {
		fmt.Printf("Event: %s\n", event.CorrelationId)
		fmt.Printf("Headers: %v\n", event.Headers)
		// The payload is in the body
		fmt.Printf("Data: %v\n", string(event.Body))
	}
	ch.Close()
}
