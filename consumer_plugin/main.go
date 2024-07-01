package main

import (
	"fmt"
	"log"
	"time"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

const (
	EVENTSTREAM = "events"
)

func main() {
	// Connect to the Stream Plugin on Rabbitmq
	env, err := stream.NewEnvironment(
		stream.NewEnvironmentOptions().
			SetHost("localhost").
			SetPort(5552).
			SetUser("guest").
			SetPassword("guest"))
	if err != nil {
		log.Fatal(err)
	}

	// Create Consumer Options, we set x-stream-offset
	consumerOptions := stream.NewConsumerOptions().
		SetConsumerName("consumer_1").
		SetOffset(stream.OffsetSpecification{}.Offset(5998))

	// Start a Consumer
	consumer, err := env.NewConsumer(EVENTSTREAM, messageHandler, consumerOptions)
	if err != nil {
		log.Fatal(err)
	}

	time.Sleep(5 * time.Second)
	consumer.Close()
}

func messageHandler(consumerContext stream.ConsumerContext, message *amqp.Message) {
	fmt.Printf("Event: %s\n", message.Properties.CorrelationID)
	fmt.Printf("Data: %v\n", string(message.GetData()))

	// Unmarshal your data into struct here
}
