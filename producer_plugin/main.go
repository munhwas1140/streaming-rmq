package main

import (
	"encoding/json"
	"log"

	"github.com/google/uuid"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

const (
	EVENTSTREAM = "events"
)

type Event struct {
	Name string
}

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

	// Declare the stream, Set segmentsize and MaxBytes on stream
	err = env.DeclareStream(EVENTSTREAM, stream.NewStreamOptions().
		SetMaxSegmentSizeBytes(stream.ByteCapacity{}.MB(1)).
		SetMaxLengthBytes(stream.ByteCapacity{}.MB(2)))

	if err != nil {
		log.Fatal(err)
	}

	// Create a new Producer
	producerOptions := stream.NewProducerOptions()
	producerOptions.SetProducerName("producer")

	// Batch 100 Events in the same Frame, and SDK will handle everything
	producerOptions.SetSubEntrySize(100)
	producerOptions.SetCompression(stream.NewProducerOptions().Compression.Gzip())

	producer, err := env.NewProducer(EVENTSTREAM, producerOptions)
	if err != nil {
		log.Fatal(err)
	}

	// Publish 6000 messages
	for i := 0; i < 6000; i++ {
		event := Event{
			Name: "test",
		}

		data, err := json.Marshal(event)
		if err != nil {
			log.Fatal(err)
		}

		message := amqp.NewMessage(data)

		// Apply properties to our message
		props := &amqp.MessageProperties{
			CorrelationID: uuid.NewString(),
		}
		message.Properties = props

		// Sending the message
		if err := producer.Send(message); err != nil {
			log.Fatal(err)
		}
	}

	producer.Close()
}
