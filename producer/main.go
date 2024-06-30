package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/google/uuid"
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

	// Create a new Queue
	q, err := ch.QueueDeclare("events", true, false, false, true, amqp.Table{
		"x-queue-type": "stream",
		// each segment file is allowed 0.03MB
		"x-stream-max-segment-size-bytes": 30000,
		// total stream size is 0.15MB
		// whenever we overflow this, we will start deleting
		// oldest segment
		"x-max-length-bytes": 150000,

		"x-max-age": "100s",
	})
	if err != nil {
		log.Fatal(err)
	}

	// Publish 1001 messages
	ctx := context.Background()
	for i := 0; i <= 1000; i++ {
		event := Event{
			Name: "test",
		}

		data, err := json.Marshal(event)
		if err != nil {
			log.Fatal(err)
		}

		err = ch.PublishWithContext(ctx, "", "events", false, false, amqp.Publishing{
			Body:          data,
			CorrelationId: uuid.NewString(),
		})
		if err != nil {
			log.Fatal(err)
		}

	}

	// Close the channel to await all messages being sent
	ch.Close()
	fmt.Println(q.Name)

}
