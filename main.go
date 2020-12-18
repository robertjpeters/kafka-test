package main

import (
	"context"
	"encoding/json"
	"fmt"
	kafka "github.com/segmentio/kafka-go"
	"kafka-test/config"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const Producer = "producer"
const Consumer = "consumer"

type KafkaConnection struct {
	connection *kafka.Conn
	err	error
}

type Message struct {
	Temperature float64
	Time int64
}

// Attempts to make a connection to our Kafka broker
func newKafkaConnection(result chan KafkaConnection, attempts int) {
	address := config.Config.GetString("address")
	fmt.Printf("Connecting to %v...\n", address)
	conn, err := kafka.DialLeader(
		context.Background(),
		"tcp",
		address,
		config.Config.GetString("topic"),
		config.Config.GetInt("partition"),
	)
	if err == nil || attempts < 1 {
		result <- KafkaConnection{ conn, err }
	} else {
		// Try again after 10 seconds to connect until we run out of attempts
		fmt.Printf("Unable to connect to %v, retrying in 5 seconds...\n", address)
		time.Sleep(5 * time.Second)
		attempts--
		newKafkaConnection(result, attempts)
	}
}

// Generates a random temp
func randomTemperature() float64 {
	return -50 + rand.Float64() * (200 - -50)
}

// Writes a random temp to the stream every 1s
func writeMessages() {
	// Create a writer using config values from config.yml
	w := &kafka.Writer{
		Addr:     kafka.TCP(config.Config.GetString("address")),
		Topic:   config.Config.GetString("topic"),
		Balancer: &kafka.LeastBytes{},
	}
	for {
		// TODO: Handle JSON marshalling error
		message, _ := json.Marshal(
			Message {
				randomTemperature(),
				time.Now().Unix(),
			},
		)
		// Write out our message to the stream
		err := w.WriteMessages(context.Background(),
			kafka.Message { Value: message },
		)
		if err != nil {
			log.Fatal("Failed to write messages: ", err)
		}
		time.Sleep(1 * time.Second)
	}
}

// Reads the stream with a 10ms delay
func readMessages() {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{config.Config.GetString("address")},
		Topic:     config.Config.GetString("topic"),
		Partition: config.Config.GetInt("partition"),
		MinBytes:  10, // 10B
		MaxBytes:  10e6, // 10MB
	})
	for {
		// Read a message from the stream and unmarshal it back to a Message
		var message Message
		b, err := r.ReadMessage(context.Background())
		if err == nil {
			// TODO: Handle json unmarshal error
			_ = json.Unmarshal(b.Value, &message)
			fmt.Println(message)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func main() {
	// Load config to get connection and topic details
	config.LoadConfig()

	// Attempt to connect to our kafka instance
	connect := make(chan KafkaConnection)
	go newKafkaConnection(connect, config.Config.GetInt("attempts"))
	instance := <- connect

	// Log if for some reason we can't connect
	if instance.err != nil {
		log.Fatal("Failed to connect: ", instance.err)
	}

	// Now we know that we can successfully connect so close our test connection and move on
	_ = instance.connection.Close()

	// Look for a client or server arg to determine how we're running
	if len(os.Args) < 2 {
		log.Fatal("Missing run type (producer/consumer)")
	}
	switch os.Args[1] {
		case Producer:
			log.Println("Running in producer mode")
			go writeMessages()
		case Consumer:
			log.Println("Running in consumer mode")
			go readMessages()
		default:
			log.Fatal("Missing run type (producer/consumer)")
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
}
