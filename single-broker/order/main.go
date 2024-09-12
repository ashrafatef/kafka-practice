package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Order struct {
	ID    int    `json:"id"`
	Name  string `json:"name"`
	Price int    `json:"price"`
}

const (
	value = "order"
)

func Producer() {

	topic := "orders-created"
	results, err := createTopic(topic)
	if err != nil {
		fmt.Println("Failed to create topic: ", err)
		os.Exit(1)
	}
	fmt.Println("Results -->", results[0].Topic)

	publishMessage(results[0].Topic, "message to be published +++++++ 2")
}

func createTopic(topic string) (topicResults []kafka.TopicResult, err error) {

	a, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": "localhost"})
	if err != nil {
		fmt.Printf("Failed to create Admin client: %s\n", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	maxDur, err := time.ParseDuration("60s")
	results, err := a.CreateTopics(
		ctx,
		[]kafka.TopicSpecification{{
			Topic:             topic,
			NumPartitions:     2,
			ReplicationFactor: 2,
		}},
		kafka.SetAdminOperationTimeout(maxDur))

	return results, err
}

func publishMessage(topic string, message string) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost"})
	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}
	defer p.Close()

	delivery_chan := make(chan kafka.Event, 10000)
	err = p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte(message),
	}, delivery_chan)

	if err != nil {
		fmt.Printf("Failed to produce message: %s\n", err)
		os.Exit(1)
	}

	e := <-delivery_chan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
	} else {
		fmt.Printf("Delivered message to topic %s [%d] at offset %v\n , key : %v , value : %s",
			*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset, m.Key, m.Value)
	}
	close(delivery_chan)

	p.Flush(1 * 1000)

	fmt.Println("Produced all messages to the topic:", topic)
}

func main() {
	// r := gin.Default()
	// r.GET("/ping", func(c *gin.Context) {
	// 	c.JSON(http.StatusOK, gin.H{
	// 		"message": "pong",
	// 	})
	// })
	// topic := "orders"

	// p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost", "broker.address.family": "v4"})

	// if err != nil {
	// 	fmt.Printf("Failed to create producer: %s\n", err)
	// 	os.Exit(1)
	// }

	// r.POST("/order", func(ctx *gin.Context) {
	// 	var order Order
	// 	if err := ctx.ShouldBindJSON(&order); err != nil {
	// 		ctx.Error(err)
	// 		ctx.AbortWithStatus(http.StatusBadRequest)
	// 		return
	// 	}
	// 	data, _ := json.Marshal(order)
	// 	delivery_chan := make(chan kafka.Event, 10000)
	// 	err = p.Produce(&kafka.Message{
	// 		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
	// 		Value:          []byte(data)},
	// 		delivery_chan,
	// 	)

	// 	e := <-delivery_chan
	// 	m := e.(*kafka.Message)

	// 	if m.TopicPartition.Error != nil {
	// 		fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
	// 	} else {
	// 		fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
	// 			*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
	// 	}
	// 	close(delivery_chan)
	// 	ctx.JSON(http.StatusOK, order)
	// })

	// r.Run("0.0.0.0:4000") // listen and serve on 0.0.0.0:8080 (for windows "localhost:8080")

	Producer()
}
