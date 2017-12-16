package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func main() {

	cnstring := flag.String("cnstring", "amqp://guest:guest@localhost:5672/", "the amqp connection string")
	queueName := flag.String("queueName", "", "queue to be consumed")
	autoack := flag.Bool("autoack", false, "auto-ack if true")

	flag.Parse()

	fmt.Println("cnstring:", *cnstring)
	fmt.Println("queueName:", *queueName)

	conn, err := amqp.Dial(*cnstring)
	failOnError(err, "Failed to connect to server")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	msgs, err := ch.Consume(
		*queueName,
		"bunnydump",
		*autoack,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "Failed to register the consumer")

	forever := make(chan bool)

	//create output directory
	err = os.MkdirAll(*queueName, os.ModePerm)
	failOnError(err, "Failed to create output directory")

	//receiving messages
	go func() {
		for d := range msgs {
			log.Printf("Received a message: %d bytes", len(d.Body))

			//inline 'cause there's no chance for reuse
			fo, err := os.Create(filepath.Join("./", *queueName, strconv.FormatInt(int64(time.Now().UnixNano()), 10)+".log"))
			if err != nil {
				failOnError(err, "Failed to create one of many output files")
			}
			defer fo.Close()

			fo.Write(d.Body)
			fo.Sync()
		}
	}()

	log.Printf(" [*] Receiving messages. To exit press CTRL+C")

	<-forever
}
