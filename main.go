package main

import (
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"os"
	"strings"
)


func main(){

	conn , err := amqp.Dial("amqp://admin:guest@localhost:5672/")
	if err != nil {
		logrus.Error("Connection to Rabbit")
		return
	}
	defer conn.Close()

	ch , err := conn.Channel()
	if err != nil{
		logrus.Error("Failed to open channel")
		return
	}

	err = ch.ExchangeDeclare(
		"flow",
		"fanout",
		true,
		false,
		false,
		false,
		nil,
	)

	if err != nil{
		logrus.Error("Failed to declare exchange")
		return
	}
	body := bodyFrom(os.Args)
	err = ch.Publish(
		"logs",
		"",
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body: []byte(body),
		})
	if err != nil{
		logrus.Error("failed to publish message")
		return
	}

	logrus.Info("Message delivered")

}

func bodyFrom(args []string) string {
	var s string
	if (len(args) < 2) || os.Args[1] == "" {
		s = "hello"
	} else {
		s = strings.Join(args[1:], " ")
	}
	return s
}