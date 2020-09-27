package rabbit

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"os"
)

type WithTopic struct {
	URI          string
	ExchangeKind string
	ExchangeName    string
}

func NewRabbitWithFanout(exName string) *WithTopic {

	usr := os.Getenv("RABBIT_USER")
	pss := os.Getenv("RABBIT_PASS")
	ip := os.Getenv("RABBIT_TARGET_IP")

	uri := URIBuilderRabbit(usr, pss, ip)

	return &WithTopic{
		URI:       uri,
		ExchangeName: exName,
		ExchangeKind: "fanout",
	}
}

func (r *WithTopic) PublishMessage(msg string) (err error) {

	conn, err := amqp.Dial(r.URI)
	if err != nil {
		logrus.Error("Failed to open connection to RabbitMQ")
		return err
	}

	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		logrus.Error("Failed to open channel to RabbitMQ")
		return err
	}

	defer ch.Close()

	err = ch.ExchangeDeclare(
		r.ExchangeName, // name
		r.ExchangeKind,    // type
		true,        // durable
		false,       // auto-deleted
		false,       // internal
		false,       // no-wait
		nil,         // arguments
	)
	if err != nil {
		logrus.Error("Failed to declare exchange")
		return err
	}
	// publishing message
	err = ch.Publish(
		r.ExchangeName,
		"",
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(msg),
		})
	return
}

func URIBuilderRabbit(usr, pss, add string) string {
	if usr == "" || pss == "" {
		usr = "guest"
		pss = "guest"
	}
	if add == "" {
		add = "localhost"
	}
	return fmt.Sprintf("amqp://" + usr + ":" + pss + "@" + add + ":5672/")
}

func (r *WithTopic) Consume() (err error) {
	conn, err := amqp.Dial(r.URI)
	if err != nil {
		logrus.Error("Error connecting to RabbitMQ")
		logrus.Error(err.Error())
		return
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		logrus.Error("Failed to open channel")
		return
	}
	err = ch.ExchangeDeclare(
		r.ExchangeName, // name
		r.ExchangeKind,    // type
		true,        // durable
		false,       // auto-deleted
		false,       // internal
		false,       // no-wait
		nil,         // arguments
	)
	if err != nil {
		logrus.Error("Failed to declare exchange")
		return
	}

	q, err := ch.QueueDeclare(
		r.ExchangeName,
		false,
		false,
		true,
		false,
		nil,
		)
	if err != nil{
		logrus.Error("Failed to declare queue")
		logrus.Error(err.Error())
		return
	}
	err = ch.QueueBind(
		q.Name,
		"",
		r.ExchangeName,
		false,
		nil,
		)

	msgs, err := ch.Consume(
		r.ExchangeName,
		"",
		true,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		logrus.Error("Failed to register consumer")
		logrus.Error(err.Error())
		return
	}

	for d := range msgs {
		logrus.Info(string(d.Body))
	}
	return
}
