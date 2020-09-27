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
	TopicName    string
}

func NewRabbitWithFanout(topic string) *WithTopic {

	usr := os.Getenv("RABBIT_USER")
	pss := os.Getenv("RABBIT_PASS")
	ip := os.Getenv("RABBIT_TARGET_IP")

	uri := URIBuilderRabbit(usr, pss, ip)

	return &WithTopic{
		URI:       uri,
		TopicName: topic,
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
		r.TopicName, // name
		"fanout",    // type
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
		r.ExchangeKind,
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
		r.TopicName, // name
		"fanout",    // type
		true,        // durable
		false,       // auto-deleted
		false,       // internal
		false,       // no-wait
		nil,         // arguments
	)
	if err != nil {
		logrus.Error("Failed to declare exchange")
		logrus.Error(err.Error())
		return
	}
	q, err := ch.QueueDeclare(
		"",
		false,
		false,
		true,
		false,
		nil,
	)
	if err != nil {
		logrus.Error("Failed to declare queue")
		logrus.Error(err.Error())
		return
	}

	err = ch.QueueBind(q.Name,
		"",
		r.TopicName,
		false,
		nil,
	)
	if err != nil {
		logrus.Error("Failed to bind queue")
		logrus.Error(err.Error())
		return
	}
	msgs, err := ch.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		logrus.Error("Failed to register consumer")
		return
	}

	for d := range msgs {
		logrus.Info(d.Body)
	}
	return
}
