package main

import (
	"github.com/barajas123/go-lib-rabbit/rabbit"
	"github.com/sirupsen/logrus"
	"os"
)

func main() {

	p := rabbit.NewRabbitWithTopic("flow-raw")
	m := os.Args
	err := p.PublishMessage(m[1])
	if err != nil {
		logrus.Errorf(err.Error())
		return
	}
	logrus.Info("Success")
}
