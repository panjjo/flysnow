package utils

import (
	"github.com/sirupsen/logrus"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

type amqpChann struct {
	chann *amqp.Channel
	conn  *amqp.Connection
}

var StartQueueListen bool

var MQDef *Rabbitmq

type Rabbitmq struct {
	config   *RabbitmqConfig
	channels *sync.Map
}

func InitMQ(config *RabbitmqConfig) {
	MQDef = NewRabbitmq(config)
}

func NewRabbitmq(config *RabbitmqConfig) *Rabbitmq {
	return &Rabbitmq{
		config:   config,
		channels: &sync.Map{},
	}
}

type RabbitmqConfig struct {
	Addr         string `json:"host" yaml:"host" mapstructure:"host"`
	Exchange     string `json:"exchange" yaml:"exchange"  mapstructure:"exchange"`
	ExchangeType string `json:"extype" yaml:"extype" mapstructure:"extype"`
	Retry        int    `json:"retry" yaml:"retry" mapstructure:"retry"`
	Name         string `json:"name" yaml:"name" mapstructure:"name"`
	Open         bool   `json:"queue" yaml:"queue" mapstructure:"queue"`
}

var DefaultMQConfig *RabbitmqConfig = &RabbitmqConfig{
	Addr:         "amqp://rabbitmq.host:5672/flysnow",
	Exchange:     "topic.flysnow",
	ExchangeType: "topic",
	Retry:        3,
	Name:         "flysnow",
	Open:         false,
}

func (r *Rabbitmq) amqpConnect() (*amqp.Connection, error) {
	conn, err := amqp.Dial(r.config.Addr)
	if err != nil {
		logrus.Warnf("conn amqp fail,host:%s,err:%v", r.config.Addr, err)
	}
	return conn, err
}

func (r *Rabbitmq) amqpChannel(name string) (ch *amqp.Channel, err error) {
	if ac, ok := r.channels.Load(name); !ok {
		conn, err := r.amqpConnect()
		if err != nil {
			return nil, err
		}
		ch, err = conn.Channel()
		if err != nil {
			logrus.Warnf("get amqp channel fail,name:%s,host:%s,err:%v", name, r.config.Addr, err)
			return nil, err
		}
		err = ch.Qos(1, 0, false)
		if err != nil {
			logrus.Warnf(" amqp channel qos fail,name:%s,host:%s,err:%v", name, r.config.Addr, err)
			return nil, err
		}
		err = ch.ExchangeDeclare(r.config.Exchange, r.config.ExchangeType, true, false, false, false, nil)
		if err != nil {
			logrus.Warnf(" amqp exchange declare fail,name:%s,host:%s,err:%v", name, r.config.Addr, err)
			return nil, err
		}
		r.channels.Store(name, amqpChann{ch, conn})
		return ch, err
	} else {
		return ac.(amqpChann).chann, nil
	}
}

func (r *Rabbitmq) Consume(name, tag, routingKey string) (<-chan amqp.Delivery, error) {
	ch, err := r.amqpChannel(name)
	if err != nil {
		logrus.Warnf("get amqp channel fail,name:%s,host:%s,err:%v", name, r.config.Addr, err)
		r.channels.Delete(name)
		return nil, err
	}
	q, err := ch.QueueDeclare(routingKey, true, false, false, false, nil)
	if err != nil {
		logrus.Warnf("amqp queue declare fail,name:%s,routingKey:%s,err:%v", name, routingKey, err)
		r.channels.Delete(name)
		return nil, err
	}

	err = ch.QueueBind(routingKey, routingKey, r.config.Exchange, false, nil)
	if err != nil {
		logrus.Warnf("amqp queue bind fail,name:%s,routingKey:%s,err:%v", name, routingKey, err)
		r.channels.Delete(name)
		return nil, err
	}

	chann, err := ch.Consume(q.Name, tag, false, false, false, false, nil)
	if err != nil {
		r.channels.Delete(name)
	}
	return chann, err
}

func (r *Rabbitmq) PublishWithRetry(routingKey string, data interface{}, n int) error {
	body := JsonEncode(data, false)

	msg := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		ContentType:  "application/json",
		Body:         body,
	}

	ch, err := r.amqpChannel("publish")
	if err == nil {
		err = ch.Publish(r.config.Exchange, routingKey, false, false, msg)
		if err != nil {
			if n > 0 {
				r.channels.Delete("publish")
				r.PublishWithRetry(routingKey, data, n-1)
			} else {
				logrus.Warnf("amqp push fail,routingKey:%s,err:%v", routingKey, err)
			}

		}
	}
	return err
}
func (r *Rabbitmq) Publish(routingKey string, data interface{}) error {
	return r.PublishWithRetry(routingKey, data, r.config.Retry)
}
