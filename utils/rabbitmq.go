package utils

import (
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

type Queueinfo struct {
	QueueRoutekey string
	QueueExchange string
}

var (
	//Conns    map[string]*amqp.Connection
	Conns    sync.Map
	Channels map[string]*amqp.Channel
	ConnChan chan *amqp.Connection
	// connectOnce, logOnce *sync.Once
	StartQueueListen bool
	QUEUE_HOST       string
	QUEUE_NAME       string
	QUEUE_EXCHANGE   string
)

func InitMQ(host string) {
	// logOnce = new(sync.Once)     //refresh Once instance
	// connectOnce = new(sync.Once) //refresh Once instance
	Channels = make(map[string]*amqp.Channel, 0)
	ConnChan = make(chan *amqp.Connection)
	Conns = sync.Map{}
	// connectOnce.Do(func() {
	// go connect(Host)
	// })
	//Conn = <-ConnChan
	// go func() {
	//  for Conn = range ConnChan {
	//      Channels = make(map[string]*amqp.Channel, 0)
	//      // dispatch()
	//      logOnce = new(sync.Once)     //refresh Once instance
	//      connectOnce = new(sync.Once) //refresh Once instance
	//      Log.Info("rabbitmq connection back online.")
	//  }
	// }()
}

func connect(host string) {
	ticker := time.Tick(1000 * time.Millisecond)
	for range ticker {
		conn, err := amqp.Dial("amqp://" + host)
		if err == nil {
			ConnChan <- conn
			return
		}
		go func() {
			Log.ERROR.Printf("Could not connect to rabbitmq server. Error: %s,Host:%s", err, host)
		}()
	}
}

func GetChannel(name string, exchange string) (ch *amqp.Channel, err error) {
	var ok bool
	if ch, ok = Channels[name]; !ok {
		if _, ok = Conns.Load(name); !ok {
			go connect(QUEUE_HOST)
			Conns.Store(name, <-ConnChan)
		}
		c, _ := Conns.Load(name)
		ch, err = c.(*amqp.Connection).Channel()
		if err != nil {
			Log.ERROR.Printf("conn.channel err: %v", err)
			Conns.Delete(name)
			// connect(Host)
			return nil, err
		}
		err = ch.Qos(3, 0, false)
		if err != nil {
			Log.ERROR.Printf("basic.qos: %v", err)
		}

		err = ch.ExchangeDeclare(exchange, strings.Split(exchange, ".")[0], true, false, false, false, nil)
		if err != nil {
			Log.ERROR.Printf("exchange declare: %v", err)
		}

		Channels[name] = ch
	}
	return Channels[name], err
}

func Consume(ch *amqp.Channel, b *Queueinfo, tag string) (<-chan amqp.Delivery, error) {
	var err error
	q, err := ch.QueueDeclare(b.QueueRoutekey, true, false, false, false, nil)
	if err != nil {
		Log.ERROR.Printf("queue declare err: %v", err)
	}

	err = ch.QueueBind(b.QueueRoutekey, b.QueueRoutekey, b.QueueExchange, false, nil)
	if err != nil {
		Log.ERROR.Printf("queue bind err: %v", err)
	}

	tasks, err := ch.Consume(q.Name, tag, false, false, false, false, nil)
	return tasks, err
}

func Publish(exchange, key string, t interface{}, n int) error {
	body := []byte(JsonEncode(t, false))
	if len(body) == 0 {
		Log.ERROR.Printf("Send empty msg to Mq :%+v", t)
		return errors.New("msg error")
	}
	msg := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		ContentType:  "application/json",
		Body:         body,
	}

	ch, err := GetChannel("publish", exchange)
	if err == nil {
		err = ch.Publish(exchange, key, false, false, msg)
		if err != nil {
			if n > 0 {
				delete(Channels, "publish")
				Conns.Delete("publish")
				Publish(exchange, key, t, n-1)
			} else {
				Log.ERROR.Printf("Send msg to MQ err:%v,%+v", err, t)
			}
		}
	}
	return err
}
