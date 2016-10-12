package fly

import (
	"flysnow/snow"
	"time"
)

type Statistics struct {
	channel chan *BodyData
}

func (s *Statistics) handle() {
	for {
		d := <-s.channel
		err, result := snow.Stat(d.Body, d.Tag)
		if err != nil {
			log.Error("Stat error tag:%s,err:%s", d.Tag, err)
		}
		ConnRespChannel <- &connResp{d.Connid, 0, result}
	}
}

func (s *Statistics) reader(data *BodyData) {

	select {
	case s.channel <- data:
	case <-time.After(1 * time.Second):
		ConnRespChannel <- &connResp{data.Connid, TimeOut, nil}
	}
}

func (s *Statistics) initchan() {
	s.channel = make(chan *BodyData, 2)
}
