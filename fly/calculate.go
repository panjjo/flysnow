package fly

import (
	instance "flysnow/tmp"
	"time"
)

type Calculation struct {
	channel chan *BodyData
}

func (c *Calculation) handle() {
	for {
		d := <-c.channel
		if itf, ok := instance.InterfaceMap[d.Tag]; ok {
			for _, v := range itf {
				go v.Exec(d.Body)
			}
			ConnRespChannel <- &connResp{d.Connid, 0, nil}
		} else {
			ConnRespChannel <- &connResp{d.Connid, ErrMethodNotFount, nil}
		}
	}
}

func (c *Calculation) reader(data *BodyData) {

	select {
	case c.channel <- data:
	case <-time.After(1 * time.Second):
		ConnRespChannel <- &connResp{data.Connid, TimeOut, nil}
	}
}

func (c *Calculation) initchan() {
	c.channel = make(chan *BodyData, 2)
}
