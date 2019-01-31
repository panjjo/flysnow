package fly

import (
	instance "github.com/panjjo/flysnow/tmp"
	"sync"
)

type Calculation struct {
}

func (c *Calculation) reader(d *BodyData) {
	t := instance.TMP{B: d.Body, Tag: d.Tag, WG: &sync.WaitGroup{}}
	log.DEBUG.Printf("receive op:%d,tag:%s,data:%s", d.Op, d.Tag, string(d.Body))
	code := t.EXEC()
	if d.NeedResp != 0 {
		ConnRespChannel <- &connResp{d.Connid, code, nil}
	}
}
