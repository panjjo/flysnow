package fly

import (
	instance "flysnow/tmp"
	"sync"
)

type Calculation struct {
}

func (c *Calculation) reader(d *BodyData) {
	t := instance.TMP{B: d.Body, Tag: d.Tag, WG: &sync.WaitGroup{}}
	code := t.EXEC()
	if d.NeedResp != 0 {
		ConnRespChannel <- &connResp{d.Connid, code, nil}
	}
}
