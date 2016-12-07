package fly

import (
	"flysnow/snow"
)

type HeartBeat struct {
}

func (s *HeartBeat) reader(d *BodyData) {
	ConnRespChannel <- &connResp{d.Connid, 0, result}
}
