package fly

type HeartBeat struct {
}

func (s *HeartBeat) reader(d *BodyData) {
	ConnRespChannel <- &connResp{d.Connid, 0, nil}
}
