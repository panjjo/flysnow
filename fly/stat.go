package fly

import "flysnow/snow"

type Statistics struct {
}

func (s *Statistics) reader(d *BodyData) {
	log.TRACE.Printf("receive connid:%s, op:%d,tag:%s,data:%s", d.Connid, d.Op, d.Tag, string(d.Body))
	err, result := snow.Stat(d.Body, d.Tag)
	if err != nil {
		log.ERROR.Printf("Stat error tag:%s,err:%s", d.Tag, err)
	}
	log.TRACE.Printf("response connid:%s, op:%d,tag:%s,data:%s", d.Connid, d.Op, d.Tag, string(d.Body))
	ConnRespChannel <- &connResp{d.Connid, 0, result}
}

type Clear struct {
}

func (s *Clear) reader(d *BodyData) {
	log.TRACE.Printf("response connid:%s, op:%d,tag:%s,data:%s", d.Connid, d.Op, d.Tag, string(d.Body))
	err, result := snow.Clear(d.Body)
	if err != nil {
		log.ERROR.Printf("Clear error err:%s", err)
	}
	log.TRACE.Printf("response connid:%s, op:%d,tag:%s,data:%s", d.Connid, d.Op, d.Tag, string(d.Body))
	ConnRespChannel <- &connResp{d.Connid, result, err}
}
func ClearRedisKey(tag string) {
	snow.ClearRedisKey(tag)
}
