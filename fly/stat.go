package fly

import "github.com/panjjo/flysnow/snow"

type Statistics struct {
}

func (s *Statistics) reader(d *BodyData) {
	log.DEBUG.Printf("receive connid:%s, op:%d,tag:%s,data:%s", d.Connid, d.Op, d.Tag, string(d.Body))
	err, result := snow.Stat(d.Body, d.Tag)
	if err != nil {
		log.ERROR.Printf("Stat error tag:%s,err:%s", d.Tag, err)
	}
	log.DEBUG.Printf("response connid:%s, op:%d,tag:%s,data:%v", d.Connid, d.Op, d.Tag, result)
	ConnRespChannel <- &connResp{d.Connid, 0, result}
}

type Clear struct {
}

func (s *Clear) reader(d *BodyData) {
	log.DEBUG.Printf("receive connid:%s, op:%d,tag:%s,data:%s", d.Connid, d.Op, d.Tag, string(d.Body))
	err, result := snow.Clear(d.Body)
	if err != nil {
		log.ERROR.Printf("Clear error err:%s", err)
	}
	log.DEBUG.Printf("response connid:%s, op:%d,tag:%s,data:%v", d.Connid, d.Op, d.Tag, result)
	ConnRespChannel <- &connResp{d.Connid, result, err}
}
func ClearRedisKey(tag string) {
	snow.ClearRedisKey(tag)
}
