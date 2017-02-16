package fly

import "flysnow/snow"

type Statistics struct {
}

func (s *Statistics) reader(d *BodyData) {

	err, result := snow.Stat(d.Body, d.Tag)
	if err != nil {
		log.ERROR.Printf("Stat error tag:%s,err:%s", d.Tag, err)
	}
	ConnRespChannel <- &connResp{d.Connid, 0, result}
}

type Clear struct {
}

func (s *Clear) reader(d *BodyData) {
	err, result := snow.Clear(d.Body)
	if err != nil {
		log.ERROR.Printf("Clear error err:%s", err)
	}
	ConnRespChannel <- &connResp{d.Connid, result, err}
}
func ClearRedisKey(tag string) {
	snow.ClearRedisKey(tag)
}
