package fly

import (
	"flysnow/snow"
	"fmt"
)

type Statistics struct {
}

func (s *Statistics) reader(d *BodyData) {

	fmt.Printf("id:%v fly/stat.go0\n", d.Connid)
	err, result := snow.Stat(d.Body, d.Tag)
	if err != nil {
		log.ERROR.Printf("Stat error tag:%s,err:%s", d.Tag, err)
	}
	fmt.Printf("id:%v fly/stat.go1\n", d.Connid)
	ConnRespChannel <- &connResp{d.Connid, 0, result}
	fmt.Printf("id:%v fly/stat.go2\n", d.Connid)
}
