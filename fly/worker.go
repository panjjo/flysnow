package fly

import (
	"sync"

	"github.com/garyburd/redigo/redis"
	"github.com/panjjo/flysnow/models"
	"github.com/panjjo/flysnow/snow"
	instance "github.com/panjjo/flysnow/tmp"
	"github.com/panjjo/flysnow/utils"
	"github.com/sirupsen/logrus"
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

type Statistics struct {
}

func (s *Statistics) reader(d *BodyData) {
	if _, ok := instance.TermListMap[d.Tag]; !ok {
		ConnRespChannel <- &connResp{d.Connid, models.ErrMethodNotFount, nil}
		return
	}
	logrus.Debugf("receive connid:%s, op:%d,tag:%s,data:%s", d.Connid, d.Op, d.Tag, string(d.Body))
	err, result := snow.Stat(d.Body, d.Tag)
	if err != nil {
		logrus.Errorf("Stat error tag:%s,err:%s", d.Tag, err)
	}
	logrus.Debugf("response connid:%s, op:%d,tag:%s,data:%v", d.Connid, d.Op, d.Tag, result)
	ConnRespChannel <- &connResp{d.Connid, 0, result}
}

type Clear struct {
}

func (s *Clear) reader(d *BodyData) {
	logrus.Debugf("receive connid:%s, op:%d,tag:%s,data:%s", d.Connid, d.Op, d.Tag, string(d.Body))
	err, result := snow.Clear(d.Body)
	if err != nil {
		logrus.Errorf("Clear error err:%s", err)
	}
	logrus.Debugf("response connid:%s, op:%d,tag:%s,data:%v", d.Connid, d.Op, d.Tag, result)
	ConnRespChannel <- &connResp{d.Connid, result, err}
}

type HeartBeat struct {
}

func (s *HeartBeat) reader(d *BodyData) {
	ConnRespChannel <- &connResp{d.Connid, 0, nil}
}

type Rotate struct {
}

func (s *Rotate) reader(d *BodyData) {
	rdsconn := utils.NewRedisConn()
	defer rdsconn.Close()
	_, err := redis.String(rdsconn.Dos("set", "apiRotate", utils.GetNowSec(), "nx"))
	if err != nil {
		ConnRespChannel <- &connResp{d.Connid, models.ErrTimeOut, nil}
	}
	defer rdsconn.Dos("del", "apiRotate")
	snow.AutoRotate()
	ConnRespChannel <- &connResp{d.Connid, 0, nil}
}
