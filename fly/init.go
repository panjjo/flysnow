package fly

import (
	"github.com/panjjo/flysnow/models"
	"github.com/panjjo/flysnow/snow"
	"github.com/panjjo/flysnow/utils"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"runtime"
	"time"
)

var (
	handleFuncs map[int]map[string]ListenChanFunc
)

type ListenChanFunc interface {
	reader(data *BodyData)
}

func Init() {

	runtime.GOMAXPROCS(runtime.NumCPU())

	ConnMaps = ConnMapStruct{m: map[string]*ConnStruct{}}

	handleFuncs = map[int]map[string]ListenChanFunc{
		0: map[string]ListenChanFunc{},                  // Ping
		1: map[string]ListenChanFunc{},                  // 统计
		2: map[string]ListenChanFunc{},                  // 计算
		3: map[string]ListenChanFunc{"clear": &Clear{}}, // Clear
	}
	// calculation
	handle := &Calculation{}
	stat := &Statistics{}
	utils.InitRedis(&utils.Config.RDS)
	utils.MgoInit(&utils.Config.Mgo)
	for _, tag := range models.TagList {
		handleFuncs[2][tag] = handle
		handleFuncs[1][tag] = stat
	}
	// 初始化数据库索引
	// 系统索引
	logrus.Info("init mongo index...")
	mgosess := utils.MgoSessionDupl()
	for tag, terms := range models.TermConfigMap {
		for _, term := range terms {
			indexTable := mgosess.DB(utils.MongoPrefix + tag).C(utils.MongoIndex + term.Name)
			for _, keys := range utils.DefaultMgoIndexs {
				indexTable.EnsureIndexKey(keys...)
			}
			objTable := mgosess.DB(utils.MongoPrefix + tag).C(utils.MongoOBJ + term.Name)
			for _, keys := range utils.DefaultMgoObjIndexs {
				objTable.EnsureIndexKey(keys...)
			}
		}
	}
	mgosess.Close()
	logrus.Info("mongo index end")

	ConnRespChannel = make(chan *connResp, 100)
	snow.Init()
	// 启动rabbitmq 监听
	if utils.Config.MQ.Open {
		utils.InitMQ(&utils.Config.MQ)
		go func() {
			for {
				pch, err := utils.MQDef.Consume(utils.Config.MQ.Name, "A", utils.Config.MQ.Name)
				if err != nil {
					logrus.Infof("Start Queue Listen Err:%v", err)
					time.Sleep(1 * time.Second)
					continue
				}
				for d := range pch {
					go func(t amqp.Delivery) {
						data := &BodyData{}
						if err := utils.JsonDecode(t.Body, data); err != nil {
							logrus.Errorf("Json Decode Queue Body Err:%v", err)
							return
						}
						if fs, ok := handleFuncs[data.Op]; ok {
							if f, ok := fs[data.Tag]; ok {
								f.reader(data)
							} else {
								logrus.Warnf("Queue data tag not found ,op:%s,tag:%s", data.Op, data.Tag)
							}
						} else {
							logrus.Warnf("Queue data op not found ,op:%s", data.Op)
						}
						t.Ack(false)
					}(d)
				}
				logrus.Infof("stop consume mq")
			}
		}()
	}
}
