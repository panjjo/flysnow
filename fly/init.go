package fly

import (
	"runtime"
	"time"

	"github.com/panjjo/flysnow/models"
	"github.com/panjjo/flysnow/snow"
	"github.com/panjjo/flysnow/utils"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

var (
	handleFuncs map[int]ListenChanFunc
)

type ListenChanFunc interface {
	reader(data *BodyData)
}

func Init() {

	runtime.GOMAXPROCS(runtime.NumCPU())

	ConnMaps = ConnMapStruct{m: map[string]*ConnStruct{}}
	handleFuncs = map[int]ListenChanFunc{
		models.OPCHECK:  &HeartBeat{},   // Ping
		models.OPQUERY:  &Statistics{},  // 统计
		models.OPSTAT:   &Calculation{}, // 计算
		models.OPCLEAR:  &Clear{},       // Clear
		models.OPROTATE: &Rotate{},      // rotate
	}
	utils.InitRedis(&utils.Config.RDS)
	utils.MgoInit(&utils.Config.Mgo)

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
							fs.reader(data)
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
