package fly

import (
	"flysnow/models"
	"flysnow/utils"
	"runtime"
)

var (
	handleFuncs map[int]map[string]ListenChanFunc
	log         utils.LogS
)

type ListenChanFunc interface {
	reader(data *BodyData)
}

func Init() {
	log = utils.Log
	runtime.GOMAXPROCS(runtime.NumCPU())

	ConnMaps = ConnMapStruct{m: map[string]*ConnStruct{}}

	handleFuncs = map[int]map[string]ListenChanFunc{
		0: map[string]ListenChanFunc{},                  //Ping
		1: map[string]ListenChanFunc{},                  //统计
		2: map[string]ListenChanFunc{},                  //计算
		3: map[string]ListenChanFunc{"clear": &Clear{}}, //Clear
		// 3: Calculation,
		// "upheader",   //3:更新统计项
		// "gethearder", //4:查询统计项
		// "adddata",    //5:添加统计数据
	}
	//calculation
	handle := &Calculation{}
	stat := &Statistics{}
	for _, tag := range models.TagList {
		handleFuncs[2][tag] = handle
		handleFuncs[1][tag] = stat
		//handle.initchan()
		utils.InitRedis(tag)
		utils.MgoInit(tag)
	}
	//每天处理一次 rds key
	go ClearRedisKey(models.TagList[0])

	ConnRespChannel = make(chan *connResp, 100)

	//启动redis 监听
	if utils.StartQueueListen {
		utils.InitMQ(utils.QUEUE_HOST)
		go func() {
			ch, err := utils.GetChannel(utils.QUEUE_NAME, utils.QUEUE_EXCHANGE)
			if err != nil {
				log.ERROR.Printf("Start Queue Listen Err:%v", err)
				return
			}

			pch, err := utils.Consume(ch, &utils.Queueinfo{utils.QUEUE_NAME, utils.QUEUE_EXCHANGE}, "a")
			if err != nil {
				log.ERROR.Printf("Start Queue Listen Err:%v", err)
				return
			}
			for t := range pch {
				go func() {
					data := &BodyData{}
					if err := utils.JsonDecode(t.Body, data); err != nil {
						log.ERROR.Printf("Json Decode Queue Body Err:%v", err)
						return
					}
					if fs, ok := handleFuncs[data.Op]; ok {
						if f, ok := fs[data.Tag]; ok {
							f.reader(data)
						} else {
							log.WARN.Printf("Queue data tag not found ,op:%s,tag:%s", data.Op, data.Tag)
						}
					} else {
						log.WARN.Printf("Queue data op not found ,op:%s", data.Op)
					}
					t.Ack(false)
				}()
			}
		}()
	}
}
