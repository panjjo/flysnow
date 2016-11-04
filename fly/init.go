package fly

import (
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

func init() {
	log = utils.Log
	runtime.GOMAXPROCS(runtime.NumCPU())

	ConnMaps = ConnMapStruct{m: map[string]*ConnStruct{}}

	handleFuncs = map[int]map[string]ListenChanFunc{
		1: map[string]ListenChanFunc{}, //统计
		2: map[string]ListenChanFunc{}, //计算
		// 3: Calculation,
		// "upheader",   //3:更新统计项
		// "gethearder", //4:查询统计项
		// "adddata",    //5:添加统计数据
	}
	//calculation
	for _, tag := range []string{"apis"} {
		handle := &Calculation{}
		handleFuncs[2][tag] = handle
		//handle.initchan()
		utils.InitRedis(tag)
		utils.MgoInit(tag)
	}
	////stat
	//stat := &Statistics{}
	//for _, tag := range instance.TagList {
	//handleFuncs[1][tag] = stat
	//}

	ConnRespChannel = make(chan *connResp, 100)
}
