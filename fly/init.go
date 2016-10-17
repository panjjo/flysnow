package fly

import (
	"errors"
	instance "flysnow/tmp"
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

	Err = &EventErr{}

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
	for _, tag := range instance.TagList {
		handle := &Calculation{}
		handleFuncs[2][tag] = handle
		//handle.initchan()
		utils.InitRedis(tag)
		utils.MgoInit(tag)
	}
	//stat
	stat := &Statistics{}
	for _, tag := range instance.TagList {
		handleFuncs[1][tag] = stat
	}

	ConnRespChannel = make(chan *connResp, 10)
}

var (
	err error
	Err *EventErr
)

const (
	ErrOpId           = 1001 //数据opid错误
	ErrMethodNotFount = 1002 //Tag不存在
	TimeOut           = 1003 //堵塞
)

var ErrMsgMap = map[int]string{
	1001: "op error",
	1002: "tag error",
	1003: "sys time out",
}

type EventErr struct {
	Code int
	Msg  string
	Err  error
}

func (e *EventErr) SetErrCode(c int) {
	e.Code = c
}
func (e *EventErr) SetErrMsg(msg string) {
	e.Msg = msg
}
func (e *EventErr) Pack() []byte {
	m := []byte(e.Msg)
	if e.Err != nil {
		m = append(m, []byte(e.Err.Error())...)
	}
	return m
}

func ErrNew(s string) error {
	return errors.New(s)
}
