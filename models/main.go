package models

import (
	"errors"
)

var (
	err error
	Err *EventErr
)
var TagList []string
var TermConfigMap map[string]map[string]*TermConfig

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

const (
	ErrOpId           = 1001 // 数据opid错误
	ErrMethodNotFount = 1002 // Tag不存在
	ErrTimeOut        = 1003 // 堵塞
	ErrData           = 2001 // 数据格式错误
	ErrClear          = 2002 // 清理失败
)

var ErrMsgMap = map[int]string{
	1001: "op error",
	1002: "tag error",
	1003: "sys time out",
	2001: "data json decode err",
}

const (
	SPKEYLAST = "last"
	SPKEYAVG  = "avg"
)

var SpKeyMap = map[string]string{
	SPKEYLAST: "时间段内保留最后一个值",
	SPKEYAVG:  "时间段内平均值",
}
// mongo数据库前缀，可通过配置文件配置
var MongoDT = "FS"
// redis数据前缀,可通过配置文件配置
var RedisKT = "fs"
// mongo 数据索引表前缀
var MongoIndex = ""
// mongo 数据元素表前缀
var MongoOBJ = "obj"

type LISTKV []struct {
	Key   interface{} `json:"key"`
	Value interface{} `json:"value"`
}

type TermConfig struct {
	Name   string
	Key    []string
	IsSnow bool
	Snow   []Snow
	SpKey  map[string]string
}

type Json struct {
	Name    string      `json:"name"`
	Reqdata interface{} `json:"reqdata"`
	Filter  []FSFilter  `json:"filter"`
	Term    []Term      `json:"terms"`
}
type FSFilter struct {
	Name     string
	OffSet   int64
	Whence   int
	Duration string
	Persistence  bool `json:"persistence"`
}
type Term struct {
	Name   string
	Key    []string
	Execs  []Exec `json:"execs"`
	Snow   []Snow `json:"snow"`
	IsSnow bool
}
type Snow struct {
	Interval         int64
	InterValDuration string
	Timeout          int64
	TimeoutDuration  string
}
type Exec struct {
	Filter []interface{}
	Do     [][]interface{}
}
