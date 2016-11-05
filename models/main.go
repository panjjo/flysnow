package models

import (
	"errors"
)

var (
	err error
	Err *EventErr
)
var TagList []string

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
	ErrOpId           = 1001 //数据opid错误
	ErrMethodNotFount = 1002 //Tag不存在
	ErrTimeOut        = 1003 //堵塞
	ErrData           = 2001 //数据格式错误
)

var ErrMsgMap = map[int]string{
	1001: "op error",
	1002: "tag error",
	1003: "sys time out",
	2001: "data json decode err",
}

const (
	MongoDT = "FS"
	RedisKT = "fs"
)

type RANGELIST []struct {
	Key   interface{} `json:"key"`
	Value interface{} `json:"value"`
}

type TermConfig struct {
	Name   string
	Key    []string
	IsSnow bool
	Snow   []Snow
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
