package models

const (
	MongoDT = "FS"
	RedisKT = "fs"
)

var TermConfigMap map[string]map[string]*TermConfig

type TermConfig struct {
	Name   string
	Key    []string
	IsSnow bool
	Snow   []Snow
}

type TermInterface interface {
	Exec(data []byte)
	SetConfig(c *TermConfig)
}
type ReqData interface {
}
type ResData interface {
}

type Json struct {
	Name    string      `json:"name"`
	Reqdata interface{} `json:"reqdata"`
	Term    []Term      `json:"terms"`
}
type Term struct {
	Name   string
	Key    []string
	Result interface{}
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
	Filter string
	Do     []ExecDo
}
type ExecDo struct {
	Name  string
	Value interface{}
	Op    string
}
