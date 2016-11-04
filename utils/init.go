package utils

import (
	"code.google.com/p/log4go"
	"errors"
	"flysnow/utils/btree"
	"os"
	"time"
)

var Log LogS
var PWD string
var FSBtree *FilterBtree

type LogS struct {
	log4go.Logger
}
type FilterBtreeItem struct {
	Key string
	T   int64
}

func (fbi FilterBtreeItem) Less(b btree.Item) bool {
	return fbi.Key < b.(FilterBtreeItem).Key
}

type FilterBtree struct {
	*btree.BTree
}

func (fb *FilterBtree) Get(key string) FilterBtreeItem {
	return fb.Get(key)
}
func (fb *FilterBtree) GetSet(item FilterBtreeItem) (resu FilterBtreeItem) {
	if res := fb.ReplaceOrInsert(item); res != nil {
		resu = res.(FilterBtreeItem)
	}
	return resu
}

func (l LogS) ERROR(s string) {
	l.Error(s)
	time.Sleep(1 * time.Second)
	os.Exit(1)
}
func (l LogS) NewErr(s string) error {
	return errors.New(s)
}

func init() {
	PWD, _ = os.Getwd()
	FSConfig = Config{}
	FSConfig.InitConfig(PWD + "/config/base.conf")
	config := FSConfig.StringDefault("logger.path", "config/logger.xml")
	Log = LogS{make(log4go.Logger)}
	Log.LoadConfiguration(config)
	FSBtree = &FilterBtree{btree.NewBtree(32)}
}
