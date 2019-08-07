package utils

import (
	// 	"code.google.com/p/log4go"
	"errors"
	"fmt"
	"github.com/panjjo/flysnow/models"
	"github.com/panjjo/flysnow/utils/btree"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/panjjo/log4go"
)

var Log LogS
var PWD string

var BTreeFilesPath = "./btreefiles"

type FilterBtreeItem struct {
	Key    string
	T      int64
	Offset int
}

func (fbi FilterBtreeItem) Less(b btree.Item) bool {
	return fbi.Key < b.(FilterBtreeItem).Key
}
func (fbi FilterBtreeItem) Trans(b btree.Item) btree.Item {
	fbi.Offset = b.(FilterBtreeItem).Offset
	return fbi
}

type FilterBtree struct {
	b       *btree.BTree
	offset  int
	save    bool
	f       *os.File
	rwmutex sync.RWMutex
}

func (fb *FilterBtree) Get(key string) FilterBtreeItem {
	if res := fb.b.Get(FilterBtreeItem{Key: key}); res != nil {
		return res.(FilterBtreeItem)
	}
	return FilterBtreeItem{}
}
func (fb *FilterBtree) Set(item FilterBtreeItem) {
	fb.b.ReplaceOrInsert(item)
}
func (fb *FilterBtree) GetSet(item FilterBtreeItem) (resu FilterBtreeItem, update bool) {
	item.Offset = fb.offset
	if res := fb.b.ReplaceOrInsert(item); res != nil {
		resu = res.(FilterBtreeItem)
		update = true
		item.Offset = resu.Offset
	}
	fb.writeFile(item)
	return
}
func (fb *FilterBtree) initBtreeByFile() {
	b := make([]byte, 1024)
	nocom := make([]byte, 0)
	while := true
	for while {
		if n, e := fb.f.Read(b); e == nil {
			// if n, e := fb.f.ReadAt(b, int64(fb.offset)); e == nil {
			if n != len(b) {
				while = false
			}
			nocom = append(nocom, b[:n]...)
			for {
				if len(nocom) < 4 {
					break
				}
				bodylen := BytesToInt(nocom[0:4])
				if len(nocom[4:]) < bodylen {
					break
				}
				fb.Set(FilterBtreeItem{Offset: fb.offset, Key: string(nocom[4 : bodylen-4]), T: BytesToInt64(nocom[bodylen-4 : bodylen+4])})
				fb.offset += bodylen + 4
				nocom = nocom[bodylen+4:]
			}

		} else {
			while = false
		}
	}
}

// int64->byte 8  int32->byte 4
func (fb *FilterBtree) writeFile(item FilterBtreeItem) {
	var offset int64
	if fb.save {
		fb.rwmutex.Lock()
		defer fb.rwmutex.Unlock()
		b := append([]byte(item.Key), Int64ToBytes(item.T)...)
		b = append(IntToBytes(len(b)), b...)
		if item.Offset < fb.offset {
			offset = int64(item.Offset)
		} else {
			offset = int64(fb.offset)
			fb.offset += len(b)
		}
		if _, err := fb.f.WriteAt(b, offset); err != nil {
			Log.ERROR.Printf("fsbtree write to file err:" + err.Error())
		}
	}
}

type LogS struct {
	*log4go.Logger
}

func (l *LogS) Error(s string) {
	l.ERROR.Print(s)
	time.Sleep(1 * time.Second)
	os.Exit(1)
}
func (l *LogS) NewErr(s string) error {
	return errors.New(s)
}

func Init() {
	PWD, _ = os.Getwd()
	FSConfig = Config{}
	FSConfig.InitConfig(PWD + "/config/base.conf")
	FSConfig.SetMod("sys")
	if tag := FSConfig.String("tag"); tag != "" {
		models.MongoDT = tag
		models.RedisKT = tag
	}
	Log = LogS{log4go.NewLogger(FSConfig.StringDefault("logger.level", "info"))}

	if FSConfig.IntDefault("queue", 0) == 1 {
		StartQueueListen = true
		QUEUE_HOST = FSConfig.StringDefault("queue.Host", "guest:guest@127.0.0.1:5672/flysnow")
		QUEUE_NAME = FSConfig.StringDefault("queue.Name", "flysnow")
		QUEUE_EXCHANGE = FSConfig.StringDefault("queue.Exchange", "direct.flysnow")
		QUEUE_EXCHANGETYPE = FSConfig.StringDefault("queue.ExchangeType", "direct")
	}
}

func NewBTree(persistence bool, name string) *FilterBtree {
	fs := &FilterBtree{btree.NewBtree(32), 0, persistence, nil, sync.RWMutex{}}
	if persistence {
		if !FileOrPathIsExist(BTreeFilesPath) {
			if err := CreatePathAll(BTreeFilesPath); err != nil {
				Log.Error(fmt.Sprintf("init create btree file path error,path:%s,err:%v", BTreeFilesPath, err))
			}
		}
		f, err := os.OpenFile(filepath.Join(BTreeFilesPath, name), os.O_RDWR|os.O_CREATE, os.ModePerm)
		if err != nil {
			Log.Error(fmt.Sprintf("init btree file error,file:%s,err:%v", name, err))
		}
		fs.f = f
		fs.initBtreeByFile()
	}
	return fs
}
