package utils

import (
	"github.com/panjjo/flysnow/utils/btree"
	"github.com/sirupsen/logrus"
	"os"
	"path/filepath"
	"sync"
)

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
	fb.rwmutex.RLock()
	defer fb.rwmutex.RUnlock()
	if res := fb.b.Get(FilterBtreeItem{Key: key}); res != nil {
		return res.(FilterBtreeItem)
	}
	return FilterBtreeItem{}
}
func (fb *FilterBtree) Set(item FilterBtreeItem) {
	fb.rwmutex.Lock()
	fb.b.ReplaceOrInsert(item)
	fb.rwmutex.Unlock()
}
func (fb *FilterBtree) GetSet(item FilterBtreeItem) (resu FilterBtreeItem, update bool) {
	fb.rwmutex.Lock()
	item.Offset = fb.offset
	if res := fb.b.ReplaceOrInsert(item); res != nil {
		resu = res.(FilterBtreeItem)
		update = true
		item.Offset = resu.Offset
	}
	if fb.save {
		fb.writeFile(item)
	}
	fb.rwmutex.Unlock()
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
		b := append([]byte(item.Key), Int64ToBytes(item.T)...)
		b = append(IntToBytes(len(b)), b...)
		if item.Offset < fb.offset {
			offset = int64(item.Offset)
		} else {
			offset = int64(fb.offset)
			fb.offset += len(b)
		}
		if _, err := fb.f.WriteAt(b, offset); err != nil {
			logrus.Errorf("fsbtree write to file err:" + err.Error())
		}
	}
}

func NewBTree(persistence bool, name string) *FilterBtree {
	fs := &FilterBtree{btree.New(32), 0, persistence, nil, sync.RWMutex{}}
	if persistence {
		if !FileOrPathIsExist(BTreeFilesPath) {
			if err := CreatePathAll(BTreeFilesPath); err != nil {
				logrus.Fatalf("init create btree file path error,path:%s,err:%v", BTreeFilesPath, err)
			}
		}
		f, err := os.OpenFile(filepath.Join(BTreeFilesPath, name), os.O_RDWR|os.O_CREATE, os.ModePerm)
		if err != nil {
			logrus.Fatalf("init btree file error,file:%s,err:%v", name, err)
		}
		fs.f = f
		fs.initBtreeByFile()
	}
	return fs
}
