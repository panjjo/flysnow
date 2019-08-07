package snow

import (
	"github.com/panjf2000/ants"
	"github.com/panjjo/flysnow/utils"
	"github.com/robfig/cron"
	"sync"
)

var rotatePool *ants.Pool
var log utils.LogS
var rotateKeyFilter *utils.FilterBtree

func Init() {
	log = utils.Log
	var err error
	rotatePool, err = ants.NewPool(10)
	if err != nil {
		log.Error("init rotatepool fail" + err.Error())
	}
	cron := cron.New()
	// 10s调节一次归档work并发数
	cron.AddFunc("@every 10s", ad)
	// 每天检查一次需要归档的元数据
	cron.AddFunc("0 0 3 * * *", autoRotate)
	// 每分钟检查一次归档work
	cron.AddFunc("@every 1m", lsrRotate)
	lsrRotate()
	rotateKeyLock = rotateKeys{k: map[string]int64{}, rw: &sync.RWMutex{}, ex: 5}
	rotateKeyFilter = utils.NewBTree(false, "")
	cron.Start()
}

var running, cap, free int

// 自动调节归档进程数
func ad() {
	running = rotatePool.Running()
	cap = rotatePool.Cap()
	free = rotatePool.Free()
	log.DEBUG.Printf("rotate pool,cap:%d,free:%d,running:%d", cap, free, running)
	if free == 0 {
		// 空闲=0
		if cap > 50 {
			// 容量超过50,报警不加
			log.WARN.Printf("rotate pool,cap > 50,cap:%d", cap)
		} else {
			// 加10个
			rotatePool.Tune(cap + 10)
		}
	}
	if free > 20 {
		// 空闲大于20 减10个
		rotatePool.Tune(cap - 10)
	}
}
