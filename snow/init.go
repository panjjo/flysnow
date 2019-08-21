package snow

import (
	"github.com/panjf2000/ants"
	"github.com/panjjo/flysnow/utils"
	"github.com/robfig/cron"
	"github.com/sirupsen/logrus"
	"sync"
)

var rotatePool *ants.Pool
var rotateKeyFilter *utils.FilterBtree

func Init() {
	var err error
	rotatePool, err = ants.NewPool(10)
	if err != nil {
		logrus.Fatal("init rotatepool fail" + err.Error())
	}
	cron := cron.New()
	// 10s调节一次归档work并发数
	cron.AddFunc("@every 10s", ad)
	// 每天检查一次需要归档的元数据
	cron.AddFunc(utils.Config.AutoRotate, autoRotate)
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
	logrus.Debugf("rotate pool,cap:%d,free:%d,running:%d", cap, free, running)
	if free == 0 {
		// 空闲=0
		if cap > utils.Config.MaxRotateNums {
			// 容量超过最大数量,报警不加
			logrus.Warnf("rotate pool,cap > %d,cap:%d", utils.Config.MaxRotateNums, cap)
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
