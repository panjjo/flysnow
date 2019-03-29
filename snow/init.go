package snow

import (
	"github.com/panjf2000/ants"
	"github.com/panjjo/flysnow/utils"
	"time"
)

var rotatePool *ants.Pool
var log utils.LogS

func Init() {
	log = utils.Log
	var err error
	rotatePool, err = ants.NewPool(10)
	if err != nil {
		log.Error("init rotatepool fail" + err.Error())
	}
	go func() {
		var running, cap, free int
		for {
			running = rotatePool.Running()
			cap = rotatePool.Cap()
			free = rotatePool.Free()
			log.DEBUG.Printf("rotate pool,cap:%d,free:%d,running:%d", cap, free, running)
			if cap > 100 {
				// 容量超过100，报警不加
				log.WARN.Printf("rotate pool,cap > 100,cap:%d", cap)
			} else {
				if free == 0 {
					// 空闲=0，加10个
					rotatePool.Tune(cap + 10)
				}
				if free > 20 {
					// 空闲大于20 加10个
					rotatePool.Tune(cap - 10)
				}
			}
			time.Sleep(10 * time.Second)
		}
	}()

}
