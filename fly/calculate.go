package fly

import (
	"flysnow/models"
	instance "flysnow/tmp"
	"sync"
)

type Calculation struct {
	channel chan *BodyData
}

func (c *Calculation) reader(d *BodyData) {

	if itf, ok := instance.InterfaceMap[d.Tag]; ok {
		go func() {
			wg := &sync.WaitGroup{}
			wg.Add(len(itf))
			for _, v := range itf {
				go func(v models.TermInterface) {
					v.Exec(d.Body)
					wg.Done()
				}(v)
			}
			wg.Wait()
			ConnRespChannel <- &connResp{d.Connid, 0, nil}
		}()
	} else {
		ConnRespChannel <- &connResp{d.Connid, ErrMethodNotFount, nil}
	}
}
