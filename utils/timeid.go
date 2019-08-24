package utils

import (
	"fmt"
	"time"
)

/**
timeid
*/
type U struct {
	prefix string
	c      chan int
	d      chan struct{}
}

func NewU(t int64, n int) *U {
	u := &U{
		prefix: time.Unix(t, 0).Format("060102150405"),
		c:      make(chan int, n),
		d:      make(chan struct{}),
	}
	u.start()
	return u
}
func (u *U) start() {
	go func() {
		i := 0
		for {
			select {
			case u.c <- i:
				i++
			case <-u.d:
				return
			}
		}
	}()
}
func (u *U) stop() {
	u.d <- struct{}{}
	close(u.c)
}

func (u *U) Next() string {
	return u.prefix + fmt.Sprintf("%d", <-u.c)
}

type TimeID struct {
	o *U
	c *U
	n *U

	l int
}

func NewTimeID(l int) *TimeID {
	return &TimeID{l: l}
}
func (u *TimeID) Start() error {
	go func() {
		t := time.NewTicker(time.Second)
		u.n = NewU(time.Now().Unix(), u.l)
		for {
			u.o = u.c
			u.c = u.n
			u.n = NewU(time.Now().Unix()+1, u.l)
			if u.o != nil {
				u.o.stop()
			}
			<-t.C
		}
	}()
	return nil
}
func (u *TimeID) Next() string {
	return u.c.Next()
}

var _systemID *TimeID

func init() {
	_systemID = NewTimeID(10)
	_systemID.Start()
}

func RandomTimeString() string {
	return _systemID.Next()
}

