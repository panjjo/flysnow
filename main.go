package main // import github.com/panjjo/flysnow

import (
	"github.com/panjjo/flysnow/fly"
	"github.com/panjjo/flysnow/tmp"
	"github.com/panjjo/flysnow/utils"
	"github.com/sirupsen/logrus"
	"net/http"
	_ "net/http/pprof"
)

func main() {
	logrus.SetLevel(logrus.DebugLevel)
	utils.LoacConfig()
	tmp.Init()
	go func() {
		logrus.Println(http.ListenAndServe("localhost:7777", nil))
	}()
	fly.StartServer()

}
