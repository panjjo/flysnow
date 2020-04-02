package main // import github.com/panjjo/flysnow

import (
	"net/http"
	_ "net/http/pprof"

	"github.com/panjjo/flysnow/fly"
	"github.com/panjjo/flysnow/tmp"
	"github.com/panjjo/flysnow/utils"
	"github.com/sirupsen/logrus"
)

func main() {
	logrus.SetLevel(logrus.DebugLevel)
	utils.LoacConfig()
	tmp.Init()
	go func() {
		logrus.Println(http.ListenAndServe(":7777", nil))
	}()
	fly.StartServer()

}
