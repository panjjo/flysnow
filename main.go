package main // import github.com/panjjo/flysnow

import (
	"github.com/panjjo/flysnow/fly"
	"github.com/panjjo/flysnow/tmp"
	"github.com/panjjo/flysnow/utils"
	"log"
	"net/http"
	_ "net/http/pprof"
)

func main() {
	utils.Init()
	tmp.Init()
	go func() {
		log.Println(http.ListenAndServe("localhost:7777", nil))
	}()
	fly.StartServer()

}
