package main // import github.com/panjjo/flysnow

import (
	"github.com/panjjo/flysnow/fly"
	_ "github.com/panjjo/flysnow/tmp"
	_ "github.com/panjjo/flysnow/utils"
	"log"
	"net/http"
	_ "net/http/pprof"
)

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:7777", nil))
	}()
	fly.StartServer()

}
