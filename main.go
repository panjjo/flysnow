package main

import (
	"flysnow/fly"
	_ "flysnow/tmp"
	_ "flysnow/utils"
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
