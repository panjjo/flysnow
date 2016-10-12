package main

import (
	"flysnow/utils"
	"fmt"
	"log"
	"net/http"
)

func main() {
	utils.FSConfig.SetMod("sys")
	if utils.FSConfig.IntDefault("web", 0) == 1 {
		port := utils.FSConfig.StringDefault("web.port", "22259")
		http.HandleFunc("/", defaultHandler)
		http.HandleFunc("/configs", configHandler)
		log.Fatal(http.ListenAndServe(":"+port, nil))
	}
}
func defaultHandler(resp http.ResponseWriter, req *http.Request) {
	resp.Write([]byte("hello default"))
}
func configHandler(resp http.ResponseWriter, req *http.Request) {
	fmt.Println(req.PostForm)
	fmt.Println(req.Form)
	fmt.Println(req.Method)
	fmt.Println(req.ContentLength)
	fmt.Println(req.URL)
	fmt.Println((req.URL.Query()))
	resp.Write([]byte("hello config"))
}
