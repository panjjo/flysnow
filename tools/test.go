package main

import (
	"fmt"
	"github.com/panjjo/flysnow/utils"
	"github.com/panjjo/go-flysnow"
	"sync"
	"time"
)

func main() {

	query()
	// send()
}
func query() {
	conn, err := flysnow.NewConnection("192.168.1.90", 22258)
	fmt.Println(err)
	res, err := conn.Stat("apis", &flysnow.StatQuery{
		Term: "all",
		Index: map[string]interface{}{
			// "code":"1",
		},
		// Spand: "d",
		// Span:  1,
		// STime: 1563897600,
		// ETime: 1564329600,
		Group: []string{"code"},
	})
	fmt.Println(err)
	data := map[string]interface{}{}
	utils.JsonDecode(res.Data, &data)
	fmt.Println("code:", res.Code)
	fmt.Println("num:", data["num"])
	fmt.Println("total:", data["total"])
	for _, v := range data["list"].([]interface{}) {
		fmt.Println(v)
	}
}
func send() {
	wgp := sync.WaitGroup{}
	wgp.Add(10)
	no := time.Now()
	for x := 0; x < 10; x++ {
		go func(a int) {
			conn, err := flysnow.NewConnection("192.168.1.90", 22258)
			fmt.Println(err)
			for i := 0; i < 50000; i++ {
				conn.Send("apis", map[string]interface{}{
					"api":    "1234",
					"appkey": "abcd",
					"code":   fmt.Sprint(a),
				})
			}
			wgp.Done()
		}(x)
	}
	wgp.Wait()
	fmt.Println(time.Since(no))
}
