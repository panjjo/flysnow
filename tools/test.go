package main

import (
	"fmt"
	"github.com/panjjo/go-flysnow"
	"sync"
	"time"
)

func main() {

	query()
	/*send()*/
}
func query() {
	conn, err := flysnow.NewConnection("192.168.1.90", 22258)
	fmt.Println(err)
	res, err := conn.Stat("shop", &flysnow.StatQuery{
		Term: "shop",
		Index: map[string]interface{}{
			"shopid": "1234",
		},
		Group: []string{"s_time"},
	})
	fmt.Println(err)
	fmt.Println(res.Code, string(res.Data))
}
func send() {
	wgp := sync.WaitGroup{}
	wgp.Add(1)
	no := time.Now()
	for x := 0; x < 1; x++ {
		go func() {
			conn, err := flysnow.NewConnection("192.168.1.90", 22258)
			fmt.Println(err)
			for i := 0; i < 100; i++ {
				conn.Send("shop", map[string]interface{}{
					"shopid":   "1234",
					"memberid": "abcd",
					"value":    1,
				})
				time.Sleep(1 * time.Second)
			}
			wgp.Done()
		}()
	}
	wgp.Wait()
	fmt.Println(time.Since(no))

}
