package main

import (
	"fmt"
	"time"

	"github.com/panjjo/go-flysnow"
)

func Str2Sec(layout, str string) int64 {
	tm2, _ := time.ParseInLocation(layout, str, time.Local)
	return tm2.Unix()
}

func main() {
	conn, err := flysnow.NewConnection("192.168.1.90", 22258)
	fmt.Println(err)
	// fmt.Println(conn.Send("apis", map[string]interface{}{"api": "test", "appkey": "123", "code": "0", "s_time": Str2Sec("2006-01-02 15", "2018-11-09 10")}))
	query := &flysnow.StatQuery{
		Term:  "all",
		Group: []string{"s_time"},
		Span:  1,
		Spand: "d",
	}
	res, err := conn.Stat("apis", query)
	fmt.Println(res.Code, string(res.Data), err)
}
