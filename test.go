package main

import (
	"fmt"
	"gopkg.in/mgo.v2/bson"
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
		Term:  "stock",
		Group: []string{"s_time"},
		STime:1547654400,
		ETime:1548950400,
		Index:bson.M{"commodityid":"201901261311260","shopid":"201901251409100","stockid":"201901261311263","type":"1"},
		Span:  1,
		Spand: "d",
		Limit:20,
		Skip:0,
		Sort:[]interface{}{"s_time",true},
	}
	res, err := conn.Stat("stock", query)
	fmt.Println(res.Code, string(res.Data), err)
}
//receive connid:3d213570, op:1,tag:stock,data:{\"Term\":\"stock\",\"DataQuery\":{},\"Index\":{\"commodityid\":\"201901261311260\"," +
//	"\"shopid\":\"201901251409100\",\"stockid\":\"201901261311263\",\"type\":1},\"STime\":1547654400,\"ETime\":1548950400,\"Group\":[\"s_time\"]," +
//	"\"Limit\":20,\"Skip\":0,\"Sort\":[\"s_time\",true],\"Span\":1,\"Spand\":\"d\"}\n","stream":"stdout","time":"2019-01-31T07:25:01.209193205Z"}