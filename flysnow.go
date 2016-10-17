package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"net"
	"strconv"
	"sync"
	"time"
)

var redispool *redis.Pool
var errmap = map[int]error{
	1002: errors.New("tag error"),
	1003: errors.New("sys time out"),
	1001: errors.New("op error"),
	0:    nil,
}

func transerr(code int) error {
	if v, ok := errmap[code]; ok {
		return v
	}
	return errors.New("sys error")
}

const (
	ConstHeader = "^"
	EndId       = "$"
	CodeLength  = 4
	TagLength   = 4
	DataLength  = 4
)

type Resp struct {
	Code int
	Data []byte
}
type FlySnowConn struct {
	Addr string
	Port int
	Tag  string
	conn net.Conn
	b    []byte
}
type StatQuery struct {
	Term         string
	Index        map[string]interface{}
	STime, ETime int64
	Group        []string
	Limit, Skip  int
	sort         []interface{}
	span         int64
	spand        string
}

func NewStatQuery() *StatQuery {
	return &StatQuery{}
}
func (sq *StatQuery) SetSort(key string, asc bool) {
	sq.sort = []interface{}{key, asc}
}

//d=[s,h,d,m,y]
func (sq *StatQuery) SetSpan(t int64, d string) {
	switch d {
	case "s", "d", "h", "m", "y":
		sq.spand = d
		sq.span = t
	default:
	}
}

var b []byte

//链接错误重连
func (f *FlySnowConn) Reconnection() (err error) {
	f.conn, err = createconn(f.Addr, f.Port)
	return err
}

//不需要返回的发送
func (f *FlySnowConn) SendWithOutResp(data interface{}) error {
	_, err := f.sender(data, 2, 0)
	return err
}

//正常发送
func (f *FlySnowConn) Send(data interface{}) (result *Resp, err error) {
	_, err = f.sender(data, 2, 1)
	if err != nil {
		return nil, err
	}
	return f.Reader()
}

//统计查询
func (f *FlySnowConn) Stat(query *StatQuery) (result *Resp, err error) {
	_, err = f.sender(query, 1, 1)
	if err != nil {
		return nil, err
	}
	return f.Reader()
}

//读取返回数据
func (f *FlySnowConn) Reader() (result *Resp, err error) {
	tmpBuffer := make([]byte, 0)
	for {
		i, err := f.conn.Read(f.b)
		if err != nil {
			return nil, err
		}
		result, tmpBuffer = f.UnPacket(append(tmpBuffer, f.b[:i]...))
		if result != nil && len(tmpBuffer) == 0 {
			return result, nil
		}
	}
}

func (f *FlySnowConn) sender(data interface{}, op int, resp int) (int, error) {
	return f.conn.Write(f.Packet(JsonEncode(data), op, resp))
}
func (f *FlySnowConn) Close() error {
	return f.conn.Close()
}

//数据包长度 = []byte(ConstHeader)+[]byte(op)+Len(tag)+[]byte(tag)+BodyDataLength+[]byte(body)+[]byte(resp)
//封包
func (f *FlySnowConn) Packet(message []byte, op int, resp int) []byte {
	result := []byte(ConstHeader)
	result = append(result, IntToBytes(op)...)
	result = append(result, IntToBytes(len([]byte(f.Tag)))...)
	result = append(result, []byte(f.Tag)...)
	result = append(result, IntToBytes(len(message))...)
	result = append(result, message...)
	result = append(result, IntToBytes(resp)...)
	return result
}
func (f *FlySnowConn) UnPacket(body []byte) (*Resp, []byte) {
	l := len(body)
	for i := 0; i < l; i++ {
		if string(body[:i]) == EndId {
			code := BytesToInt(body[i : i+CodeLength])
			i = i + CodeLength
			datalen := BytesToInt(body[i : i+DataLength])
			i = i + DataLength
			if l < i+datalen {
				return nil, body
			} else {
				return &Resp{Code: code, Data: body[i : i+datalen]}, body[i+datalen:]
			}
		}
	}
	return nil, body
}
func Connection(addr string, port int, tag string) (*FlySnowConn, error) {
	conn, err := createconn(addr, port)
	if err != nil {
		return nil, err
	}
	return &FlySnowConn{
		Addr: addr,
		Port: port,
		Tag:  tag,
		conn: conn,
		b:    make([]byte, 1024),
	}, nil
}

func createconn(addr string, port int) (net.Conn, error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp4", addr+fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, err
	}
	return net.DialTCP("tcp", nil, tcpAddr)
}

func JsonEncode(ob interface{}) []byte {
	var (
		b   []byte
		err error
	)
	if b, err = json.Marshal(ob); err != nil {
		b = []byte("")
	}
	return b
}

//整形转换成字节
func IntToBytes(n int) []byte {
	x := int32(n)

	bytesBuffer := bytes.NewBuffer([]byte{})
	binary.Write(bytesBuffer, binary.BigEndian, x)
	return bytesBuffer.Bytes()
}

//字节转换成整形
func BytesToInt(b []byte) int {
	bytesBuffer := bytes.NewBuffer(b)

	var x int32
	binary.Read(bytesBuffer, binary.BigEndian, &x)
	return int(x)
}
func main() {
	InitRedis()
	rdk := "fs_apis_counts"
	RedisDo("DEL", rdk)
	//for t := 0; t < 10; t++ {
	//var s int64
	//sn := time.Now().Unix()
	//for x := 0; x < 2; x++ {
	//go func() {
	//flysnow, _ := Connection("192.168.1.90", 22258, "apis")
	//for i := 0; i <= 50000; i++ {
	//fsres, err := flysnow.Send(map[string]interface{}{"api": "user.add", "code": fmt.Sprintf("%d", i%10), "appkey": "1001"})
	//if err != nil {
	//fmt.Println(err)
	//}
	//if fsres.Code != 0 {
	//fmt.Println(fsres.Code)
	//}
	//if s1 := time.Now().Unix(); s1 > s {
	//s = s1
	//}
	//}
	//flysnow.Close()
	//}()
	//}
	//time.Sleep(30 * time.Second)
	//fmt.Println("2*50000  t:", sn-s)
	//res, _ := RedisDo("HGETALL", rdk)
	//RedisDo("DEL", rdk)
	//ttt := res.([]interface{})
	//tsdd := map[string]interface{}{}
	//for ti := 0; ti < len(ttt); ti = ti + 2 {
	//tsdd[string(ttt[ti].([]byte))] = string(ttt[ti+1].([]byte))
	//}
	//fmt.Println("times:", t, "counts:", tsdd["total"])
	//}
	//for t := 0; t < 10; t++ {
	//var s int64
	//sn := time.Now().Unix()
	//for x := 0; x < 5; x++ {
	//go func() {
	//flysnow, _ := Connection("192.168.1.90", 22258, "apis")
	//for i := 0; i <= 20000; i++ {
	//fsres, err := flysnow.Send(map[string]interface{}{"api": "user.add", "code": fmt.Sprintf("%d", i%10), "appkey": "1001"})
	//if err != nil {
	//fmt.Println(err)
	//}
	//if fsres.Code != 0 {
	//fmt.Println(fsres.Code)
	//}
	//if s1 := time.Now().Unix(); s1 > s {
	//s = s1
	//}
	//}
	//flysnow.Close()
	//}()
	//}
	//time.Sleep(30 * time.Second)
	//fmt.Println("5*20000  t:", sn-s)
	//res, _ := RedisDo("HGETALL", rdk)
	//RedisDo("DEL", rdk)
	//ttt := res.([]interface{})
	//tsdd := map[string]interface{}{}
	//for ti := 0; ti < len(ttt); ti = ti + 2 {
	//tsdd[string(ttt[ti].([]byte))] = string(ttt[ti+1].([]byte))
	//}
	//fmt.Println("times:", t, "counts:", tsdd["total"])
	//}
	//for t := 0; t < 10; t++ {
	//var s int64
	//sn := time.Now().Unix()
	//for x := 0; x < 10; x++ {
	//go func() {
	//flysnow, _ := Connection("192.168.1.90", 22258, "apis")
	//for i := 0; i <= 10000; i++ {
	//fsres, err := flysnow.Send(map[string]interface{}{"api": "user.add", "code": fmt.Sprintf("%d", i%10), "appkey": "1001"})
	//if err != nil {
	//fmt.Println(err)
	//}
	//if fsres.Code != 0 {
	//fmt.Println(fsres.Code)
	//}
	//if s1 := time.Now().Unix(); s1 > s {
	//s = s1
	//}
	//}
	//flysnow.Close()
	//}()
	//}
	//time.Sleep(30 * time.Second)
	//fmt.Println("10*10000  t:", sn-s)
	//res, _ := RedisDo("HGETALL", rdk)
	//RedisDo("DEL", rdk)
	//ttt := res.([]interface{})
	//tsdd := map[string]interface{}{}
	//for ti := 0; ti < len(ttt); ti = ti + 2 {
	//tsdd[string(ttt[ti].([]byte))] = string(ttt[ti+1].([]byte))
	//}
	//fmt.Println("times:", t, "counts:", tsdd["total"])
	//}
	//for t := 0; t < 10; t++ {

	wg := sync.WaitGroup{}
	wg.Add(100000)
	sn := time.Now()
	for x := 0; x < 100; x++ {
		go func() {
			flysnow, _ := Connection("192.168.1.90", 22258, "apis")
			for i := 0; i < 1000; i++ {
				fsres, err := flysnow.Send(map[string]interface{}{"api": "user.add", "code": fmt.Sprintf("%d", i%10), "appkey": "1001"})
				if err != nil {
					fmt.Println(err)
				}
				if fsres.Code != 0 {
					fmt.Println(fsres.Code)
				}
				wg.Done()
			}
			flysnow.Close()
		}()
	}
	wg.Wait()
	fmt.Println("1*100000  t:", time.Since(sn))
	res, _ := RedisDo("HGETALL", rdk)
	RedisDo("DEL", rdk)
	ttt := res.([]interface{})
	tsdd := map[string]interface{}{}
	for ti := 0; ti < len(ttt); ti = ti + 2 {
		tsdd[string(ttt[ti].([]byte))] = string(ttt[ti+1].([]byte))
	}
	//}
}

/**
  生成redis连接池
*/
func InitRedis() {
	rdshost := "127.0.0.1:6379"
	//rdshost := "114.215.84.37:6379"
	count := "1"
	rdsmaxpool, _ := strconv.Atoi(count)
	redispool = newRedisPool(rdshost, rdsmaxpool)
}

func RedisDo(key string, args ...interface{}) (res interface{}, err error) {
	conn := redispool.Get()
	defer conn.Close()
	res, err = conn.Do(key, args...)
	return res, err
}

/**
  生成redis连接池
*/
func newRedisPool(server string, maxidle int) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     maxidle,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", server)
			if err != nil {
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}
