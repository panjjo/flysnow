package fly

import (
	"encoding/hex"
	"github.com/panjjo/flysnow/models"
	"github.com/panjjo/flysnow/utils"
	"github.com/sirupsen/logrus"
	"math/rand"
	"net"
	"sync"
	"time"
)

var (
	ConnMaps        ConnMapStruct
	ConnRespChannel chan *connResp
)

type ConnMapStruct struct {
	m       map[string]*ConnStruct
	rwmutex sync.RWMutex
}

func (s *ConnMapStruct) Get(key string) (value *ConnStruct, ok bool) {
	s.rwmutex.Lock()
	defer s.rwmutex.Unlock()
	value, ok = s.m[key]
	return
}
func (s *ConnMapStruct) Put(key string, value *ConnStruct) bool {
	s.rwmutex.RLock()
	defer s.rwmutex.RUnlock()
	s.m[key] = value
	return true
}
func (s *ConnMapStruct) Len() int {
	return len(s.m)
}
func (s *ConnMapStruct) Remove(key string) bool {
	_, ok := s.Get(key)
	s.rwmutex.Lock()
	defer s.rwmutex.Unlock()
	if ok {
		delete(s.m, key)
		return true
	}
	return false
}

type ConnStruct struct {
	conn net.Conn
	// reader chan []byte
	connid string
}
type connResp struct {
	connid string
	code   int
	body   interface{}
}

func ConnWrite() {
	for {
		select {
		case connresp := <-ConnRespChannel:
			if v, ok := ConnMaps.Get(connresp.connid); ok {
				v.conn.Write(RespPacket(connresp.code, connresp.body))
			}

		}
	}
}

// byte(code)+len(body)+byte(body)
func RespPacket(code int, body interface{}) []byte {
	result := []byte(endId)
	result = append(result, utils.IntToBytes(code)...)
	if code != 0 {
		if v, ok := models.ErrMsgMap[code]; ok {
			body = v
		}
	}
	b := utils.JsonEncode(body, false)
	result = append(result, utils.IntToBytes(len(b))...)
	result = append(result, b...)
	return result
}

func StartServer() {
	Init()
	netListen, err := net.Listen("tcp", utils.Config.Listen)
	if err != nil {
		logrus.Fatal(err)
	}
	defer netListen.Close()
	// TODO:Check auth
	go ConnWrite()

	logrus.Infof("Server Start Succ,Listen:%v", utils.Config.Listen)
	buffer := make([]byte, 4)

	for {
		if _, err := rand.Read(buffer); err != nil {
			logrus.Fatal(err.Error())
		}

		connid := hex.EncodeToString(buffer)
		conn, err := netListen.Accept()
		if err != nil {
			continue
		}

		expconn := &ConnStruct{
			conn:   conn,
			connid: connid,
		}
		ConnMaps.Put(connid, expconn)
		logrus.Infof("new connect from:%v,connid:%v,connect_num(%d)", conn.RemoteAddr().String(), connid, ConnMaps.Len())
		go handleConnection(expconn)
	}
}

func handleConnection(expconn *ConnStruct) {
	defer func() { ConnMaps.Remove(expconn.connid) }()

	// 声明一个临时缓冲区，用来存储被截断的数据
	tmpBuffer := make([]byte, 0)

	// go reader(expconn)
	buffer := make([]byte, 1024)
	for {
		n, err := expconn.conn.Read(buffer)
		if err != nil {
			logrus.Warnf(" connection :%v ,error: %v", expconn.conn.RemoteAddr().String(), err)
			return
		}
		tmpBuffer = Unpack(append(tmpBuffer, buffer[:n]...), expconn)
	}
}

const (
	startId    = "^"
	endId      = "$"
	opLength   = 4
	tagLength  = 4
	bodyLength = 4
	RespLength = 4
)

var startIdLength = len([]byte("^"))
var minlen = startIdLength + opLength + tagLength + bodyLength + RespLength

// var minlen = startIdLength + typeLength + opLength + tagLength + bodyLength+RespLength

// 数据包长度 = []byte(statId)+typeLength+opLength+tagLength+[]byte(tag)+bodyDataLength+[]byte(body)+[]byte(resp)

type BodyData struct {
	Op       int
	Body     []byte
	Connid   string
	Tag      string
	NeedResp int
}

// 解包
func Unpack(buffer []byte, conn *ConnStruct) []byte {
	// 数据包最小字节长度

	length := len(buffer)

	var i, cursor int
	// 查找起始位置
	for i = 0; i < length; i = i + 1 {
		cursor = i
		// 判断buffer长度,小于最小长度,认为不完整数据
		if length < cursor+minlen {
			break
		}
		// 找到起始位,并读取数据
		if string(buffer[cursor:cursor+startIdLength]) == startId {
			cursor += startIdLength
			// //读取type
			// dtype := utils.BytesToInt(buffer[cursor : cursor+typeLength])
			// cursor += typeLength
			// op
			op := utils.BytesToInt(buffer[cursor : cursor+opLength])
			cursor += opLength
			// tag
			taglen := utils.BytesToInt(buffer[cursor : cursor+tagLength])
			// buffer 长度小于数据包应该长度,数据没读取完整
			if length < taglen+minlen {
				break
			}
			cursor += tagLength
			tagdata := buffer[cursor : cursor+taglen]
			cursor += taglen
			// body
			if length < cursor+bodyLength {
				break
			}
			bodylen := utils.BytesToInt(buffer[cursor : cursor+bodyLength])
			cursor += bodyLength
			cursor += bodylen
			// buffer 长度小于数据包应该长度,数据没读取完整
			if length < cursor+RespLength {
				break
			}
			body := buffer[cursor-bodylen : cursor]
			cursor += RespLength
			if v, ok := handleFuncs[op]; !ok {
				ConnRespChannel <- &connResp{conn.connid, models.ErrOpId, nil}
			} else {
				rand.Seed(time.Now().UnixNano())
				// check heardbeat
				if op == 0 {
					ConnRespChannel <- &connResp{conn.connid, 0, nil}
				} else if op == 3 {
					go v["clear"].reader(&BodyData{
						Op:       op,
						Body:     body,
						Connid:   conn.connid,
						Tag:      string(tagdata),
						NeedResp: utils.BytesToInt(buffer[cursor-RespLength : cursor]),
					})
				} else {
					if cal, ok := v[string(tagdata)]; ok {
						// check heardbeat
						go cal.reader(&BodyData{
							Op:       op,
							Body:     body,
							Connid:   conn.connid,
							Tag:      string(tagdata),
							NeedResp: utils.BytesToInt(buffer[cursor-RespLength : cursor]),
						})
					} else {
						ConnRespChannel <- &connResp{conn.connid, models.ErrMethodNotFount, nil}
					}
				}
			}
		}
		i = cursor - 1

	}
	if i == length {
		return make([]byte, 0)
	}
	return buffer[i:]
}
