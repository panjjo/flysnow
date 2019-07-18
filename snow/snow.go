package snow

import (
	"github.com/panjjo/flysnow/models"
	"github.com/panjjo/flysnow/utils"
	"gopkg.in/mgo.v2/bson"
	"strconv"
	"sync"
)

type SnowSys struct {
	*utils.SnowKey
	RedisConn *utils.RedisConn
	Tag, Term string
	Now       int64
	SpKey     map[string]string // 特殊key处理
}

var snowlock rwmutex
var ROTATEKEYS = "ROTATEKEYS"

type rwmutex struct {
	l *sync.Mutex
}

func init() {
	snowlock = rwmutex{l: new(sync.Mutex)}
}

type SnowData struct {
	Key   string                   `json:"s_key" bson:"s_key"`
	STime int64                    `json:"s_time" bson:"s_time"`
	ETime int64                    `json:"e_time" bson:"e_time"`
	Data  []map[string]interface{} `json:"data" bson:"data"`
	Index map[string]interface{}
	Term  string
	Tag   string
	Query bson.M `bson:"-"`
}


func NeedRotate(snowsys *SnowSys, snow models.Snow) {

	snowlock.l.Lock()
	defer snowlock.l.Unlock()

	now := snowsys.Now
	b, _ := snowsys.RedisConn.Dos("HGET", snowsys.Key, "e_time")
	if b != nil {
		endt, _ := strconv.ParseInt(string(b.([]byte)), 10, 64)
		if endt < now || now <= utils.DurationMap[snow.InterValDuration+"l"](endt, snow.Interval) {
			// 正常rotate 新来数据时间>redis的结束时间 生成新的一条
			// 旧数据rotate  新来数据时间<= redis的开始时间 讲现有数据rotate ,生成老数据对应的redis数据
			// 对于数据造成的同个snow有多条数据，在mongo rotate是进行合并
			rotateKey := "rotate_" + utils.RandomTimeString()
			snowsys.RedisConn.Dos("RENAME", snowsys.Key, rotateKey)
			snowsys.RedisConn.Dos("HMSET", rotateKey, "key", snowsys.Key, "tag", snowsys.Tag, "term", snowsys.Term)
			// redis 队列存储需要归档的key，队列为左入
			snowsys.RedisConn.Dos("LPUSH", ROTATEKEYS, rotateKey)
			log.DEBUG.Printf("start rotate key:%s,rename", snowsys.Key)
			if !snowsys.SnowKey.KeyCheck {
				// 非全局自检 新增一个key
				end := utils.DurationMap[snow.InterValDuration](now, snow.Interval)
				start := utils.DurationMap[snow.InterValDuration+"l"](end, snow.Interval)
				snowsys.RedisConn.Dos("HMSET", snowsys.Key, "s_time", start, "e_time", end)
				log.DEBUG.Printf("new key:%s,s:%d,e:%d", snowsys.Key, start, end)
			}
		} else {
			return
		}
	} else if !snowsys.SnowKey.KeyCheck {
		// redis中没有数据，生成新的一条
		// KeyCheck=true 标识每天的自动全局rotate
		end := utils.DurationMap[snow.InterValDuration](now, snow.Interval)
		start := utils.DurationMap[snow.InterValDuration+"l"](end, snow.Interval)
		snowsys.RedisConn.Dos("HMSET", snowsys.Key, "s_time", start, "e_time", end)
		log.DEBUG.Printf("new key:%s,s:%d,e:%d", snowsys.Key, start, end)
	}
	return
}





// 处理redis的特殊key
func redisSpKey(data map[string]interface{}, snow *SnowSys) {
	commands := []*utils.RdsSendStruct{}
	for k, _ := range data {
		if tpe, ok := snow.SpKey[k]; ok {
			if comms := utils.RDSSpKeyFuncs(tpe, k, data, snow.SnowKey); len(comms.Commands) > 0 {
				commands = append(commands, &utils.RdsSendStruct{comms.Key, comms.Commands})
			}
		}
	}
	if len(commands) > 0 {
		utils.RdsBatchCommands(snow.Tag, commands)
	}
}


