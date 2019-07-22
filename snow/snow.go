package snow

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"github.com/panjjo/flysnow/models"
	"github.com/panjjo/flysnow/utils"
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

var HyperLogLogList []string
var lastHyperLogLog string

// 每一个小时生成一个hyperloglog 最多保留5个hyperloglog ,新生成的那个merge前两个小时的数据
func newHyperLogLog() {
	conn := utils.NewRedisConn(models.TagList[0])
	defer conn.Close()
	key := "hyperloglog_" + utils.RandomTimeString()
	conn.Dos("PFADD", key)
	conn.Dos("EXPIRE", key, 5*60*60)
	lastHyperLogLog = key
	switch len(HyperLogLogList) {
	case 1:
		conn.Dos("PFMERGE", key, HyperLogLogList[0])
	case 2:
		conn.Dos("PFMERGE", key, HyperLogLogList[0], HyperLogLogList[1])
	case 3:
		conn.Dos("PFMERGE", key, HyperLogLogList[1], HyperLogLogList[2])
		HyperLogLogList = HyperLogLogList[1:]
	}
	HyperLogLogList = append(HyperLogLogList, key)

}

func CheckNeedRotate(key string, t int64, snow models.Snow) bool {
	endtime := utils.DurationMap[snow.InterValDuration](t, snow.Interval)
	hllkey := fmt.Sprint(key, endtime)
	c := utils.NewRedisConn(models.TagList[0])
	defer c.Close()
	result, err := c.Dos("PFADD", lastHyperLogLog, hllkey)
	if err != nil || result == nil {
		// 失败，返回true 需要check
		return true
	}
	if i, e := redis.Int(result, err); i == 0 && e == nil {
		return false
	}
	return true
}

func NeedRotate(snowsys *SnowSys, snow models.Snow) {

	snowlock.l.Lock()
	defer snowlock.l.Unlock()
	if snowsys.RedisConn == nil {
		snowsys.RedisConn = utils.NewRedisConn(snowsys.Tag)
		defer snowsys.RedisConn.Close()
	}

	now := snowsys.Now
	b, _ := snowsys.RedisConn.Dos("HGET", snowsys.Key, "e_time")
	if b != nil {
		endt, _ := strconv.ParseInt(string(b.([]byte)), 10, 64)
		if endt < now || now <= utils.DurationMap[snow.InterValDuration+"l"](endt, snow.Interval) {
			// 正常rotate 新来数据时间>redis的结束时间 生成新的一条
			// 旧数据rotate  新来数据时间<= redis的开始时间 讲现有数据rotate ,生成老数据对应的redis数据
			// 对于数据造成的同个snow有多条数据，在mongo rotate是进行合并
			rotateKey := "rotate_" + utils.RandomTimeString()
			_, err := snowsys.RedisConn.Dos("RENAME", snowsys.Key, rotateKey)
			if err != nil {
				log.ERROR.Printf("rotate rename fail,%s->%s,err:%v", snowsys.Key, rotateKey, err)
			}
			snowsys.RedisConn.Dos("HMSET", rotateKey, "key", snowsys.Key, "tag", snowsys.Tag, "term", snowsys.Term)
			// redis 队列存储需要归档的key，队列为左入
			snowsys.RedisConn.Dos("LPUSH", ROTATEKEYS, rotateKey)
			log.DEBUG.Printf("need rotate key:%s,rename:%s", snowsys.Key, rotateKey)
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
