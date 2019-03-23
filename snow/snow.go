package snow

import (
	"fmt"
	"github.com/panjjo/flysnow/models"
	"github.com/panjjo/flysnow/utils"
	"gopkg.in/mgo.v2"
	"strconv"
	"strings"
	"sync"
	"time"

	"gopkg.in/mgo.v2/bson"
)

type SnowSys struct {
	*utils.SnowKey
	RedisConn *utils.RedisConn
	Tag, Term string
	Now       int64
	SpKey     map[string]string // 特殊key处理
}

var snowlock rwmutex

type rwmutex struct {
	// m map[string]*sync.RWMutex
	l *sync.Mutex
}

func init() {
	// snowlock = rwmutex{m: map[string]*sync.RWMutex{}}
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

func NeedRotate(snowsys *SnowSys, snow models.Snow) (bl bool) {
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
			bl = true
			snowsys.RedisConn.Dos("RENAME", snowsys.Key, snowsys.Key+"_rotate")
			utils.Log.DEBUG.Printf("start rotate key:%s,rename", snowsys.Key)
			if !snowsys.SnowKey.KeyCheck {
				// 非全局自检 新增一个key
				end := utils.DurationMap[snow.InterValDuration](now, snow.Interval)
				start := utils.DurationMap[snow.InterValDuration+"l"](end, snow.Interval)
				snowsys.RedisConn.Dos("HMSET", snowsys.Key, "s_time", start, "e_time", end)
				utils.Log.DEBUG.Printf("new key:%s,s:%d,e:%d", snowsys.Key, start, end)
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
		utils.Log.DEBUG.Printf("new key:%s,s:%d,e:%d", snowsys.Key, start, end)
	}
	return
}

func Rotate(snowsys *SnowSys, snows []models.Snow) {
	snowsys.RedisConn = utils.NewRedisConn(snowsys.Tag)
	defer snowsys.RedisConn.Close()
	tag := snowsys.Tag
	term := snowsys.Term
	if len(snows) == 0 || !NeedRotate(snowsys, snows[0]) {
		return
	}
	utils.Log.DEBUG.Printf("rotate key:%s", snowsys.Key)
	b, _ := snowsys.RedisConn.Dos("HGETALL", snowsys.Key+"_rotate")
	if b == nil {
		utils.Log.ERROR.Printf("rotate key:%s end,data is nil", snowsys.Key)
		return
	}
	tb := b.([]interface{})
	if len(tb) == 0 {
		utils.Log.ERROR.Printf("rotate key:%s end,data is empty", snowsys.Key)
		return
	}
	defer snowsys.RedisConn.Dos("DEL", snowsys.Key+"_rotate")
	go func(tb []interface{}) { // 开始归档
		now := snowsys.Now
		session := utils.MgoSessionDupl(tag)
		defer session.Close()
		// tb为redis 数据
		// 把tb 转化成map
		dm := map[string]interface{}{}
		for i := 0; i < len(tb); i = i + 2 {
			if key := string(tb[i].([]uint8)); key == "s_time" || key == "e_time" {
				dm[key], _ = strconv.ParseInt(string(tb[i+1].([]uint8)), 10, 64)
			} else {
				dm[key], _ = strconv.ParseFloat(string(tb[i+1].([]uint8)), 64)
			}
		}

		// 特殊key处理
		redisSpKey(dm, snowsys)

		mc := session.DB(models.MongoDT + tag).C(term)
		var data SnowData
		// 存储归档集合的开始时间，用作下一个归档集合的结束时间
		var lasttime int64
		rotatedata := []map[string]interface{}{}
		// 循环归档配置
		for sk, s := range snows {
			// key = fs_shop_@shopid_xxxx_1_m
			key := snowsys.Key + "_" + fmt.Sprintf("%d", s.Interval) + "_" + s.InterValDuration
			// 如果为第一个归档,表示redis 入mongo
			if sk == 0 {
				// 获取第一个归档mongo数据集合
				// 第一归档表示从redis归档到mongo，时间跨度
				if err := mc.Find(bson.M{"s_key": key}).One(&data); err != nil {
					if err != mgo.ErrNotFound {
						utils.Log.ERROR.Printf("rotate get mgo key:%s,err:%v", key, err)
					} else {
						utils.Log.INFO.Printf("rotate get mgo notfound key:%s", key)
					}
				}

				// 重置mongo第一个归档数据集合的截止时间,为redis数据的截止时间
				utils.Log.DEBUG.Printf("rotate rds->mgo,key:%s,data:%+v,ms:%d,me:%d", key, dm, data.STime, data.ETime)
				if data.ETime < dm["e_time"].(int64) {
					// 如果mongo第一归档截止时间小于 redis截止时间 ，正常rotate
					data.ETime = dm["e_time"].(int64)
					// 根据第一归档数据集合的存储时间总长度，计算当前集合的开始时间
					data.STime = utils.DurationMap[s.TimeoutDuration+"l"](data.ETime, s.Timeout)
					// 将最新redis数据 append到第一归档数据集合-默认redis数据的时间间隔为第一归档数据集合单位数据的时间跨度
					data.Data = append(data.Data, dm)
					utils.Log.DEBUG.Printf("rotate key:%s,append", key)
				} else {
					// 老旧数据rotate 不需要进行时间的替换
					// 老旧数据 循环 第一归档数据集合 进行判断是否需要数据合并
					for i, d := range data.Data {
						if d["e_time"].(int64) == dm["e_time"].(int64) {
							// 单位数据时间与新数据时间一致
							// 进行合并 ，应该本就是一天数据，无需进行spkey处理
							data.Data[i] = rotate(d, dm, map[string]string{})
							utils.Log.DEBUG.Printf("rotate key:%s,merge", key)
							break
						} else if d["e_time"].(int64) > dm["e_time"].(int64) {
							// 单位数据时间大于新数据时间，表示需要将新数据插入到此数据位置
							f := append([]map[string]interface{}{}, data.Data[:i]...)
							s := append([]map[string]interface{}{}, data.Data[i:]...)
							data.Data = append(append(f, dm), s...)
							utils.Log.DEBUG.Printf("rotate key:%s,insert", key)
							break
						}
					}
				}
				// 本次归档集合剩余需要保存的列表
				td := []map[string]interface{}{}

				// rotatedata 需要进行下次归档的数据，默认为全部数据
				rotatedata = data.Data
				// 复制当前归档集合开始时间
				lasttime = data.STime
				// 1. 循环第一归档内所有单位数据，判断是否超过此集合时间限制
				for k, v := range data.Data {
					if d, ok := v["s_time"]; ok {
						if utils.TInt64(d) >= data.STime {
							// 因为归档数据所有单位是按照时间先后进行append的，如果找到第一个不超期时间，剩余皆不超期
							td = data.Data[k:]
							rotatedata = data.Data[:k]
							break
						}
					}
				}
				// 重新复制归档数据集合
				data.Data = td
				if data.Key == "" {
					// 如果第一归档数据不存在，进行初始化
					data.Index = snowsys.Index
					data.Key = key
					data.Tag = tag
					data.Term = term
				}
				// cinfo, err := mc.Upsert(bson.M{"s_key": key}, bson.M{"$set": bson.M{"s_time": data.STime, "e_time": data.ETime, "tag": tag, "term": term, "data": td, "index": snowsys.Index}})
				// 第一归档数据upsert，确保一定至少有一条，不存在则写入
				if _, err := mc.Upsert(bson.M{"s_key": key}, data); err != nil {
					utils.Log.ERROR.Printf("save mgo key:%s,err:%v", key, err)
				}
				if len(rotatedata) == 0 {
					// 如果不存在超期数据，结束循环
					break
				}
				// 如果有，继续下一次归档
			} else {
				data = SnowData{}
				// 查询第sk个归档数据集合
				if err := mc.Find(bson.M{"s_key": key}).One(&data); err != nil {
					if err != mgo.ErrNotFound {
						utils.Log.ERROR.Printf("rotate get mgo key:%s,err:%v", key, err)
					} else {
						utils.Log.INFO.Printf("rotate get mgo notfound key:%s", key)
					}
				}
				// 重置mongo第sk个归档数据集合的截止时间,为上一个归档集合的开始时间
				data.ETime = lasttime
				// 根据集合的存储时间总长度，计算当前集合的开始时间
				data.STime = utils.DurationMap[s.TimeoutDuration+"l"](data.ETime, s.Timeout)
				// 复制当前归档集合开始时间
				lasttime = data.STime
				// 赋值上一个集合所剩超期需归档数据集合
				ttt := rotatedata
				td := []map[string]interface{}{}
				rotatedata = data.Data
				// 循环第sk归档内所有单位数据，判断是否超过此集合时间限制
				for k, v := range data.Data {
					if d, ok := v["s_time"]; ok {
						if utils.TInt64(d) >= data.STime {
							// 因为归档数据所有单位是按照时间先后进行append的，如果找到第一个不超期时间，剩余皆不超期
							td = data.Data[k:]
							rotatedata = data.Data[:k]
							break
						}
					}
				}
				// 循环上个归档集合所遗留的超期集合
				for _, v := range ttt {
					o := false
					v["e_time"] = utils.DurationMap[s.InterValDuration](utils.TInt64(v["e_time"]), s.Interval)
					v["s_time"] = utils.DurationMap[s.InterValDuration+"l"](utils.TInt64(v["e_time"]), s.Interval)
					/*lasttime = utils.TInt64(v["e_time"])*/
					for k1, v1 := range td {
						// 判断超期集合的元素是否属于当前集合中的一个子项，如果是累加到子项里面
						if v["s_time"].(int64) >= v1["s_time"].(int64) && v["e_time"].(int64) <= v1["e_time"].(int64) {
							td[k1] = rotate(v, v1, snowsys.SpKey)
							o = true
						}
					}
					if !o {
						// 如果不是且被当前归档集合时间包含，在当前集合新增一个子项
						if v["s_time"].(int64) >= data.STime {
							td = append(td, v)
						} else {
							// 放到过期集合，进行下一个归档
							rotatedata = append(rotatedata, v)
						}

					}
				}
				utils.Log.DEBUG.Printf("save mgo key:%s,s:%d,e:%d", key, data.STime, data.ETime)
				if _, err := mc.Upsert(bson.M{"s_key": key},
					bson.M{"$set":
					bson.M{
						"s_time": data.STime,
						"e_time": data.ETime,
						"tag":    tag,
						"term":   term,
						"data":   td,
						"index":  snowsys.Index}}); err != nil {
					utils.Log.ERROR.Printf("save mgo key:%s,err:%v", key, err)
				}
				if len(rotatedata) == 0 {
					break
				}
			}
		}
		if len(rotatedata) > 0 {
			utils.Log.ERROR.Printf("rotate last snow. term:%s-%s,key:%s,data:%v", snowsys.Tag, snowsys.Term, snowsys.SnowKey.Key, rotatedata)
			tmp := bson.M{}
			for _, v := range rotatedata {
				for k1, v1 := range v {
					if k1 == "s_time" || k1 == "e_time" {
						continue
					}
					if v2, ok := tmp[k1]; ok {
						tmp[k1] = utils.TFloat64(v2) + utils.TFloat64(v1)
					} else {
						tmp[k1] = v1
					}
				}
			}
			mc.Upsert(bson.M{"s_key": snowsys.Key}, bson.M{"$inc": tmp, "$set": bson.M{
				"e_time": now, "tag": tag, "term": term, "index": snowsys.Index}})

		}
	}(tb)
}

// rds key rotate
func ClearRedisKey(tag string) {
	for {
		now := utils.GetNowSec()
		if utils.Sec2Str("15", now) == "04" {
			utils.Log.INFO.Println("Do rds'key rollback", utils.Sec2Str("2006-01-02 15:04", now))
			rdsconn := utils.NewRedisConn(tag)
			defer rdsconn.Close()

			keys, err := rdsconn.Dos("KEYS", "fs_*")
			if err != nil {
				continue
			}
			var index map[string]interface{}
			var ks, tl []string
			var tk string
			for _, k := range keys.([]interface{}) {
				tk = string(k.([]byte))
				tl = strings.Split(tk, "_")
				tag = tl[1]
				ks = []string{}
				index = map[string]interface{}{}
				for i := 2; i <= len(tl[1:]); i = i + 1 {
					ks = append(ks, tl[i])
					if tl[i][:1] == "@" {
						index[tl[i][1:]] = tl[i+1]
						i += 1
					}
				}
				utils.Log.DEBUG.Printf("auto rotate key:%s,tag:%s", tk, tag)
				for term, config := range models.TermConfigMap[tag] {
					fmt.Println(config.Key, ks)
					if strings.Join(config.Key, "_") == strings.Join(ks, "_") {
						newSnow := &SnowSys{
							&utils.SnowKey{
								tk, index,
								true,
							},
							nil,
							tag,
							term,
							now,
							config.SpKey,
						}
						Rotate(newSnow, config.Snow)
						break
					}
				}
			}
		}
		time.Sleep(1 * time.Hour)
	}
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

func rotate(from, to map[string]interface{}, spkey map[string]string) map[string]interface{} {
	for tk, tv := range from {
		if tk != "s_time" && tk != "e_time" && tk[:1] != "@" {
			if tpe, ok := spkey[tk]; ok {
				utils.RotateSpKeyFuncs(tpe, tk, from, to)
			} else {
				if v2, ok := to[tk]; ok {
					to[tk] = utils.TFloat64(v2) + utils.TFloat64(tv)
				} else {
					to[tk] = tv
				}
			}
		}
	}
	return to
}
