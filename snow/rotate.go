package snow

import (
	"fmt"
	"github.com/panjjo/flysnow/models"
	"github.com/panjjo/flysnow/utils"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"strconv"
	"strings"
	"time"
)

// 归档计算
func rotateObj(from, to map[string]interface{}, spkey map[string]string) map[string]interface{} {
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

// rds key rotate 每天定时归档需要归档的数据（有些数据后面没请求，不会触发归档，所以每天自动检测归档）
func autoRotate() {
	var result interface{}
	var startCurr, curr string
	var data []interface{}
	var keys []interface{}
	var rdsconn *utils.RedisConn
	startCurr = "0"
	var tk string
	var tl, ks []string
	now := utils.GetNowSec()
	log.INFO.Println("Do rds'key rollback", utils.Sec2Str("2006-01-02 15:04", now))
	rdsconn = utils.NewRedisConn(models.TagList[0])
	defer rdsconn.Close()
	curr = startCurr
	keys = []interface{}{}
	for {
		result, _ = rdsconn.Dos("SCAN", curr, "MATCH", models.RedisKT+"_*")
		data = result.([]interface{})
		if len(data) != 2 {
			break
		}
		curr = fmt.Sprintf("%s", data[0].([]uint8))
		if v, ok := data[1].([]interface{}); ok {
			keys = append(keys, v...)
		}
		if curr == startCurr {
			break
		}
	}
	log.INFO.Println("wait rotate keys len:", len(keys))
	for i, k := range keys {
		tk = string(k.([]byte))
		tl = strings.Split(tk, "_")
		ks = []string{}
		for i := 2; i <= len(tl[1:]); i = i + 1 {
			ks = append(ks, tl[i])
		}
		log.DEBUG.Printf("auto rotate index:%d, key:%s,tag:%s", i, tk, tl[1])
		for term, config := range models.TermConfigMap[tl[1]] {
			if strings.Join(config.Key, "_") == strings.Join(ks, "_") {
				newSnow := &SnowSys{
					&utils.SnowKey{
						tk, nil,
						true,
					},
					nil,
					tl[1],
					term,
					now,
					config.SpKey,
				}
				NeedRotate(newSnow, config.Snow[0])
				break
			}
		}
	}
}

var haveRotatePro bool

// 归档工作 每分钟一次，存在跳过，不存在启动
func lsrRotate() {
	if !haveRotatePro {
		go rotate()
	}
}

func rotate() {
	redisConn := utils.NewRedisConn(models.TagList[0])
	defer func() {
		haveRotatePro = false
		redisConn.Close()
		log.INFO.Print("stop rotate process")
	}()
	haveRotatePro = true
	log.INFO.Print("start rotate process")
	var result interface{}
	var err error
	var b interface{}
	var sourceKey string
	for {
		// 取出集合中的待归档key，从右侧取出（左入右出）
		result, err = redisConn.Dos("RPOP", ROTATEKEYS)
		if err != nil {
			log.ERROR.Printf("get rotate key err:%v", err)
			return
		}
		if result == nil {
			time.Sleep(1 * time.Second)
			continue
		}
		sourceKey = string(result.([]uint8))
		b, _ = redisConn.Dos("HGETALL", sourceKey)
		if b == nil {
			log.ERROR.Printf("rotate key:%s,data is nil", sourceKey)
			continue
		}
		tb := b.([]interface{})
		if len(tb) == 0 {
			log.ERROR.Printf("rotate key:%s end,data is empty", sourceKey)
			continue
		}
		rotateDo(tb, sourceKey)
	}
}
func rotateDo(tb []interface{}, sourceKey string) {

	rotateFunc := func() { // 开始归档
		var datakey, tag, term, key string
		dm := map[string]interface{}{}
		for i := 0; i < len(tb); i = i + 2 {
			datakey = string(tb[i].([]uint8))
			switch datakey {
			case "s_time", "e_time":
				dm[datakey], _ = strconv.ParseInt(string(tb[i+1].([]uint8)), 10, 64)
			case "tag":
				tag = string(tb[i+1].([]uint8))
			case "key":
				key = string(tb[i+1].([]uint8))
			case "term":
				term = string(tb[i+1].([]uint8))
			default:
				dm[datakey], _ = strconv.ParseFloat(string(tb[i+1].([]uint8)), 64)
			}
		}
		log.DEBUG.Printf("start rotate ,sourcekey %s,key:%s,tag:%s,term:%s", sourceKey, key, tag, term)
		snowCfg := models.TermConfigMap[tag][term]
		snowsys := &SnowSys{
			&utils.SnowKey{
				key, utils.GetIndexBySKey(key), false,
			},
			utils.NewRedisConn(tag),
			tag,
			term,
			utils.GetNowSec(),
			snowCfg.SpKey,
		}
		defer func() {
			snowsys.RedisConn.Dos("DEL", sourceKey)
			snowsys.RedisConn.Close()
		}()

		session := utils.MgoSessionDupl(tag)
		defer session.Close()
		// tb为redis 数据
		// 把tb 转化成map

		// 特殊key处理
		redisSpKey(dm, snowsys)

		mc := session.DB(models.MongoDT + tag).C(term)
		var data SnowData
		// 存储归档集合的开始时间，用作下一个归档集合的结束时间
		var lasttime int64
		rotatedata := []map[string]interface{}{}
		// 循环归档配置
		for sk, s := range snowCfg.Snow {
			// key = fs_shop_@shopid_xxxx_1_m
			key := snowsys.Key + "_" + fmt.Sprintf("%d", s.Interval) + "_" + s.InterValDuration
			// 如果为第一个归档,表示redis 入mongo
			if sk == 0 {
				// 获取第一个归档mongo数据集合
				// 第一归档表示从redis归档到mongo，时间跨度
				if err := mc.Find(bson.M{"s_key": key}).One(&data); err != nil {
					if err != mgo.ErrNotFound {
						log.ERROR.Printf("rotate get mgo key:%s,err:%v", key, err)
					} else {
						log.DEBUG.Printf("rotate get mgo notfound key:%s", key)
					}
				}

				// 重置mongo第一个归档数据集合的截止时间,为redis数据的截止时间
				log.DEBUG.Printf("rotate rds->mgo,key:%s,data:%+v,ms:%d,me:%d", key, dm, data.STime, data.ETime)
				if data.ETime < dm["e_time"].(int64) {
					// 如果mongo第一归档截止时间小于 redis截止时间 ，正常rotate
					data.ETime = dm["e_time"].(int64)
					// 根据第一归档数据集合的存储时间总长度，计算当前集合的开始时间
					data.STime = utils.DurationMap[s.TimeoutDuration+"l"](data.ETime, s.Timeout)
					// 将最新redis数据 append到第一归档数据集合-默认redis数据的时间间隔为第一归档数据集合单位数据的时间跨度
					data.Data = append(data.Data, dm)
					log.DEBUG.Printf("rotate key:%s,append", key)
				} else {
					// 老旧数据rotate 不需要进行时间的替换
					// 老旧数据 循环 第一归档数据集合 进行判断是否需要数据合并
					for i, d := range data.Data {
						if d["e_time"].(int64) == dm["e_time"].(int64) {
							// 单位数据时间与新数据时间一致
							// 进行合并 ，应该本就是一天数据，无需进行spkey处理
							data.Data[i] = rotateObj(d, dm, map[string]string{})
							log.DEBUG.Printf("rotate key:%s,merge", key)
							break
						} else if d["e_time"].(int64) > dm["e_time"].(int64) {
							// 单位数据时间大于新数据时间，表示需要将新数据插入到此数据位置
							f := append([]map[string]interface{}{}, data.Data[:i]...)
							s := append([]map[string]interface{}{}, data.Data[i:]...)
							data.Data = append(append(f, dm), s...)
							log.DEBUG.Printf("rotate key:%s,insert", key)
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
					log.ERROR.Printf("save mgo key:%s,err:%v", key, err)
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
						log.ERROR.Printf("rotate get mgo key:%s,err:%v", key, err)
					} else {
						log.INFO.Printf("rotate get mgo notfound key:%s", key)
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
							td[k1] = rotateObj(v, v1, snowsys.SpKey)
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
				log.DEBUG.Printf("save mgo key:%s,s:%d,e:%d", key, data.STime, data.ETime)
				if _, err := mc.Upsert(bson.M{"s_key": key},
					bson.M{"$set":
					bson.M{
						"s_time": data.STime,
						"e_time": data.ETime,
						"tag":    tag,
						"term":   term,
						"data":   td,
						"index":  snowsys.Index}}); err != nil {
					log.ERROR.Printf("save mgo key:%s,err:%v", key, err)
				}
				if len(rotatedata) == 0 {
					break
				}
			}
		}
		if len(rotatedata) > 0 {
			log.ERROR.Printf("rotate last snow. term:%s-%s,key:%s,data:%v", snowsys.Tag, snowsys.Term, snowsys.SnowKey.Key, rotatedata)
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
				"e_time": snowsys.Now, "tag": tag, "term": term, "index": snowsys.Index}})

		}
	}
	// if err := rotatePool.Submit(rotateFunc); err != nil {
	// 	log.Error(fmt.Sprintf("rotate pool submit task err:%v", err))
	// }
	rotateFunc()

}
