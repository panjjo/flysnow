package snow

import (
	"flysnow/models"
	"flysnow/utils"
	"fmt"
	"labix.org/v2/mgo/bson"
	"strconv"
	"sync"
)

type SnowSys struct {
	Key       string
	Index     map[string]interface{}
	RedisConn *utils.RedisConn
	Tag, Term string
	Now       int64
}

var snowlock rwmutex

type rwmutex struct {
	//m map[string]*sync.RWMutex
	l *sync.Mutex
}

func init() {
	//snowlock = rwmutex{m: map[string]*sync.RWMutex{}}
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
}

func NeedRotate(snowsys *SnowSys, snow models.Snow) bool {
	now := snowsys.Now
	b, _ := snowsys.RedisConn.Dos("HGET", snowsys.Key, "e_time")
	if b == nil {
		end := DurationMap[snow.InterValDuration](now, snow.Interval)
		start := DurationMap[snow.InterValDuration+"l"](end, snow.Interval)
		snowsys.RedisConn.Dos("HMSET", snowsys.Key, "s_time", start, "e_time", end)
		return false
	} else {
		endt, _ := strconv.ParseInt(string(b.([]byte)), 10, 64)
		return endt < now
	}

}

func Rotate(snowsys *SnowSys, snows []models.Snow) {
	tag := snowsys.Tag
	term := snowsys.Term
	if len(snows) == 0 || !NeedRotate(snowsys, snows[0]) {
		return
	}
	snowlock.l.Lock()
	defer snowlock.l.Unlock()
	snowsys.RedisConn.Dos("RENAME", snowsys.Key, snowsys.Key+"_rotate")
	NeedRotate(snowsys, snows[0])
	now := snowsys.Now
	session := utils.MgoSessionDupl(tag)
	mc := session.DB(models.MongoDT + tag).C(term)
	defer session.Close()
	var data SnowData
	var lasttime int64
	retatedata := []map[string]interface{}{}
	for sk, s := range snows {
		key := snowsys.Key + "_" + fmt.Sprintf("%d", s.Interval) + "_" + s.InterValDuration
		if sk == 0 {
			b, _ := snowsys.RedisConn.Dos("HGETALL", snowsys.Key+"_rotate")
			if b == nil {
				return
			}
			tb := b.([]interface{})
			if len(tb) == 0 {
				return
			}
			dm := map[string]interface{}{}
			for i := 0; i < len(tb); i = i + 2 {
				dm[string(tb[i].([]uint8))], _ = strconv.ParseInt(string(tb[i+1].([]uint8)), 10, 64)
			}
			mc.Find(bson.M{"s_key": key}).One(&data)
			data.ETime = dm["e_time"].(int64)
			data.STime = DurationMap[s.TimeoutDuration+"l"](data.ETime, s.Timeout)
			td := []map[string]interface{}{}
			data.Data = append(data.Data, dm)
			retatedata = data.Data
			lasttime = data.STime
			for k, v := range data.Data {
				if d, ok := v["s_time"]; ok {
					if d.(int64) >= data.STime {
						td = data.Data[k:]
						retatedata = data.Data[:k]
						break
					}
				}
			}
			mc.Upsert(bson.M{"s_key": key}, bson.M{"$set": bson.M{"s_time": data.STime, "e_time": data.ETime, "tag": tag, "term": term, "data": td, "index": snowsys.Index}})

			b1, _ := snowsys.RedisConn.Dos("HGETALL", snowsys.Key+"_rotate")
			tb1 := b1.([]interface{})
			dm1 := map[string]interface{}{}
			for i := 0; i < len(tb1); i = i + 2 {
				dm1[string(tb1[i].([]uint8))], _ = strconv.ParseInt(string(tb1[i+1].([]uint8)), 10, 64)
			}

			snowsys.RedisConn.Dos("DEL", snowsys.Key+"_rotate")
			if len(retatedata) == 0 {
				break
			}
		} else {
			data = SnowData{}
			mc.Find(bson.M{"s_key": key}).One(&data)
			data.ETime = lasttime
			data.STime = DurationMap[s.TimeoutDuration+"l"](data.ETime, s.Timeout)
			lasttime = data.STime
			ttt := retatedata
			td := []map[string]interface{}{}
			retatedata = data.Data
			for k, v := range data.Data {
				if d, ok := v["s_time"]; ok {
					if d.(int64) >= data.STime {
						td = data.Data[k:]
						retatedata = data.Data[:k]
						break
					}
				}
			}

			for _, v := range ttt {
				o := false
				tmpsnow := snows[sk]
				v["e_time"] = DurationMap[tmpsnow.InterValDuration](v["e_time"].(int64), tmpsnow.Interval)
				v["s_time"] = DurationMap[tmpsnow.InterValDuration+"l"](v["e_time"].(int64), tmpsnow.Interval)
				lasttime = v["e_time"].(int64)
				for k1, v1 := range td {
					if v["s_time"].(int64) >= v1["s_time"].(int64) && v["e_time"].(int64) <= v1["e_time"].(int64) {
						for tk, tv := range v {
							if tk != "s_time" && tk != "e_time" {
								if v2, ok := v1[tk]; ok {
									v1[tk] = v2.(int64) + tv.(int64)
								} else {
									v1[tk] = tv
								}
							}
						}
						td[k1] = v1
						o = true
					}
				}
				if !o {
					if v["s_time"].(int64) >= data.STime {
						td = append(td, v)
					} else {
						retatedata = append(retatedata, v)
					}

				}
			}
			mc.Upsert(bson.M{"s_key": key}, bson.M{"$set": bson.M{"s_time": data.STime, "e_time": data.ETime, "tag": tag, "term": term, "data": td, "index": snowsys.Index}})
			if len(retatedata) == 0 {
				break
			}
		}
	}
	if len(retatedata) > 0 {
		tmp := bson.M{}
		for _, v := range retatedata {
			for k1, v1 := range v {
				if k1 == "s_time" || k1 == "e_time" {
					continue
				}
				if v2, ok := tmp[k1]; ok {
					tmp[k1] = v2.(int64) + v1.(int64)
				} else {
					tmp[k1] = v1
				}
			}
		}
		mc.Upsert(bson.M{"s_key": snowsys.Key}, bson.M{"$inc": tmp, "$set": bson.M{
			"e_time": now, "tag": tag, "term": term, "index": snowsys.Index}})

	}
}

var DurationMap = map[string]func(t, num int64) int64{
	"s":  durationS,
	"h":  durationH,
	"d":  durationD,
	"m":  durationM,
	"sl": durationSL,
	"hl": durationHL,
	"dl": durationDL,
	"ml": durationML,
}

func durationS(t, num int64) int64 {
	return t - t%num + num
}
func durationH(t, num int64) int64 {
	num = num * 60 * 60
	return t - t%num + num
}
func durationD(t, num int64) int64 {
	num = num * 60 * 60 * 24
	return t - t%num + num
}
func durationM(t, num int64) int64 {
	sm := utils.Sec2Str("200601", t)
	year, _ := strconv.ParseInt(sm[:4], 10, 64)
	month := sm[4:]
	start, _ := strconv.ParseInt(month, 10, 64)
	end := start + num + 1
	year += end / 12
	end = end % 12
	return utils.Str2Sec("200601", fmt.Sprintf("%4d%02d", year, end))
}
func durationSL(e, num int64) int64 {
	return e - num
}
func durationHL(e, num int64) int64 {
	return e - num*60*60
}
func durationDL(e, num int64) int64 {
	return e - num*60*60*24
}
func durationML(e, num int64) int64 {
	sm := utils.Sec2Str("20060102 15:04:05", e)
	year, _ := strconv.ParseInt(sm[:4], 10, 64)
	month := sm[4:6]
	other := sm[6:]
	start, _ := strconv.ParseInt(month, 10, 64)
	end := start - num
	year += end / 12
	end = end % 12
	if end == 0 {
		year += -1
		end = 12
	}
	return e - utils.Str2Sec("20060102 15:04:05", fmt.Sprintf("%4d%02d%s", year, end, other))
}
