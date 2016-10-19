package snow

import (
	"flysnow/models"
	"flysnow/utils"
	"fmt"
	"labix.org/v2/mgo/bson"
	"sort"
	"strconv"
	"strings"
)

type StatReq struct {
	Term                    string
	Index                   bson.M
	STime                   int64
	ETime                   int64
	Span                    int64
	SpanD                   string
	Group                   []string
	Sort                    []interface{}
	Limit, Skip             int
	IsSort, IsGroup, IsSpan bool
}

func (s *StatReq) GroupKeyRedis(key string, dm map[string]interface{}) {
	id := ""
	tm := map[string]string{}
	if s.IsGroup {
		tl := strings.Split(key, "_")
		for i, v := range tl {
			if v[:1] == "@" {
				tm[v[1:]] = tl[i+1]
			}
		}
		for _, i := range s.Group {
			id += tm[i]
		}
	}
	dm["@groupkey"] = id
	dm["@index"] = tm
	return
}
func (s *StatReq) GroupKeyMgo(index map[string]interface{}) (id string) {
	if s.IsGroup {
		for _, k := range s.Group {
			if v, ok := index[k]; ok {
				id += strings.Replace(v.(string), "_", ".", -1)
			}
		}
	}
	return
}
func (s *StatReq) GSKey(d map[string]interface{}) (skip bool, id string) {
	id = d["@groupkey"].(string)
	if s.IsSpan {
		t := d["s_time"].(int64)
		e_time := DurationMap[s.SpanD](t, s.Span)
		s_time := DurationMap[s.SpanD+"l"](e_time, s.Span)
		if d["e_time"].(int64) <= e_time && s_time <= d["s_time"].(int64) {
			d["s_time"], d["e_time"] = s_time, e_time
			id += fmt.Sprintf("%d%d", s_time, e_time)
		} else {
			skip = true
		}
	}
	return
}

func Stat(d []byte, tag string) (error, interface{}) {
	req := StatReq{}
	err := utils.JsonDecode(d, &req)
	if err != nil {
		return err, nil
	}
	if len(req.Group) != 0 {
		req.IsGroup = true
	}
	if req.Span != 0 {
		req.IsSpan = true
	}
	mgos := utils.MgoSessionDupl(tag)
	mc := mgos.DB(models.MongoDT + tag).C(req.Term)
	query := bson.M{}
	if len(req.Index) > 0 {
		query["index"] = req.Index
	}
	//获取数据
	tl := []map[string]interface{}{}
	rdsk := models.RedisKT + "_" + tag + "_" + utils.GetRdsKeyByIndex(req.Index, models.TermConfigMap[tag][req.Term].Key)
	//get from redis
	rdsconn := utils.NewRedisConn(tag)
	keys, err := rdsconn.Dos("KEYS", rdsk)
	for _, k := range keys.([]interface{}) {
		tk := string(k.([]byte))
		rdsd, err := rdsconn.Dos("HGETALL", tk)
		tb := rdsd.([]interface{})
		if err == nil && len(tb) != 0 {
			dm := map[string]interface{}{}
			for i := 0; i < len(tb); i = i + 2 {
				dm[string(tb[i].([]uint8))], _ = strconv.ParseInt(string(tb[i+1].([]uint8)), 10, 64)
			}
			if dm["s_time"].(int64) >= req.STime && (dm["e_time"].(int64) <= req.ETime || req.ETime == 0) {
				req.GroupKeyRedis(tk, dm)
				tl = append(tl, dm)
			}
		}
	}
	//redis end
	//mgo start
	datas := []SnowData{}
	mc.Find(query).All(&datas)
	if len(datas) > 0 {
		for _, data := range datas {
			groupkey := req.GroupKeyMgo(data.Index)
			for _, v := range data.Data {
				if v["s_time"].(int64) >= req.STime && (v["e_time"].(int64) <= req.ETime || req.ETime == 0) {
					v["@groupkey"] = groupkey
					v["@index"] = data.Index
					tl = append(tl, v)
				}
			}
		}
	}

	//group and span
	groupdata := map[string]map[string]interface{}{}
	for _, l := range tl {
		skip, gsk := req.GSKey(l)
		if skip {
			continue
		}
		if v, ok := groupdata[gsk]; ok {
			for lk, lv := range l {
				if lk[:1] != "@" && lk[1:] != "_time" {
					if llv, ok := v[lk]; ok {
						v[lk] = llv.(int64) + lv.(int64)
					} else {
						v[lk] = lv
					}
				}
			}
		} else {
			groupdata[gsk] = l
		}
	}

	sortdata := []interface{}{}
	for _, v := range groupdata {
		sortdata = append(sortdata, v)
	}
	//sort
	if len(req.Sort) == 2 {
		sortdata = SortMapList(sortdata, req.Sort[0], req.Sort[1].(bool))
	}
	//limit
	lm := req.Limit + req.Skip
	if lm != 0 {
		if lm >= len(sortdata) {
			sortdata = sortdata[req.Skip:]
		} else {
			sortdata = sortdata[req.Skip:lm]
		}
	}
	return nil, sortdata
}

func SortMapList(source []interface{}, name interface{}, asc bool) []interface{} {
	s := &SortMapLister{
		source,
		func(a, b interface{}) bool {
			a1, b1 := a.(map[string]interface{}), b.(map[string]interface{})
			va, vb := a1[name.(string)], b1[name.(string)]
			if va == nil {
				return true
			} else if vb == nil {
				return false
			} else {
				switch va.(type) {
				case int:
					return va.(int) < vb.(int)
				case int64:
					return va.(int64) < vb.(int64)
				case float32:
					return va.(float32) < vb.(float32)
				case float64:
					return va.(float64) < vb.(float64)
				case string:
					return va.(string) < vb.(string)
				default:
					return false
				}
			}
		},
	}
	if asc {
		sort.Sort(s)
	} else {
		sort.Sort(sort.Reverse(s))
	}
	return s.List
}

type SortMapLister struct {
	List      []interface{}
	FrontFunc func(a, b interface{}) bool
}

func (s SortMapLister) Len() int {
	return len(s.List)
}
func (s SortMapLister) Swap(i, j int) {
	s.List[i], s.List[j] = s.List[j], s.List[i]
}
func (s SortMapLister) Less(i, j int) bool {
	return s.FrontFunc(s.List[i], s.List[j])
}
