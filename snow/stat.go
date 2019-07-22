package snow

import (
	"fmt"
	"github.com/panjjo/flysnow/models"
	"github.com/panjjo/flysnow/utils"
	"sort"
	"strconv"
	"strings"

	"gopkg.in/mgo.v2/bson"
)

type StatReq struct {
	Term                    string
	Index                   bson.M
	DataQuery               bson.M
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
	tl := strings.Split(key, "_")
	for i, v := range tl {
		if v == "" {
			continue
		}
		if v[:1] == "@" {
			tm[v[1:]] = tl[i+1]
		}
	}
	if s.IsGroup {
		for _, i := range s.Group {
			id += tm[i]
		}
	}
	dm["@groupkey"] = id
	dm["@index"] = tm
	return
}
func (s *StatReq) GroupKeyMgo(index map[string]interface{}) (id string) {
	// group 只能是index中的key
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
	// 计算每条数据的分组key
	id = d["@groupkey"].(string)
	if s.IsSpan {
		t := utils.TInt64(d["e_time"]) - 1
		e_time := utils.DurationMap[s.SpanD](t, s.Span)
		s_time := utils.DurationMap[s.SpanD+"l"](e_time, s.Span)
		if utils.TInt64(d["e_time"]) <= e_time && s_time <= utils.TInt64(d["s_time"]) {
			d["s_time"], d["e_time"] = s_time, e_time
			id += fmt.Sprintf("%d%d", s_time, e_time)
		} else {
			// 时间条件不满足的，跳过
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
	if req.ETime == 0 {
		req.ETime = utils.DurationMap["d"](utils.GetNowSec(), 1)
	}
	mgos := utils.MgoSessionDupl(tag)
	defer mgos.Close()
	mc := mgos.DB(models.MongoDT + tag).C(req.Term)
	query := bson.M{}
	if len(req.Index) > 0 {
		for k, v := range req.Index {
			query["index."+k] = v
		}
	}
	if len(req.DataQuery) > 0 {
		req.DataQuery["s_time"] = bson.M{"$gte": req.STime}
		req.DataQuery["e_time"] = bson.M{"$lte": req.ETime}
		query["data"] = bson.M{"$elemMatch": req.DataQuery}
	}
	// 获取数据
	rdsconn := utils.NewRedisConn(tag)
	defer rdsconn.Close()
	tl := []map[string]interface{}{}
	var keys interface{}
	termConfig := models.TermConfigMap[tag][req.Term]
	for _, tmpkey := range utils.GetRdsKeyByIndex(req.Index, termConfig.Key) {
		if tmpkey.Re {
			rdsk := models.RedisKT + "_" + tag + "_" + tmpkey.Key
			// get from redis
			keys, err = rdsconn.Dos("KEYS", rdsk)
			if err != nil {
				continue
			}
		} else {
			keys = []interface{}{[]byte(models.RedisKT + "_" + tag + "_" + tmpkey.Key)}
		}
		for _, k := range keys.([]interface{}) {
			tk := string(k.([]byte))
			rdsd, err := rdsconn.Dos("HGETALL", tk)
			tb := rdsd.([]interface{})
			if err == nil && len(tb) != 0 {
				dm := map[string]interface{}{}
				for i := 0; i < len(tb); i = i + 2 {
					dm[string(tb[i].([]uint8))], _ = strconv.ParseFloat(string(tb[i+1].([]uint8)), 64)
				}
				if utils.TInt64(dm["s_time"]) >= req.STime && (utils.TInt64(dm["e_time"]) <= req.ETime || req.ETime == 0) {
					req.GroupKeyRedis(tk, dm)
					tl = append(tl, dm)
				}
			}
		}
	}
	// redis end
	// mgo start
	datas := []SnowData{}
	err = mc.Find(query).All(&datas)
	mgoList := []map[string]interface{}{}
	if len(datas) > 0 {
		for _, data := range datas {
			groupkey := req.GroupKeyMgo(data.Index)
			for _, v := range data.Data {
				if utils.TInt64(v["s_time"]) >= req.STime && (utils.TInt64(v["e_time"]) <= req.ETime || req.ETime == 0) {
					v["@groupkey"] = groupkey
					v["@index"] = data.Index
					mgoList = append(mgoList, v)
				}
			}
		}
	}
	tl = append(mgoList, tl...)
	// mongo end
	// group and span
	groupdata := map[string]int{}
	data := []map[string]interface{}{}
	for _, l := range tl {
		skip, gsk := req.GSKey(l)
		l["@groupkey"] = gsk
		if skip {
			// 时间不满足，跳过
			continue
		}
		if v, ok := groupdata[gsk]; ok {
			// 相同分组的累加到一起
			rotateObj(l, data[v], termConfig.SpKey)
		} else {
			// 新的一组
			data = append(data, l)
			groupdata[gsk] = len(data) - 1
		}
	}

	sortdata := []interface{}{}
	total := map[string]interface{}{}
	for _, v := range data {
		// 查询条件数据过滤
		if utils.DataFilter(v, req.DataQuery) {
			// 计算总数
			rotateObj(v, total, termConfig.SpKey)
			// 处理单项特殊key并加入排序集合
			sortdata = append(sortdata, spkeystat(v, termConfig.SpKey))
		}
	}
	// spkey,处理合计的特殊key
	total = spkeystat(total, termConfig.SpKey)
	// 按照时间skip的，补充无时间统计的数据
	if req.IsSpan && (req.STime != 0 || len(sortdata) > 0) {
		emptyIndex := map[string]string{}
		for _, k := range termConfig.Key {
			emptyIndex[k[1:]] = ""
		}
		keymaps := map[int64][]interface{}{}
		var dataTime int64
		for _, data := range sortdata {
			dataTime = data.(map[string]interface{})["s_time"].(int64)
			if _, ok := keymaps[dataTime]; !ok {
				keymaps[dataTime] = []interface{}{data}
			} else {
				keymaps[dataTime] = append(keymaps[dataTime], data)

			}
		}
		nums := len(sortdata)
		sortdata = []interface{}{}
		var stime, etime int64
		etime = req.ETime
		for {
			etime = utils.DurationMap[req.SpanD](etime-1, req.Span)
			stime = utils.DurationMap[req.SpanD+"l"](etime, req.Span)
			if etime <= req.STime {
				break
			}
			if req.STime == 0 && nums == 0 {
				break
			}
			if v, ok := keymaps[stime]; ok {
				sortdata = append(sortdata, v...)
			} else {
				sortdata = append(sortdata, map[string]interface{}{"s_time": stime, "e_time": etime, "@index": emptyIndex})
			}
			etime = stime
			nums--
		}
	}
	// sort
	if len(req.Sort) == 2 {
		sortdata = SortMapList(sortdata, req.Sort[0], req.Sort[1].(bool))
	}
	lens := len(sortdata)
	// limit
	lm := req.Limit + req.Skip
	if lm != 0 {
		start, end := 0, 0
		if lm >= lens {
			end = lens
		} else {
			end = lm
		}
		if req.Skip <= lens {
			start = req.Skip
		} else {
			start = end
		}
		sortdata = sortdata[start:end]
	}

	return nil, map[string]interface{}{"num": lens, "list": sortdata, "total": total}
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
				case int, int64, float32, float64:
					return utils.TFloat64(va) < utils.TFloat64(vb)
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

func spkeystat(data map[string]interface{}, spkey map[string]string) map[string]interface{} {
	for tk, _ := range data {
		if tk != "s_time" && tk != "e_time" && tk[:1] != "@" {
			if tpe, ok := spkey[tk]; ok {
				utils.StatSpKeyFuncs(tpe, tk, data)
			}
		}
	}
	return data
}
