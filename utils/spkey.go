package utils

import (
	"flysnow/models"
)

//归档时redis需要针对spkey做的特殊处理
func RDSSpKeyFuncs(t, key string, data map[string]interface{}, rkey *SnowKey) RdsSendStruct {
	switch t {
	case models.SPKEYLAST:
		return spkeyLastRedis(key, data[key], rkey)
	case models.SPKEYAVG:
		return spkeyAvgRedis(key, data, rkey)
	default:
		Log.ERROR.Printf("not found spkey type:%s", t)
		return RdsSendStruct{}
	}
}

//last
func spkeyLastRedis(key string, value interface{}, rkey *SnowKey) RdsSendStruct {
	//将上一条的数据写入到下一条中
	return RdsSendStruct{rkey.Key, []RdsCommand{RdsCommand{"HINCRBYFLOAT", []interface{}{key, TFloat64(value)}}}}
}

//avg
func spkeyAvgRedis(key string, data map[string]interface{}, rkey *SnowKey) RdsSendStruct {
	data["@num_"+key] = 1.0
	//将上一条的数据写入到下一条中
	return RdsSendStruct{rkey.Key, []RdsCommand{RdsCommand{"HINCRBYFLOAT", []interface{}{key, TFloat64(data[key])}}}}
}

// 归档时redis,mongo数据进行的计算
func RotateSpKeyFuncs(t, key string, from, data map[string]interface{}) map[string]interface{} {
	switch t {
	case models.SPKEYLAST:
		spkeyLastRotate(key, from[key], data)
	case models.SPKEYAVG:
		spkeyAvgRotate(key, from, data)
	default:
		Log.ERROR.Printf("not found spkey type:%s", t)
	}
	return data
}

func spkeyLastRotate(key string, value interface{}, data map[string]interface{}) {
	//last，使用新值覆盖原值
	data[key] = value
}
func spkeyAvgRotate(key string, from, data map[string]interface{}) {
	//avg 值累加，附加一个计数字段  avg=key/@key
	numkey := "@num_" + key
	if _, ok := from[numkey]; !ok {
		from[numkey] = 1
	}
	if v, ok := data[key]; ok {
		data[key] = TFloat64(v) + TFloat64(from[key])
		data[numkey] = TFloat64(data[numkey]) + TFloat64(from[numkey])
	} else {
		data[key] = from[key]
		data[numkey] = from[numkey]
	}
}

//统计时的特殊计算
func StatSpKeyFuncs(t, key string, data map[string]interface{}) map[string]interface{} {
	switch t {
	case models.SPKEYLAST:
	case models.SPKEYAVG:
		//计算平均值
		spkeyAvgStat(key, data)
	default:
		Log.ERROR.Printf("not found spkey type:%s", t)
	}
	return data
}

func spkeyAvgStat(key string, data map[string]interface{}) {
	data["@"+key] = data[key]
	if _, ok := data["@num_"+key]; !ok {
		data["@num_"+key] = 1
	}
	data[key] = TFloat64(data[key]) / TFloat64(data["@num_"+key])
}
