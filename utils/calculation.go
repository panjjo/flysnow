package utils

import (
	"reflect"
	"strings"
)

type SnowKey struct {
	Key      string
	Index    map[string]interface{}
	KeyCheck bool
}

func GetIndexByMap(keys []string, data map[string]interface{}) map[string]interface{} {
	result := map[string]interface{}{}
	for _, key := range keys {
		if v, ok := data[key]; ok {
			result[key] = v
		}
	}
	return result
}

func GetIndexBySKey(key string) map[string]interface{} {
	result := map[string]interface{}{}
	tl := strings.Split(key, "_")
	for i := 2; i <= len(tl[1:]); i = i + 1 {
		if tl[i][:1] == "@" {
			result[tl[i][1:]] = tl[i+1]
			i += 1
		}
	}
	return result
}
func GetKeyByMap(keys []string, data map[string]interface{}) string {
	strs := []string{}
	for _, key := range keys {
		if v, ok := data[key]; ok {
			strs = append(strs, "@"+key+"_"+strings.Replace(v.(string), "_", ".", -1))
		} else {
			strs = append(strs, key)
		}
	}
	return strings.Join(strs, "_")
}

func GetKeyAndIndexByMap(keys []string, data map[string]interface{}) (string, map[string]interface{}) {
	result := map[string]interface{}{}
	strs := []string{}
	for _, key := range keys {
		if v, ok := data[key]; ok {
			strs = append(strs, "@"+key+"_"+strings.Replace(v.(string), "_", ".", -1))
			result[key] = v
		} else {
			strs = append(strs, key)
		}
	}
	return strings.Join(strs, "_"), result

}

func GetKeyByObj(keys []string, obj interface{}) string {
	t := reflect.TypeOf(obj)
	v := reflect.ValueOf(obj)
	data := map[string]string{}
	for i := 0; i < t.NumField(); i++ {
		data["@"+strings.ToLower(t.Field(i).Name)] = v.Field(i).String()
	}
	strs := []string{}
	for _, key := range keys {
		if v, ok := data[key]; ok {
			strs = append(strs, key+"_"+strings.Replace(v, "_", ".", -1))
		} else {
			strs = append(strs, key)
		}
	}
	return strings.Join(strs, "_")
}

type ComplexRdsKey struct {
	keys []string
	Key  string
	Re   bool
}

func GetRdsKeyByIndex(d map[string]interface{}, keys []string) []ComplexRdsKey {
	strs := []ComplexRdsKey{{}}
	data := map[string]interface{}{}
	for k, v := range d {
		data["@"+k] = v
	}
	for _, key := range keys {
		if v, ok := data[key]; ok {
			if v1, ok1 := v.(string); ok1 {
				for i, strlist := range strs {
					strs[i].keys = append(strlist.keys, key+"_"+strings.Replace(v1, "_", ".", -1))
				}
			} else if v2, ok2 := v.(map[string]interface{}); ok2 {
				tmpstrs := []ComplexRdsKey{}
				for _, str := range strs {
					for _, ttk := range v2["$in"].([]interface{}) {
						tmpstrs = append(tmpstrs, ComplexRdsKey{keys: append(str.keys, key+"_"+ttk.(string)), Re: str.Re})
					}
				}
				strs = tmpstrs
			}
		} else if key[:1] == "@" {
			for i, strlist := range strs {
				strs[i].keys = append(strlist.keys, key+"_*")
				strs[i].Re = true
			}
		} else {
			for i, strlist := range strs {
				strs[i].keys = append(strlist.keys, key)
			}
		}
	}
	for i, str := range strs {
		str.Key = strings.Join(str.keys, "_")
		strs[i] = str
	}
	return strs
}
func DataFilter(data map[string]interface{}, filter map[string]interface{}) bool {
	for k, f := range filter {
		if k == "$or" {
			ok := false
			for _, tmp := range f.([]interface{}) {
				tm := tmp.(map[string]interface{})
				if DataFilter(data, tm) {
					ok = true
					break
				}
			}
			if !ok {
				return false
			}
		} else if k == "$and" {
			for _, tmp := range f.([]interface{}) {
				if !DataFilter(data, tmp.(map[string]interface{})) {
					return false
				}
			}
		} else {
			if value, ok := data[k]; !ok {
				return false
			} else {
				switch n := f.(type) {
				case float64:
					if n != TFloat64(value) {
						return false
					}
				case int64:
					if n != TInt64(value) {
						return false
					}
				case map[string]interface{}:
					for kk, tv := range n {
						vv := TFloat64(tv)
						switch kk {
						case "$gt": // >
							if vv >= TFloat64(value) {
								return false
							}
						case "$gte":
							if vv > TFloat64(value) {
								return false
							}
						case "$lt":
							if vv <= TFloat64(value) {
								return false
							}
						case "$lte":
							if vv < TFloat64(value) {
								return false
							}
						case "$ne":
							if vv == TFloat64(value) {
								return false
							}
						}
					}
				default:
					Log.ERROR.Println(n, f)
				}
				return true
			}
		}
	}
	return true
}
