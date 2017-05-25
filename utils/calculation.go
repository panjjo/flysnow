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

func GetKey(obj interface{}, keys []string) *SnowKey {
	result := &SnowKey{Index: map[string]interface{}{}}
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
			result.Index[key[1:]] = v
		} else {
			strs = append(strs, key)
		}
	}
	result.Key = strings.Join(strs, "_")
	return result
}

type ComplexRdsKey struct {
	keys []string
	Key  string
	Re   bool
}

func GetRdsKeyByIndex(d map[string]interface{}, keys []string) []ComplexRdsKey {
	strs := []ComplexRdsKey{ComplexRdsKey{}}
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
						//str.keys = append(str.keys, key+"_"+ttk.(string))
						tmpstrs = append(tmpstrs, ComplexRdsKey{keys: append(str.keys, key+"_"+ttk.(string))})
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
					if n != float64(value.(int64)) {
						return false
					}
				case int64:
					if n != value.(int64) {
						return false
					}
				case map[string]interface{}:
					for kk, tv := range n {
						vv := tv.(float64)
						switch kk {
						case "$gt": //>
							if vv >= value.(float64) {
								return false
							}
						case "$gte":
							if vv > value.(float64) {
								return false
							}
						case "$lt":
							if vv <= value.(float64) {
								return false
							}
						case "$lte":
							if vv < value.(float64) {
								return false
							}
						case "$ne":
							if vv == value.(float64) {
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
