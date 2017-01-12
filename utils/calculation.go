package utils

import (
	"reflect"
	"strings"
)

type SnowKey struct {
	Key   string
	Index map[string]interface{}
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
func GetRdsKeyByIndex(d map[string]interface{}, keys []string) []string {
	strs := [][]string{[]string{""}}
	data := map[string]interface{}{}
	for k, v := range d {
		data["@"+k] = v
	}
	for _, key := range keys {
		if v, ok := data[key]; ok {
			if v1, ok1 := v.(string); ok1 {
				for i, strlist := range strs {
					strs[i] = append(strlist, key+"_"+strings.Replace(v1, "_", ".", -1))
				}
			} else if v2, ok2 := v.(map[string]interface{}); ok2 {
				tmpstrs := [][]string{}
				for _, str := range strs {
					for _, ttk := range v2["$in"].([]interface{}) {
						tmpstrs = append(tmpstrs, append(str, key+"_"+ttk.(string)))
					}
				}
				strs = tmpstrs
			}
		} else if key[:1] == "@" {
			for i, strlist := range strs {
				strs[i] = append(strlist, key+"_*")
			}
		} else {
			for i, strlist := range strs {
				strs[i] = append(strlist, key)
			}
		}
	}
	result := []string{}
	for _, str := range strs {
		result = append(result, strings.Join(str[1:], "_"))
	}
	return result
}
