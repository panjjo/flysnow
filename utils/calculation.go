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
func GetRdsKeyByIndex(d map[string]interface{}, keys []string) string {
	strs := []string{}
	data := map[string]interface{}{}
	for k, v := range d {
		data["@"+k] = v
	}
	for _, key := range keys {
		if v, ok := data[key]; ok {
			strs = append(strs, key+"_"+strings.Replace(v.(string), "_", ".", -1))
		} else if key[:1] == "@" {
			strs = append(strs, key+"_*")
		} else {
			strs = append(strs, key)
		}
	}
	return strings.Join(strs, "_")
}
