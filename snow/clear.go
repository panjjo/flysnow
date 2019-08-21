package snow

import (
	"fmt"
	"github.com/panjjo/flysnow/models"
	"github.com/panjjo/flysnow/utils"

	"gopkg.in/mgo.v2/bson"
)

type ClearReq struct {
	TagTerms map[string][]string `json:"tag_terms" `
	Query    bson.M              `json:"query"`
}
type clearList struct {
	Tag, Term  string
	RdsKey     string
	MongoQuery bson.M
}

func Clear(body []byte) (error, int) {
	req := ClearReq{}
	list := []clearList{}
	err := utils.JsonDecode(body, &req)
	if err != nil {
		return err, models.ErrData
	}
	// 解析需要清理的统计项
	var find bool
	var rdskey string
	var query bson.M
	for tag, terms := range req.TagTerms {
		for _, term := range terms {
			if termconfig, ok := models.TermConfigMap[tag][term]; ok {
				rdskey = fmt.Sprintf("%s_%s_*", utils.RDSPrefix, tag)
				query = bson.M{}
				for key, value := range req.Query {
					find = false
					for _, k := range termconfig.Key {
						if "@"+key == k {
							find = true
							rdskey += fmt.Sprintf("@%s_%s", key, value)
							query["index."+key] = value
						}
					}
					if !find {
						return models.ErrNew(fmt.Sprintf("%s-%s key:%s not found", tag, term, key)), models.ErrClear
					}
				}
				rdskey += "*"
				list = append(list, clearList{tag, term, rdskey, query})
			} else {
				return models.ErrNew(fmt.Sprintf("%s-%s not found", tag, term)), models.ErrClear
			}
		}
	}
	var key string

	for _, clear := range list {
		session := utils.MgoSessionDupl()
		// clear redis
		rdsconn := utils.NewRedisConn()
		keys, _ := rdsconn.Dos("KEYS", clear.RdsKey)
		for _, k := range keys.([]interface{}) {
			key = string(k.([]byte))
			rdsconn.Dos("DEL", key)
		}
		rdsconn.Close()
		// clear mongo
		session.DB(utils.MongoPrefix + clear.Tag).C(clear.Term).RemoveAll(clear.MongoQuery)
		session.Close()

	}
	return nil, 0
}
