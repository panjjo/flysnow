package main

import (
	"fmt"
	"github.com/panjjo/flysnow/models"
	"github.com/panjjo/flysnow/tmp"
	"github.com/panjjo/flysnow/utils"
	"gopkg.in/mgo.v2/bson"
	"os"
	"sync"
)

func main() {
	updateToRelesase1200()
}

type Indexs struct {
	Key   string                   `bson:"s_key"`
	ETime int64                    `bson:"e_time"`
	STime int64                    `bson:"s_time"`
	Index map[string]interface{}   `bson:"index"`
	Term  string                   `bson:"term"`
	Tag   string                   `bson:"tag"`
	Data  []map[string]interface{} `bson:"data"`
}

// 从release1000升级到release1200
func updateToRelesase1200() {
	PWD, _ := os.Getwd()
	fmt.Println(PWD)
	utils.FSConfig = utils.Config{}
	utils.FSConfig.InitConfig(PWD + "/config/base.conf")
	utils.FSConfig.SetMod("sys")
	wg := sync.WaitGroup{}
	tmp.Init()
	for tag, terms := range models.TermConfigMap {
		utils.MgoInit(tag)
		for term, _ := range terms {
			fmt.Println("start update tag:", tag, "term:", term)
			ms := utils.MgoSessionDupl(tag).DB(models.MongoDT + tag)
			msIndex := ms.C(models.MongoIndex + term)
			var n int
			for {
				indexs := []Indexs{}
				msIndex.Find(bson.M{}).Skip(n).Limit(100).All(&indexs)
				fmt.Println(len(indexs),tag,term,n,msIndex.FullName)
				go func(objs []Indexs,tag,term string) {
					wg.Add(1)
					defer wg.Done()
					fmt.Println(tag)
					mss := utils.MgoSessionDupl(tag)
					defer mss.Close()
					mssIndex := mss.DB(models.MongoDT + tag).C(models.MongoIndex + term)
					mssObj := mss.DB(models.MongoDT + tag).C(models.MongoOBJ + term)
					for _, obj := range objs {
						mssIndex.Remove(bson.M{"$or":[]bson.M{bson.M{"s_key": obj.Key},bson.M{"key":obj.Key}}})
						mssIndex.Insert(bson.M{
							"key":    obj.Key,
							"e_time": obj.ETime,
							"s_time": obj.STime,
							"index":  obj.Index,
							"term":   obj.Term,
							"tag":    obj.Tag,
						})
						for _, d := range obj.Data {
							mv := map[string]interface{}{"key": obj.Key}
							for k, v := range d {
								mv[k] = v
							}
							mssObj.Insert(mv)
						}
					}
				}(indexs,tag,term)
				if len(indexs) < 100 {
					break
				}
				n = n + 100
			}
		}
	}
	wg.Wait()
}
