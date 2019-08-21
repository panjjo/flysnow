package main

import (
	"fmt"
	"github.com/panjf2000/ants"
	"github.com/panjjo/flysnow/models"
	"github.com/panjjo/flysnow/tmp"
	"github.com/panjjo/flysnow/utils"
	"gopkg.in/mgo.v2/bson"
	"os"
	"time"
)

var pool *ants.Pool

func main() {
	pool, _ = ants.NewPool(10)
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
	s:=time.Now()
	PWD, _ := os.Getwd()
	fmt.Println(PWD)
	utils.FSConfig = utils.Config{}
	utils.FSConfig.InitConfig(PWD + "/config/base.conf")
	utils.FSConfig.SetMod("sys")
	tmp.Init()
	for tag, terms := range models.TermConfigMap {
		fmt.Println("tag:",tag)
		utils.MgoInit(tag)
		for term, _ := range terms {
			fmt.Println("start update tag:", tag, "term:", term)
			ms := utils.MgoSessionDupl(tag).DB(utils.MongoPrefix + tag)
			msIndex := ms.C(models.MongoIndex + term)
			var n int
			for {
				indexs := []Indexs{}
				msIndex.Find(bson.M{"s_key": bson.M{"$exists": true}}).Skip(n).Limit(100).All(&indexs)
				fmt.Println(len(indexs), tag, term, n, msIndex.FullName)
				if len(indexs) == 0 {
					break
				}
				ss := func() {
					mss := utils.MgoSessionDupl(tag)
					defer mss.Close()
					mssIndex := mss.DB(utils.MongoPrefix + tag).C(models.MongoIndex + term)
					mssObj := mss.DB(utils.MongoPrefix + tag).C(utils.MongoOBJ + term)
					var objs []interface{}
					newIndexs := []interface{}{}
					for _, index := range indexs {
						newIndexs = append(newIndexs, bson.M{
							"key":    index.Key,
							"e_time": index.ETime,
							"s_time": index.STime,
							"index":  index.Index,
						})
						objs = []interface{}{}
						for _, d := range index.Data {
							d["key"] = index.Key
							d["index"] = index.Index
							objs = append(objs, d)
						}
						mssObj.Insert(objs...)
					}
					mssIndex.Insert(newIndexs...)
				}
				ssss := time.Now()
				pool.Submit(ss)
				fmt.Println(tag, term, time.Since(ssss))
				if len(indexs) < 100 {
					break
				}
				n = n + 100
			}
			msIndex.RemoveAll(bson.M{"s_key": bson.M{"$exists": true}})

		}
	}
	fmt.Println(time.Since(s))
}
