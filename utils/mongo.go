package utils

import (
	"github.com/sirupsen/logrus"
	"gopkg.in/mgo.v2"
)

var mongoQuery *mgo.Query
var MongoSession *mgo.Session
var mongoSessionDuplType string

var MongoPrefix string
// mongo 数据索引表前缀
var MongoIndex = ""
// mongo 数据元素表前缀
var MongoOBJ = "obj"

var DefaultMgoConfig *MgoConfig = &MgoConfig{
	Host:   "mongo://mongo.base:27017",
	Dupl:   "clone",
	Prefix: "FS",
}

type MgoConfig struct {
	Host   string `json:"host" yaml:"host" mapstructure:"host"`
	Dupl   string `json:"dupl" yaml:"mongo" mapstructure:"dupl"`
	Prefix string `json:"prefix" yaml:"prefix" mapstructure:"prefix"`
}

func MgoInit(config *MgoConfig) {
	if config == nil {
		config = DefaultMgoConfig
	}
	mongoSessionDuplType = config.Dupl
	MongoPrefix = config.Prefix
	var err error
	// Let's try to connect to Mongo DB right upon starting revel but don't
	// raise an error. Errors will be handled if there is actually a request
	MongoSession, err = mgo.Dial(config.Host)
	if err != nil {
		// Only warn since we'll retry later for each request
		logrus.Fatalf("Could not connect to Mongo DB. Error: %s,Host:%s", err, config.Host)
	}
}

func MgoSessionDupl() *mgo.Session {
	switch mongoSessionDuplType {
	case "clone":
		return MongoSession.Clone()
	case "copy":
		return MongoSession.Copy()
	case "new":
		return MongoSession.New()
	default:
		return MongoSession.Clone()
	}
}
