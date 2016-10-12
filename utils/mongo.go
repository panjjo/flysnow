package utils

import (
	"labix.org/v2/mgo"
)

var mongoQuery *mgo.Query
var MongoSession map[string]*mgo.Session
var MgoSessionDuplType map[string]string

func MgoInit(tag string) {
	var err error
	MgoSessionDuplType = map[string]string{}
	MongoSession = map[string]*mgo.Session{}
	FSConfig.SetMod(tag)
	// Let's try to connect to Mongo DB right upon starting revel but don't
	// raise an error. Errors will be handled if there is actually a request
	h := FSConfig.StringDefault("revmgo.dial", "localhost")
	MgoSessionDuplType[tag] = FSConfig.StringDefault("revmgo.dupl", "clone")
	MongoSession[tag], err = mgo.Dial(h)
	if err != nil {
		// Only warn since we'll retry later for each request
		Log.Error("Could not connect to Mongo DB. Error: %s", err)
	}
}

func MgoSessionDupl(tag string) *mgo.Session {
	switch MgoSessionDuplType[tag] {
	case "clone":
		return MongoSession[tag].Clone()
	case "copy":
		return MongoSession[tag].Copy()
	case "new":
		return MongoSession[tag].New()
	default:
		return MongoSession[tag].Clone()
	}
}
