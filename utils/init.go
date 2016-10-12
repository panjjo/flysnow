package utils

import (
	"code.google.com/p/log4go"
	"errors"
	"os"
	"time"
)

var Log LogS
var PWD string

type LogS struct {
	log4go.Logger
}

func (l LogS) ERROR(s string) {
	l.Error(s)
	time.Sleep(1 * time.Second)
	os.Exit(1)
}
func (l LogS) NewErr(s string) error {
	return errors.New(s)
}

func init() {
	PWD, _ = os.Getwd()
	FSConfig = Config{}
	FSConfig.InitConfig(PWD + "/config/base.conf")
	config := FSConfig.StringDefault("logger.path", "config/logger.xml")
	Log = LogS{make(log4go.Logger)}
	Log.LoadConfiguration(config)
}
