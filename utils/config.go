package utils

import (
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"strings"
)

type config struct {
	MQ            *RabbitmqConfig `json:"rabbitmq" yaml:"rabbitmq" mapstructure:"rabbitmq"`
	RDS           *RDSConfig      `json:"redis" yaml:"redis" mapstructure:"redis"`
	Mgo           *MgoConfig      `json:"mongo" yaml:"mongo" mapstructure:"mongo"`
	LogLevel      string          `json:"logger" yaml:"logger" mapstructure:"logger"`
	AutoRotate    string          `json:"autoRotate" yaml:"autoRotate" mapstructure:"autoRotate"`
	DataPath      string          `json:"dataPath" yaml:"dataPath" mapstructure:"dataPath"`
	Listen        string          `json:"listen" yaml:"listen" mapstructure:"listen"`
	MaxRotateNums int             `json:"maxRotateNums" yaml:"maxRotateNums" mapstructure:"maxRotateNums"`
}

var Config *config

func LoacConfig() {
	viper.SetConfigType("yaml")
	viper.SetConfigName("config")
	viper.AddConfigPath("./")
	viper.SetDefault("logger", "debug")
	viper.SetDefault("dataPath", "./btreefiles")
	viper.SetDefault("autoRotate", "0 0 1 * * *")
	viper.SetDefault("rabbitmq", DefaultMQConfig)
	viper.SetDefault("redis", DefaultRDSConfig)
	viper.SetDefault("mongo", DefaultMgoConfig)
	viper.SetDefault("listen", ":22258")
	viper.SetDefault("maxRotateNums", 50)

	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	err := viper.ReadInConfig()
	if err != nil {
		logrus.Fatalln("init config error:", err)
	}
	logrus.Infoln("init config ok")
	Config = &config{}
	err = viper.Unmarshal(&Config)
	if err != nil {
		logrus.Fatalln("init config unmarshal error:", err)
	}
	logrus.Debugf("config :%+v", Config)
	logrus.Debugf("mongo :%+v", Config.Mgo)
	logrus.Debugf("redis :%+v", Config.RDS)
	logrus.Debugf("rabbitmq :%+v", Config.MQ)
	level, _ := logrus.ParseLevel(Config.LogLevel)
	logrus.SetLevel(level)
	MongoPrefix = Config.Mgo.Prefix
	RDSPrefix = Config.RDS.Prefix
}
