package utils

import (
	"time"

	"github.com/sirupsen/logrus"

	"github.com/garyburd/redigo/redis"
)

const (
	RDSHINCRBYFLOAT = "HINCRBYFLOAT"
)

// redis key前缀
var RDSPrefix string

// 等待归档key集合
var RotateSetsKey string = "ROTATEKEYS"

// 等待归档单条源数据key前缀
var SRotateKeyPre string = "rotate_"
var DefaultRDSConfig *RDSConfig = &RDSConfig{
	Host:    "redis.base:6379",
	MaxConn: 100,
	DB:      1,
	Prefix:  "fs",
}

type RDSConfig struct {
	Host    string `json:"host" yaml:"host" mapstructure:"host"`
	MaxConn int    `json:"maxConn" yaml:"maxConn" mapstructure:"maxConn"`
	DB      int    `json:"db" yaml:"db" mapstructure:"db"`
	Prefix  string `json:"prefix" yaml:"prefix" mapstructure:"prefix"`
}

var redispool *redis.Pool

type RedisConn struct {
	Con redis.Conn
}

func NewRedisConn() *RedisConn {
	return &RedisConn{redispool.Get()}
}
func (r *RedisConn) Dos(cmd string, args ...interface{}) (result interface{}, err error) {
	result, err = r.Con.Do(cmd, args...)
	if err != nil {
		logrus.Fatalf("RDS DOS ERR,cmd:%s,args:%v ,err:%v", cmd, args, err)
	}
	return
}

func (r *RedisConn) Sends(cmd string, args ...interface{}) error {
	err := r.Con.Send(cmd, args...)
	if err != nil {
		logrus.Fatalf("RDS SENDS ERR,cmd:%s,args:%v ,err:%v", cmd, args, err)
	}
	return err
}

func (r *RedisConn) Close() {
	r.Con.Close()
}

/*
生成redis连接池
*/
func newRedisPool(conf *RDSConfig) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     conf.MaxConn,
		MaxActive:   conf.MaxConn,
		IdleTimeout: 240 * time.Second,
		Wait:        true,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", conf.Host, redis.DialDatabase(conf.DB))
			if err != nil {
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

/*
生成redis连接池
*/
func InitRedis(config *RDSConfig) {
	if config == nil {
		config = DefaultRDSConfig
	}
	if redispool == nil {
		redispool = newRedisPool(config)
	}
}

type RdsSendStruct struct {
	Key      string
	Commands []RdsCommand
}
type RdsCommand struct {
	Cmd string
	V   []interface{}
}

func RdsBatchCommands(commands []*RdsSendStruct) {
	conn := NewRedisConn()
	defer conn.Close()
	conn.Dos("MULTI")
	defer conn.Dos("EXEC")
	var k string
	for _, cs := range commands {
		k = cs.Key
		for _, c := range cs.Commands {
			c.V = append([]interface{}{k}, c.V...)
			conn.Sends(c.Cmd, c.V...)
		}
	}
}
