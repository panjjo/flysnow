package utils

import (
	"time"

	"fmt"
	"github.com/garyburd/redigo/redis"
)

var redispool = map[string]*redis.Pool{}

type RedisConn struct {
	Con redis.Conn
}

func NewRedisConn(tag string) *RedisConn {
	return &RedisConn{redispool[tag].Get()}
}
func (r *RedisConn) Dos(cmd string, args ...interface{}) (result interface{}, err error) {
	result, err = r.Con.Do(cmd, args...)
	if err != nil {
		Log.Error(err.Error())
	}
	return
}

func (r *RedisConn) Sends(cmd string, args ...interface{}) error {
	err := r.Con.Send(cmd, args...)
	if err != nil {
		Log.Error(err.Error())
	}
	return err
}

func (r *RedisConn) Close() {
	r.Con.Close()
}

type RedisConfig struct {
	Server             string
	MaxIdle, MaxActive int
	DB                 int
}

/*
生成redis连接池
*/
func newRedisPool(conf *RedisConfig) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     conf.MaxIdle,
		MaxActive:   conf.MaxActive,
		IdleTimeout: 240 * time.Second,
		Wait:        true,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", conf.Server, redis.DialDatabase(conf.DB))
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
func InitRedis(tag string) {
	if redispool[tag] == nil {
		FSConfig.SetMod(tag)
		config := RedisConfig{
			Server:    FSConfig.StringDefault("redis.Host", "192.168.1.9:6379"),
			MaxActive: FSConfig.IntDefault("redis.MaxActive", 100),
			MaxIdle:   FSConfig.IntDefault("redis.MaxPoolConn", 100),
			DB:        FSConfig.IntDefault("redis.DB", 0),
		}
		redispool[tag] = newRedisPool(&config)
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

func RdsBatchCommands(tag string, commands []*RdsSendStruct) {
	conn := NewRedisConn(tag)
	defer conn.Close()
	conn.Dos("MULTI")
	defer conn.Dos("EXEC")
	var k string
	for _, cs := range commands {
		k = cs.Key
		fmt.Println(k, cs.Commands)
		for _, c := range cs.Commands {
			c.V = append([]interface{}{k}, c.V...)
			conn.Sends(c.Cmd, c.V...)
		}
	}

}
