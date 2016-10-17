package utils

import (
	"github.com/garyburd/redigo/redis"
	"time"
)

var redispool map[string]*redis.Pool

type RedisConn struct {
	Con redis.Conn
}

func NewRedisConn(tag string) *RedisConn {
	return &RedisConn{redispool[tag].Get()}
}
func (r *RedisConn) Dos(cmd string, args ...interface{}) (result interface{}, err error) {
	result, err = r.Con.Do(cmd, args...)
	if err != nil {
		Log.Error(err)
	}
	return
}

func (r *RedisConn) Sends(cmd string, args ...interface{}) error {
	err := r.Con.Send(cmd, args...)
	if err != nil {
		Log.Error(err)
	}
	return err
}

func (r *RedisConn) Close() {
	r.Con.Close()
}

/*
生成redis连接池
*/
func newRedisPool(server string, maxidle, maxactive int) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     maxidle,
		MaxActive:   maxactive,
		IdleTimeout: 240 * time.Second,
		Wait:        true,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", server)
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
	redispool = map[string]*redis.Pool{}
	FSConfig.SetMod(tag)
	rdshost := FSConfig.StringDefault("redis.Host", "192.168.1.9:6379")
	rdsmaxpool := FSConfig.IntDefault("redis.MaxPoolConn", 100)
	rdsmaxactive := FSConfig.IntDefault("redis.MaxActive", 100)
	redispool[tag] = newRedisPool(rdshost, rdsmaxpool, rdsmaxactive)
}
