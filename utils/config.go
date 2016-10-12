package utils

import (
	"bufio"
	"io"
	"os"
	"strconv"
	"strings"
)

const middle = "========="

var FSConfig Config
var fsctags map[string]int

type Config struct {
	Mymap       map[string]string
	strcet, Mod string
}

func (c *Config) SetMod(tag string) {
	if _, ok := fsctags[tag]; ok {
		c.Mod = tag
	} else {
		c.Mod = "sys"
	}

}

func (c *Config) InitConfig(path string) {
	c.Mymap = make(map[string]string)
	fsctags = map[string]int{}

	f, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	r := bufio.NewReader(f)
	for {
		b, _, err := r.ReadLine()
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}

		s := strings.TrimSpace(string(b))
		//fmt.Println(s)
		if strings.Index(s, "#") == 0 {
			continue
		}

		n1 := strings.Index(s, "[")
		n2 := strings.LastIndex(s, "]")
		if n1 > -1 && n2 > -1 && n2 > n1+1 {
			c.strcet = strings.TrimSpace(s[n1+1 : n2])
			continue
		}

		if len(c.strcet) == 0 {
			continue
		}
		index := strings.Index(s, "=")
		if index < 0 {
			continue
		}

		frist := strings.TrimSpace(s[:index])
		if len(frist) == 0 {
			continue
		}
		second := strings.TrimSpace(s[index+1:])

		pos := strings.Index(second, "\t#")
		if pos > -1 {
			second = second[0:pos]
		}

		pos = strings.Index(second, " #")
		if pos > -1 {
			second = second[0:pos]
		}

		pos = strings.Index(second, "\t//")
		if pos > -1 {
			second = second[0:pos]
		}

		pos = strings.Index(second, " //")
		if pos > -1 {
			second = second[0:pos]
		}

		if len(second) == 0 {
			continue
		}

		key := c.strcet + middle + frist
		fsctags[c.strcet] = 1
		c.Mymap[key] = strings.TrimSpace(second)
	}
}

func (c Config) StringDefault(k, d string) string {
	k = c.Mod + middle + k
	v, found := c.Mymap[k]
	if !found {
		return d
	}
	return v
}

func (c Config) String(k string) string {
	k = c.Mod + middle + k
	v, found := c.Mymap[k]
	if !found {
		return ""
	}
	return v
}

func (c Config) IntDefault(k string, d int) int {
	k = c.Mod + middle + k
	v, found := c.Mymap[k]
	if !found {
		return d
	}
	i, _ := strconv.Atoi(v)
	return i
}

func (c Config) Int(k string) int {
	k = c.Mod + middle + k
	v, found := c.Mymap[k]
	if !found {
		return 0
	}
	i, _ := strconv.Atoi(v)
	return i
}
