package main

import (
	"flysnow/models"
	"flysnow/utils"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"regexp"
	"strings"
	"time"
)

var suffix = ".json"

var ParserMap map[string]models.Json

var PWD, DataPath, TmpPath string
var log utils.LogS

func main() {
	log = utils.Log
	ParserMap = map[string]models.Json{}
	suffix = strings.ToUpper(suffix)
	PWD, _ = os.Getwd()
	DataPath = PWD + "/data"
	TmpPath = PWD + "/tmp"

	//clean tmp
	log.Info("Clean tmp:%v", TmpPath)

	getfiles(DataPath, true)
	parser()
	log.Info("parser tmp:succ")
	time.Sleep(1 * time.Second)
}
func parser() {
	basestr := "package tmp\n import \"flysnow/models\" \n import \"flysnow/utils\"\n"
	interfacemap := "var InterfaceMap = map[string]map[string]models.TermInterface{\n"
	termconfigmap := map[string]map[string]models.TermConfig{}
	for name, v := range ParserMap {
		interfacemap += "\"" + name + "\":map[string]models.TermInterface{\n"
		termconfigmap[name] = map[string]models.TermConfig{}
		utils.CreatePathAll(TmpPath + "/" + name)
		file := TmpPath + "/" + name + "/" + name + ".go"
		//组装头
		filestr := "package " + v.Name + "\n"
		filestr += `import (
    "flysnow/models"
    "flysnow/utils"
    "flysnow/snow"
    )
    `
		//请求结构体
		filestr += "type RequestData struct{\n"
		for k, t := range v.Reqdata.(map[string]interface{}) {
			filestr += strings.ToUpper(k) + "  " + t.(string) + "\n"
		}
		filestr += "STime int64 `json:\"s_time\"`\n"
		filestr += "}\n"
		//各个term
		for _, term := range v.Term {
			if len(term.Snow) != 0 {
				term.IsSnow = true
			}
			//term struct
			filestr += "type " + strings.ToUpper(term.Name) + "  struct { \n Request *RequestData \n Config *models.TermConfig\n"
			filestr += "Result *" + strings.ToUpper(term.Name) + "Result\n"
			filestr += "}\n"
			//返回结构体
			filestr += "type " + strings.ToUpper(term.Name) + "Result  struct { \n"
			for tk, tt := range term.Result.(map[string]interface{}) {
				filestr += strings.ToUpper(tk) + "  " + tt.(string) + "\n"
			}
			filestr += "}\n"
			//内嵌函数 exec
			filestr += "func(t *" + strings.ToUpper(term.Name) + ") Exec(body []byte){\n"
			if len(term.Execs) > 0 {
				filestr += "request:=t.TransReq(body)\n"
				filestr += "redisconn:=utils.NewRedisConn(\"" + strings.ToLower(name) + "\")\n"
				filestr += `defer redisconn.Close()
				keys := utils.GetKey(*request, t.Config.Key)
				key := models.RedisKT+"_` + strings.ToLower(name) + `_"+keys.Key
        `
				filestr += ""
				filestr += "snow.Rotate(&snow.SnowSys{Key: key,Now:request.STime, Index: keys.Index, Tag: \"" + strings.ToLower(name) + "\", Term: \"" + strings.ToLower(term.Name) + "\", RedisConn: redisconn}, t.Config.Snow)\n"
				filestr += `
        redisconn.Dos("MULTI")
        defer redisconn.Dos("EXEC")
        `
			}

			for _, ex := range term.Execs {
				filestr += "if " + strings.Replace(stringReplaceRegexp(ex.Filter, strings.ToUpper), "@", "request.", -1) + "{\n"
				for _, d := range ex.Do {
					var dvalue string
					switch reflect.TypeOf(d.Value).Name() {
					case "string":
						if len(d.Value.(string)) != 0 && d.Value.(string)[0:1] == "@" {
							dvalue = "request." + strings.ToUpper(d.Value.(string)[1:])
						} else {
							dvalue = d.Value.(string)
						}
					case "int":
						dvalue = fmt.Sprintf("%d", d.Value.(int))
					case "int64":
						dvalue = fmt.Sprintf("%d", d.Value.(int64))
					case "float64":
						dvalue = fmt.Sprintf("%f", d.Value.(float64))
					}
					if len(d.Name) != 0 && d.Name[0:1] == "@" {
						d.Name = "request." + strings.ToUpper(d.Name[1:])
					} else {
						d.Name = "\"" + strings.ToLower(d.Name) + "\""
					}
					switch strings.ToLower(d.Op) {
					case "sum":
						filestr += "redisconn.Sends(\"HINCRBYFLOAT\",key," + d.Name + "," + dvalue + ")\n"
					}
				}
				filestr += "}\n"
			}
			filestr += "}\n"
			//transreq func
			filestr += "func(t *" + strings.ToUpper(term.Name) + ")TransReq(body []byte)*RequestData{\n"
			filestr += "req:=&RequestData{}\n"
			filestr += "utils.JsonDecode(body,req)\n"
			filestr += `
      if req.STime==0{
        req.STime=utils.GetNowSec()
        }
      `
			filestr += "return req\n}\n"
			//config func
			filestr += "func(t *" + strings.ToUpper(term.Name) + ")SetConfig(c *models.TermConfig){\n"
			filestr += "t.Config=c\n}\n"
			interfacemap += "\"" + term.Name + "\":&" + name + "." + strings.ToUpper(term.Name) + "{},\n"
			termconfigmap[name][term.Name] = models.TermConfig{Name: term.Name, Key: term.Key, IsSnow: term.IsSnow, Snow: term.Snow}
		}
		interfacemap += "}"
		if err := ioutil.WriteFile(file, []byte(filestr), os.ModePerm); err != nil {
			log.Warn("parser write tmp file err:%v", err)
		} else {
			basestr += "import \"flysnow/tmp/" + v.Name + "\"\n"
		}
	}
	interfacemap += "}\n"
	termconfigstr := "var termconfigstr=`" + string(utils.JsonEncode(termconfigmap, true)) + "`\n"
	termparserfunc := `
  var TagList []string
  func init(){ 
    models.TermConfigMap = map[string]map[string]*models.TermConfig{}
    utils.JsonDecode([]byte(termconfigstr),&models.TermConfigMap)
    for k,v:=range InterfaceMap{
      TagList=append(TagList,k)
      for k1,v1:=range v{
        v1.SetConfig(models.TermConfigMap[k][k1])
        }
      }
    }
  `
	if err := ioutil.WriteFile(TmpPath+"/base.go", []byte(basestr+interfacemap+termconfigstr+termparserfunc), os.ModePerm); err != nil {
		log.Warn("parser write base file err:%v", err)
	}
}

func getfiles(path string, header bool) {
	if dirs, err := ioutil.ReadDir(path); err == nil {
		for _, dir := range dirs {
			if !dir.IsDir() {
				if strings.HasSuffix(strings.ToUpper(dir.Name()), suffix) {
					assemble(path+"/"+dir.Name(), header)
				}
			}
		}
		for _, dir := range dirs {
			if dir.IsDir() {
				getfiles(path+"/"+dir.Name(), false)
			}
		}
	}

}
func assemble(file string, header bool) {
	b, _ := ioutil.ReadFile(file)
	if header {
		json := models.Json{}
		e := utils.JsonDecode(b, &json)
		if e != nil {
			log.ERROR("load json file:" + file + " error:" + e.Error())
		}
		ParserMap[json.Name] = json
	} else {
		json := models.Term{}
		e := utils.JsonDecode(b, &json)
		if e != nil {
			log.ERROR("load json file:" + file + " error:" + e.Error())
		}
		dirlist := strings.Split(file, "/")
		belang := dirlist[len(dirlist)-2]
		if v, ok := ParserMap[belang]; ok {
			v.Term = append(v.Term, json)
			ParserMap[belang] = v
		} else {
			log.ERROR("not found header json:" + belang)
		}
	}
}
func stringReplaceRegexp(str string, f func(s string) string) string {
	re, _ := regexp.Compile(`@[a-z]+`)
	return re.ReplaceAllStringFunc(str, f)
}
