package main

import (
	"flysnow/models"
	"flysnow/utils"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"strings"
)

var S_Expmap = map[string]sExpStruct{
	"&&": sExpStruct{-1, "bool", []string{"eq", "bool"}, " && "},
	"==": sExpStruct{2, "bool", []string{"eq", "interface"}, " == "},
	"||": sExpStruct{-1, "bool", []string{"eq", "bool"}, " || "},
	"!=": sExpStruct{2, "bool", []string{"eq", "interface"}, " != "},
	"+":  sExpStruct{-1, "float64", []string{"eq", "float64"}, " + "},
	/*"last": sExpStruct{-1, "float64", []string{"eq", "float64"}, " + "},
	"avg":  sExpStruct{-1, "float64", []string{"eq", "float64"}, " + "},*/
	/*"-":  sExpStruct{-1, "float64", "eq", " - "},*/
}

type sExpStruct struct {
	paramnum    int      //参数个数
	return_type string   // 返回类型
	child_type  []string //请求参数类型 eq 表示类型一致即可
	split       string   // 接串连接符
}

var suffix = ".json"
var PWD, DataPath, TmpPath string
var ParserMap map[string]models.Json // 存放统计项配置文件数据
var baseStrMap map[string]string     // tmp/base.go 文件代码串
var datastruct dataStruct            //单配置文件的解析数据
var termstruct termStruct
var termConfigMap map[string]map[string]models.TermConfig

type dataStruct struct {
	name       string                         //名称
	termname   string                         //当前解析的term名称
	request    map[string]interface{}         //请求参数
	requeststr string                         //请求参数组装的go代码
	term       []termStruct                   //数据源下面的统计项列表
	funcsmap   map[string]models.FSFuncStruct //系统函数的列表
	funcstr    string                         //系统函数组装的go代码
	data       models.Json                    //统计项源数据
}
type termStruct struct {
	name string
	exec string
}

func formatErr(arges ...interface{}) {
	arges = append(arges, []interface{}{datastruct.name, datastruct.termname}...)
	fmt.Println(arges...)
	os.Exit(1)
}
func formatInfo(arges ...interface{}) {
	arges = append(arges, []interface{}{datastruct.name, datastruct.termname}...)
	fmt.Println(arges...)
}
func formatWarn(arges ...interface{}) {
	arges = append(arges, []interface{}{datastruct.name, datastruct.termname}...)
	fmt.Println(arges...)
}

func main() {
	/*defer func() {
		if err := recover(); err != nil {
			fmt.Printf("err: %v\n", err) // 这里的err其实就是panic传入的内容，55
		}
	}()*/
	baseStrMap = map[string]string{"termlistmap": ""}
	termConfigMap = map[string]map[string]models.TermConfig{}
	PWD, _ = os.Getwd()
	ParserMap = map[string]models.Json{}
	DataPath = PWD + "/data"
	TmpPath = PWD + "/tmp"
	suffix = strings.ToUpper(suffix)
	//检查文件目录是否存在并合法  data/  tmp/
	checkPath()
	// 复制生成 tmp/main.go
	copyMainFile()
	// 加载统计项配置文件
	parserJsonFile(DataPath)
	//解析配置文件
	parserJson()
	formatInfo("parser finish")
}

func parserJson() {
	//循环所有配置文件
	for name, v := range ParserMap {
		datastruct = dataStruct{funcsmap: map[string]models.FSFuncStruct{}, term: []termStruct{}, data: v}
		datastruct.name = name
		//解析请求数据
		parserDataRequest(v.Reqdata)
		//解析过滤器
		parserFuncFilter(v.Filter)
		// tmp/base.go 下的 TermListMap 组装代码
		baseStrMap["termlistmap"] += "\"" + name + "\":&DATATERM{\nData:New" + strings.ToUpper(name) + ",\nTerms:[]func(t interface{}){\n"
		//循环各统计项
		for _, term := range v.Term {
			setBaseTermMap(name, term)
			datastruct.termname = term.Name
			//检查term 的key 所需要的参数是否存在
			checkTermKey(term)
			termstruct = termStruct{}
			termstruct.name = term.Name
			// 解析exec 操作集合
			termstruct.exec = complexExec(term)
			baseStrMap["termlistmap"] += "termmap[\"" + name + term.Name + "\"].Exec,"
			datastruct.term = append(datastruct.term, termstruct)
		}
		baseStrMap["termlistmap"] += "},},"
		writeBaseFile()
		writeTermFile()
		formatInfo("parse", name, "succ")
	}
}
func writeTermFile() {
	waitwritestr := ""
	filename := TmpPath + "/" + datastruct.name + ".go"
	tm, _ := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, os.ModePerm)
	defer tm.Close()
	if mm, err := ioutil.ReadFile(PWD + "/models/tmp.data.tmpl"); err != nil {
		formatErr("parser tmp.data.tmpl fail:", err)
	} else {
		str := string(mm)
		str = strings.Replace(str, "{{name}}", strings.ToUpper(datastruct.name), -1)
		str = strings.Replace(str, "{{requestdata}}", datastruct.requeststr, -1)
		waitwritestr += str
	}
	waitwritestr += datastruct.funcstr
	for _, term := range datastruct.term {
		if mm, err := ioutil.ReadFile(PWD + "/models/tmp.term.tmpl"); err != nil {
			formatErr("parser tmp.term.tmpl fail:", err)
		} else {
			str := string(mm)
			str = strings.Replace(str, "{{termname}}", strings.ToUpper(datastruct.name+term.name), -1)
			str = strings.Replace(str, "{{name}}", strings.ToUpper(datastruct.name), -1)
			str = strings.Replace(str, "{{term}}", term.name, -1)
			str = strings.Replace(str, "{{exec}}", term.exec, -1)
			waitwritestr += str
		}
	}
	tm.WriteString(waitwritestr)
}
func parserFuncFilter(fs []models.FSFilter) {
	//解析过滤器
	funcstruct := models.FSFuncMap["filter"]
	for _, f := range fs {
		if _, ok := utils.DurationMap[f.Duration]; !ok {
			formatErr("parer ", datastruct.name, " ", termstruct.name, " funcs err: filter.duration must in [s,d,m,y] get", f.Duration)
		}
		str := funcstruct.FuncBody
		str = strings.Replace(str, "{{name}}", strings.ToUpper(datastruct.name+f.Name), -1)
		str = strings.Replace(str, "{{names}}", datastruct.name+f.Name, -1)
		baseStrMap["init"] += "tmp" + datastruct.name + f.Name + `:=&` + strings.ToUpper(datastruct.name+f.Name) + fmt.Sprintf(`{utils.FSBtree,%d,%d,"%s","%s"}
    %s=tmp%s.Do
    `, f.OffSet, f.Whence, f.Name, f.Duration, datastruct.name+f.Name, datastruct.name+f.Name)
		datastruct.funcsmap[f.Name] = funcstruct
		//组装方法代码串
		datastruct.funcstr += str
	}

}
func complexExec(term models.Term) (str string) {
	for _, e := range term.Execs {
		isif := false
		tmpstr := ""
		if len(e.Filter) > 0 {
			//解析条件
			ifstr, res_type := complexTerm(e.Filter)
			if res_type != "bool" {
				//条件返回结果不是bool类型 报错
				formatErr("Filter Error: return type must bool but get", res_type, "filter:", e.Filter)
			}
			tmpstr = "if " + ifstr + "{\n"
			isif = true
		}
		if len(e.Do) > 0 {
			//解析do操作
			for _, d := range e.Do {
				tmpstr += complexTermDo(d) + "\n"
			}
		}
		if isif {
			tmpstr += "}\n"
		}
		str += tmpstr
	}
	return str
}
func complexTermDo(f []interface{}) string {
	if len(f) < 2 {
		formatErr("Complex DoErr:request params num must be greater than 2,get:", len(f), f)
	}
	car, ok := f[0].(string)
	//op 必须为string
	if !ok {
		formatErr("Complex ExecErr: op not found", f)
	}
	switch car {
	case "+", "avg", "last":
	default:
		formatErr("Complex ExecErr: op not found", f)
	}
	var fkn string //更新键名
	var tvl string //更新键的值
	//第一个参数必须为string,为redis操作的key
	first, ok := f[1].(string)
	if first == "" {
		formatErr("Complex ExecErr: op the first params is empoty", f)
	}

	if first[:1] == "@" {
		//传入参数的判断
		fp := first[1:]
		if drt, ok := datastruct.request[fp]; ok {
			switch strings.ToLower(drt.(string)) {
			//特殊结构
			case "$listkv":
				//循环计算,直接替换返回
				return strings.Replace(models.FSFuncMap["listkv"].FuncBody, "{{name}}", strings.ToUpper(fp), -1)
			default:
				//简单取值
				fkn = "d.req." + strings.ToUpper(fp)
			}
		} else {
			// 没有这个key
			formatErr("Complex ExecErr: not found key,", fp, f)
		}
	} else {
		//普通key
		fkn = fmt.Sprintf(`"%s"`, first)
	}
	//spkey 写入termconfig
	switch car {
	case "avg", "last":
		spkey := termConfigMap[datastruct.name][datastruct.termname]
		spkey.SpKey[first] = car
		termConfigMap[datastruct.name][datastruct.termname] = spkey

	}

	//从除op外第二个参数开始都可以当做是+的元素处理
	str, returntype := complexTerm(append([]interface{}{"+"}, f[2:]...))
	if returntype == "float64" {
		tvl = str
	} else {
	}

	return fmt.Sprintf(`commands.Commands=append(commands.Commands,utils.RdsCommand{Cmd:"HINCRBYFLOAT",V:[]interface{}{%s,%s}})`, fkn, tvl)
}

//解析操作方法 [+...,Key1,Key2...]
func complexTerm(f []interface{}) (str string, return_type string) {
	//获取op
	if len(f) < 2 {
		formatErr("Complex ExecErr: params must be greater than one,", f)
	}

	car := fmt.Sprintf("%v", f[0])
	cartype := "op"
	returntype := ""
	paramsList := []string{}
	funcname := ""

	if len(car) > 0 && car[:1] == "$" {
		//特殊方法
		cartype = "func"
		//特殊操作符
		//判断特殊函数是否存在
		if fsf, ok := datastruct.funcsmap[car[1:]]; ok {
			//判断输入参数是否正确
			if len(f)-1 != len(fsf.Paramstype) {
				formatErr("Complex ExecErr: func need ", len(fsf.Paramstype), "params,but get ", len(f)-1, ".", f)
			}
			funcname = fsf.Name
			str = datastruct.name + car[1:] + "("
			paramsList = fsf.Paramstype
			returntype = fsf.ReturnType[0]
		} else {
			// 特殊方法不存在
			formatErr("Complex ExecErr: func not found", f)
		}
	} else {
		//基础操作符
		if v, ok := S_Expmap[car]; ok {
			if v.paramnum != -1 && v.paramnum != len(f)-1 {
				//op item数量与期望不匹配
				formatErr("Complex ExecErr: op need ", v.paramnum, "params,but get ", len(f)-1, ".", f)
			}
			paramsList = v.child_type
			returntype = v.return_type
		} else {
			// 基础操作不存在
			formatErr("Complex ExecErr: op not found", f)
		}
	}
	tmpparams := []string{}
	//检测输入参数列表的各个数据类型是否一致
	for k, p := range f[1:] {
		fkn := "" // 代码串
		fpt := "" //类型字符串
		switch p.(type) {
		case string:
			fkn = fmt.Sprintf(`"%s"`, p)
			fpt = "string"
			if len(p.(string)) > 0 && p.(string)[:1] == "@" {
				fp := p.(string)[1:]
				if drt, ok := datastruct.request[fp]; ok {
					fpt = drt.(string)
					fkn = "d.req." + strings.ToUpper(p.(string)[1:])
				} else {
					// 没有这个key
					formatErr("Complex ExecErr: not found key,", f)
				}
			}
		case int:
			fkn = fmt.Sprintf("%d", p.(int))
			fpt = "int"
		case int64:
			fkn = fmt.Sprintf("%d", p.(int64))
			fpt = "int64"
		case float32:
			fkn = fmt.Sprintf("%v", p)
			fpt = "float32"
		case float64:
			fkn = fmt.Sprintf("%v", p)
			fpt = "float64"
		case bool:
			fkn = fmt.Sprintf("%v", p.(bool))
			fpt = "bool"
		case []interface{}:
			//多层
			// 解析下一层
			//TODO:多层的下层参数类型 默认使用了上级的
			fkn, fpt = complexTerm(p.([]interface{}))
		default:
			formatErr("Complex ExecError: unknown type,", reflect.TypeOf(p).Name(), f)
		}
		//判断类型是否一致
		if paramsList[0] == "eq" {
			if len(paramsList) == 2 {
				//已有子方法类型
				if paramsList[1] == "interface" {
					//interface类型 赋值参数类型
					paramsList[1] = fpt
				} else {
					if paramsList[1] != fpt {
						formatErr("Complex ExecErr: params type want", p, "have", fpt, f)
					}
				}
			} else {
				//没有子类型添加子类型
				paramsList = append(paramsList, fpt)
			}
		} else {
			if paramsList[k] != fpt {
				//数据类型不一致
				formatErr("Complex ExecErr: params type want", p, "have", fpt, f)
			}
		}
		tmpparams = append(tmpparams, fkn)
	}
	if cartype == "func" {
		str += strings.Join(tmpparams, ",")
		if funcname == "filter" {
			//filter 自动添加stime参数
			str += ",d.req.STime"
		}
		str += ")"
	} else {
		str += strings.Join(tmpparams, S_Expmap[car].split)
	}
	return str, returntype
}
func checkTermKey(term models.Term) {
	//检查term的key中所用到的请求参数字段是否都存在
	for _, i := range term.Key {
		if i[:1] == "@" {
			if _, ok := datastruct.request[i[1:]]; !ok {
				formatErr("parse", datastruct.name, ":", term.Name, ".Key ERROR:not found key", i)
			}
		}
	}
}

func parserDataRequest(s interface{}) {
	filestr := ""
	datastruct.request = s.(map[string]interface{})
	for k, t := range datastruct.request {
		if t.(string)[:1] == "$" {
			//特殊数据结构
			filestr += strings.ToUpper(k) + "  models." + strings.ToUpper(t.(string)[1:]) + "\n"
		} else {
			filestr += strings.ToUpper(k) + "  " + t.(string) + "\n"
		}
	}
	filestr += "STime int64 `json:\"s_time\"`\n"
	//拼装请求数据go代码串
	datastruct.requeststr = filestr
}
func writeBaseFile() {
	tm, _ := os.OpenFile(TmpPath+"/base.go", os.O_RDWR|os.O_CREATE, os.ModePerm)
	defer tm.Close()
	if mm, err := ioutil.ReadFile(PWD + "/models/tmp.base.tmpl"); err != nil {
		formatErr("parser base.go fail:", err)
	} else {
		str := string(mm)
		str = strings.Replace(str, "{{termmap}}", baseStrMap["termmap"], -1)
		str = strings.Replace(str, "{{init}}", baseStrMap["init"], -1)
		str = strings.Replace(str, "{{termlistmap}}", baseStrMap["termlistmap"], -1)
		str = strings.Replace(str, "{{termconfigstr}}", string(utils.JsonEncode(termConfigMap, true)), -1)
		tm.WriteString(str)
	}
}
func setBaseTermMap(name string, term models.Term) {
	if _, ok := baseStrMap["termmap"]; ok {
		baseStrMap["termmap"] += fmt.Sprintf(`"%s":&%s{rotateat:utils.GetNowSec()},`, name+term.Name, strings.ToUpper(name+term.Name))
	} else {
		baseStrMap["termmap"] = fmt.Sprintf(`"%s":&%s{},`, name+term.Name, strings.ToUpper(name+term.Name))
	}
	if _, ok := termConfigMap[name]; !ok {
		termConfigMap[name] = map[string]models.TermConfig{}
	}
	termConfigMap[name][term.Name] = models.TermConfig{Name: term.Name, Key: term.Key, IsSnow: term.IsSnow, Snow: term.Snow, SpKey: map[string]string{}}
}

func copyMainFile() {
	tm, _ := os.OpenFile(TmpPath+"/main.go", os.O_RDWR|os.O_CREATE, os.ModePerm)
	defer tm.Close()
	if mm, err := ioutil.ReadFile(PWD + "/models/tmp.main.tmpl"); err != nil {
		formatErr("parser main.go fail:", err)
	} else {
		tm.Write(mm)
	}
}
func checkPath() {
	PWD, _ = os.Getwd()
	DataPath = PWD + "/data"
	TmpPath = PWD + "/tmp"
	if !utils.FileOrPathIsExist(DataPath) {
		formatErr("data path not found:", DataPath)
	}
	if utils.FileOrPathIsExist(TmpPath) {
		formatInfo("tmp path found:", TmpPath, "clear it")
		os.RemoveAll(TmpPath)
	}
	utils.CreatePathAll(TmpPath)
}
func parserJsonFile(path string) {
	if dirs, err := ioutil.ReadDir(path); err == nil {
		if path != DataPath {
			for _, dir := range dirs {
				if !dir.IsDir() {
					if strings.HasSuffix(strings.ToUpper(dir.Name()), suffix) {
						if strings.ToLower(dir.Name()) == "main.json" {
							assemble(path+"/"+dir.Name(), true)
						} else {
							assemble(path+"/"+dir.Name(), false)
						}
					}
				}
			}
		}
		for _, dir := range dirs {
			if dir.IsDir() {
				parserJsonFile(path + "/" + dir.Name())
			}
		}
	}

}
func assemble(file string, header bool) {
	b, _ := ioutil.ReadFile(file)
	if header {
		//各数据源头文件
		json := models.Json{}
		e := utils.JsonDecode(b, &json)
		if e != nil {
			formatErr("load json file:", file, " error:", e.Error())
		}
		if v, ok := ParserMap[json.Name]; ok {
			json.Term = v.Term
			ParserMap[json.Name] = json
		} else {
			ParserMap[json.Name] = json
		}
	} else {
		//各统计项文件
		json := models.Term{}
		e := utils.JsonDecode(b, &json)
		if e != nil {
			formatErr("load json file:", file, " error:", e.Error())
		}
		dirlist := strings.Split(file, "/")
		belang := dirlist[len(dirlist)-2]
		if v, ok := ParserMap[belang]; ok {
			v.Term = append(v.Term, json)
			ParserMap[belang] = v
		} else {
			ParserMap[belang] = models.Json{Term: []models.Term{json}}
		}
	}
}
