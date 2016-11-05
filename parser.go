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
	"&&": sExpStruct{-1, "bool", "bool", " && "},
	"==": sExpStruct{2, "bool", "eq", " == "},
	"||": sExpStruct{-1, "bool", "bool", " || "},
	"!=": sExpStruct{2, "bool", "eq", " != "},
	"+":  sExpStruct{-1, "interface", "eq", " + "},
}

type sExpStruct struct {
	paramnum    int
	return_type string
	child_type  string
	split       string
}

var suffix = ".json"
var PWD, DataPath, TmpPath string
var ParserMap map[string]models.Json
var baseStrMap map[string]string
var datastruct dataStruct
var termstruct termStruct
var termConfigMap map[string]map[string]models.TermConfig

type dataStruct struct {
	name       string
	request    map[string]interface{}
	requeststr string
	term       []termStruct
	funcsmap   map[string]models.FSFuncStruct
	funcstr    string
	data       models.Json
}
type termStruct struct {
	name string
	exec string
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
	checkPath()
	copyMainFile()
	parserJsonFile(DataPath)
	parserJson()
	fmt.Println("parser finish")
}

func parserJson() {
	for name, v := range ParserMap {
		datastruct = dataStruct{funcsmap: map[string]models.FSFuncStruct{}, term: []termStruct{}, data: v}
		datastruct.name = name
		parserDataRequest(v.Reqdata)
		parserFuncFilter(v.Filter)
		baseStrMap["termlistmap"] += "\"" + name + "\":&DATATERM{\nData:New" + strings.ToUpper(name) + ",\nTerms:[]func(t interface{}){\n"
		for _, term := range v.Term {
			checkTermKey(term)
			termstruct = termStruct{}
			termstruct.name = term.Name
			termstruct.exec = complexExec(term)
			baseStrMap["termlistmap"] += "termmap[\"" + name + term.Name + "\"].Exec,"
			setBaseTermMap(name, term)
			datastruct.term = append(datastruct.term, termstruct)
		}
		baseStrMap["termlistmap"] += "},},"
		writeBaseFile()
		writeTermFile()
		fmt.Println("parse", name, "succ")
	}
}
func writeTermFile() {
	waitwritestr := ""
	filename := TmpPath + "/" + datastruct.name + ".go"
	tm, _ := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, os.ModePerm)
	defer tm.Close()
	if mm, err := ioutil.ReadFile(PWD + "/models/tmp.data.tmpl"); err != nil {
		fmt.Println("parser tmp.data.tmpl fail:", err)
	} else {
		str := string(mm)
		str = strings.Replace(str, "{{name}}", strings.ToUpper(datastruct.name), -1)
		str = strings.Replace(str, "{{requestdata}}", datastruct.requeststr, -1)
		waitwritestr += str
	}
	waitwritestr += datastruct.funcstr
	for _, term := range datastruct.term {
		if mm, err := ioutil.ReadFile(PWD + "/models/tmp.term.tmpl"); err != nil {
			fmt.Println("parser tmp.term.tmpl fail:", err)
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
	funcstruct := models.FSFuncMap["filter"]
	for _, f := range fs {
		if _, ok := utils.DurationMap[f.Duration]; !ok {
			fmt.Println("parer ", datastruct.name, " ", termstruct.name, " funcs err: filter.duration must in [s,d,m,y] get", f.Duration)
			os.Exit(1)
		}
		str := funcstruct.FuncBody
		str = strings.Replace(str, "{{name}}", strings.ToUpper(datastruct.name+f.Name), -1)
		str = strings.Replace(str, "{{names}}", datastruct.name+f.Name, -1)
		baseStrMap["init"] += "tmp" + datastruct.name + f.Name + `:=&` + strings.ToUpper(datastruct.name+f.Name) + fmt.Sprintf(`{utils.FSBtree,%d,%d,"%s","%s"}
    %s=tmp%s.Do
    `, f.OffSet, f.Whence, f.Name, f.Duration, datastruct.name+f.Name, datastruct.name+f.Name)
		datastruct.funcsmap[f.Name] = funcstruct
		datastruct.funcstr += str
	}

}
func complexExec(term models.Term) (str string) {
	for _, e := range term.Execs {
		isif := false
		tmpstr := ""
		if len(e.Filter) > 0 {
			ifstr, res_type := complexTermFilter(e.Filter, "bool")
			if res_type != "bool" {
				fmt.Println("parer ", datastruct.name, " ", term.Name, " filter err: return must bool but get ", res_type)
				os.Exit(1)
			}
			tmpstr = "if " + ifstr + "{\n"
			isif = true
		} else if len(e.Filter) == 1 {
			fmt.Println("parer ", datastruct.name, " ", term.Name, " filter err: Len<=1 ", e.Filter)
			os.Exit(1)
		}
		if len(e.Do) > 0 {
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
func complexTermDo(f []interface{}) (str string) {
	car, ok := f[0].(string)
	if !ok {
		fmt.Println("parer ", datastruct.name, " ", termstruct.name, " do err: not found op ", f[0])
		os.Exit(1)
	}
	switch car {
	case "+=":
		return complexDoFuncSum(f)
	case "rangesum":
		return complexDoFuncSumList(f)
	default:
		fmt.Println("parer ", datastruct.name, " ", termstruct.name, " do err: not found op ", f[0])
		os.Exit(1)
	}
	return ""
}
func complexDoFuncSumList(f []interface{}) (str string) {
	car, _ := f[0].(string)
	if len(f)-1 != 1 {
		fmt.Println("parser ", datastruct.name, " ", termstruct.name, " do err: op ", car,
			"need 1 params", " but get", len(f)-1)
		os.Exit(1)
	}
	if _, ok := f[1].(string); !ok {
		fmt.Println("parser ", datastruct.name, " ", termstruct.name, " do err: op ", car,
			"the second param type must string", " but get", reflect.TypeOf(f[1]).Name())
		os.Exit(1)
	}
	fkn := ""
	if len(f[1].(string)) > 0 && f[1].(string)[:1] == "@" {
		fp := f[1].(string)[1:]
		if drt, ok := datastruct.request[fp]; ok {
			if drt.(string) != "$rangelist" {
				fmt.Println("parser ", datastruct.name, " ", termstruct.name, " do err: op ", car,
					"the second param", f[1], "type", drt, " but want rangelist")
				os.Exit(1)
			}
			fkn = strings.ToUpper(f[1].(string)[1:])
		} else {
			fmt.Println("parser ", datastruct.name, termstruct.name, "do err:op", car, " Not Found Key ", fp)
			os.Exit(1)
		}
	} else {
		fmt.Println("parser ", datastruct.name, " ", termstruct.name, " do err: op ", car,
			"the second param type must @string", " but get", f[1])
		os.Exit(1)
	}
	str = models.FSFuncMap["rangesum"].FuncBody
	str = strings.Replace(str, "{{name}}", fkn, -1)
	return str
}
func complexDoFuncSum(f []interface{}) (str string) {
	car, _ := f[0].(string)
	if len(f)-1 != 2 {
		fmt.Println("parser ", datastruct.name, " ", termstruct.name, " do err: op ", car,
			"need 2 params", " but get", len(f)-1)
		os.Exit(1)
	}
	switch f[1].(type) {
	case int, int64, float64, string:
	default:
		fmt.Println("parser ", datastruct.name, " ", termstruct.name, " do err: op ", car,
			"the second param type must interface{}", " but get", reflect.TypeOf(f[1]).Name())
		os.Exit(1)
	}
	fkn := ""
	if f[1].(string)[:1] == "@" {
		fp := f[1].(string)[1:]
		if drt, ok := datastruct.request[fp]; ok {
			if drt.(string) == "$rangelist" {
				fmt.Println("parser ", datastruct.name, " ", termstruct.name, " do err: op ", car,
					"the second param", f[1], "type", drt, " but want float64")
				os.Exit(1)
			}
			fkn = "d.req." + strings.ToUpper(f[1].(string)[1:])
		} else {
			fmt.Println("parser ", datastruct.name, termstruct.name, "do err:op", car, " Not Found Key ", fp)
			os.Exit(1)
		}
	} else {
		fkn = `"` + f[1].(string) + `"`
	}
	tvl := ""
	switch f[2].(type) {
	case string:
		if len(f[2].(string)) > 0 && f[2].(string)[:1] == "@" {
			fp := f[2].(string)[1:]
			if drt, ok := datastruct.request[fp]; ok {
				if drt.(string) == "$rangelist" {
					fmt.Println("parser ", datastruct.name, " ", termstruct.name, " do err: op ", car,
						"the second param", f[1], "type", drt, " but want float64")
					os.Exit(1)
				}
				tvl = "d.req." + strings.ToUpper(f[2].(string)[1:])
			} else {
				fmt.Println("parser ", datastruct.name, termstruct.name, "do err:op", car, " Not Found Key ", fp)
				os.Exit(1)
			}
		} else {
			fmt.Println("parser ", datastruct.name, " ", termstruct.name, " do err: op ", car,
				"the second param", f[2], "type string but want float64")
			os.Exit(1)
		}

	case int, int64, float64, float32:
		tvl = fmt.Sprintf("%v", f[2])
	case []interface{}:
		tvl1, returntype := complexTermFilter(f[2].([]interface{}), "float64")
		if returntype != "float64" {
			fmt.Println(tvl1, returntype, f[2])
			fmt.Println("parser ", datastruct.name, " ", termstruct.name, " do err: op ", car,
				"the second param", f[2], "type", returntype, " but want float64")
			os.Exit(1)
		}
		tvl = tvl1
	}
	str += fmt.Sprintf(`commands.Commands=append(commands.Commands,RdsCommand{Cmd:"HINCRBYFLOAT",V:[]interface{}{%s,%s}})`, fkn, tvl)
	return str
}
func complexTermFilter(f []interface{}, child_type string) (str string, return_type string) {
	//获取op
	car := f[0]
	switch car.(type) {
	case string:
		if len(car.(string)) > 0 && car.(string)[:1] == "$" {
			funcname := car.(string)[1:]
			if fsf, ok := datastruct.funcsmap[funcname]; ok {
				if fsf.ReturnType[0] != child_type && child_type != "eq" {
					fmt.Println("parser ", datastruct.name, " ", termstruct.name, " filter err: op ", car,
						" return ", fsf.ReturnType, " but want ", child_type)
					os.Exit(1)
				}
				if len(f)-1 != len(fsf.Paramstype) {
					fmt.Println("parser ", datastruct.name, " ", termstruct.name, " filter err: op ", car,
						"need", len(fsf.Paramstype), " but get", len(f)-1)
					os.Exit(1)
				}
				str = datastruct.name + funcname + "("
				tmpparams := []string{}
				for k, p := range fsf.Paramstype {
					fkn := ""
					fpt := ""
					switch f[k+1].(type) {
					case string:
						fkn = fmt.Sprintf(`"%s"`, f[k+1])
						fpt = "string"
						if f[k+1].(string)[:1] == "@" {
							fp := f[k+1].(string)[1:]
							if drt, ok := datastruct.request[fp]; ok {
								fpt = drt.(string)
								fkn = "d.req." + strings.ToUpper(f[k+1].(string)[1:])
							} else {
								fmt.Println("parser ", datastruct.name, termstruct.name, "func err:func", car, " Not Found Key ", fp)
								os.Exit(1)
							}
						}
					case int:
						fkn = fmt.Sprintf("%d", f[k+1].(int))
						fpt = "int"
					case int64:
						fkn = fmt.Sprintf("%d", f[k+1].(int64))
						fpt = "int64"
					case float32:
						fkn = fmt.Sprintf("%v", f[k+1])
						fpt = "float32"
					case float64:
						fkn = fmt.Sprintf("%v", f[k+1])
						fpt = "float64"
					case bool:
						fkn = fmt.Sprintf("%v", f[k+1].(bool))
						fpt = "bool"
					default:
						fmt.Println("parser ", datastruct.name, " ", termstruct.name, " filter err: unknown type ", reflect.TypeOf(f[k+1]).Name())
						os.Exit(1)

					}
					if fpt == "string" && f[k+1].(string)[:1] == "@" {
						fp := f[k+1].(string)[1:]
						if drt, ok := datastruct.request[fp]; ok {
							fpt = drt.(string)
						} else {
							fmt.Println("parser ", datastruct.name, termstruct.name, "func err:func", car, " Not Found Key ", fp)
							os.Exit(1)
						}
					}
					if p != fpt {
						fmt.Println("parser ", datastruct.name, " ", termstruct.name, " func err: func", car,
							k, "param type is", p, " but give", fpt)
						os.Exit(1)
					}
					tmpparams = append(tmpparams, fkn)
				}
				str += strings.Join(tmpparams, ",")
				if fsf.Name == "filter" {
					str += ",d.req.STime"
				}
				str += ")"
				return str, fsf.ReturnType[0]
			} else {
				fmt.Println("parser ", datastruct.name, " ", termstruct.name, " filter err: not found func", funcname)
				os.Exit(1)
			}
			//函数调用
		} else {
			//运算比较符
			if v, ok := S_Expmap[car.(string)]; ok {
				if v.paramnum != -1 && v.paramnum != len(f)-1 {
					//op item数量与期望不匹配
					fmt.Println("parser ", datastruct.name, " ", termstruct.name, " filter err: op ", car,
						" need ", v.paramnum, " param")
					os.Exit(1)
				}
				if child_type != v.return_type && v.return_type != "interface" {
					//op返回值类型与期望不匹配
					fmt.Println("parser ", datastruct.name, " ", termstruct.name, " filter err: op ", car,
						" return ", v.return_type, " but want ", child_type)
					os.Exit(1)
				}
				//解析op 操作元素
				param := []string{}
				paramtype := []string{}
				for _, tf := range f[1:] {
					vtype := ""
					str := ""
					switch tf.(type) {
					case string:
						if len(tf.(string)) > 0 && tf.(string)[:1] == "@" {
							if vt, ok := datastruct.request[tf.(string)[1:]]; ok {
								vtype = vt.(string)
								param = append(param, "d.req."+strings.ToUpper(tf.(string)[1:]))
							} else {
								fmt.Println("parser ", datastruct.name, " ", termstruct.name, " key err: Not Found Key ", tf)
								os.Exit(1)
							}
						} else {
							vtype = "string"
							param = append(param, "\""+tf.(string)+"\"")
						}
					case []interface{}:
						str, vtype = complexTermFilter(tf.([]interface{}), v.child_type)
						param = append(param, str)
					case int, int64, float32, float64:
						param = append(param, fmt.Sprintf("%v", tf))
						vtype = reflect.TypeOf(tf).Name()
					case bool:
						param = append(param, fmt.Sprintf("%v", tf.(bool)))
						vtype = "bool"
					default:
						fmt.Println("parser ", datastruct.name, " ", termstruct.name, " filter err: unknown type ", reflect.TypeOf(tf).Name())
						os.Exit(1)
					}
					if vtype != v.child_type && v.child_type != "eq" {
						fmt.Println("parser ", datastruct.name, " ", termstruct.name, " filter err: op ", car,
							" param return ", vtype, " but want ", v.child_type)
						os.Exit(1)
					}
					paramtype = append(paramtype, vtype)
				}
				if v.child_type == "eq" {
					eqrt := ""
					for i, x := range paramtype[1:] {
						eqrt = x
						if paramtype[i] != x && reflect.TypeOf(param[i]).Name() != reflect.TypeOf(param[i+1]).Name() {
							fmt.Println("parser ", datastruct.name, " ", termstruct.name, " filter err: can't use op ", car,
								"with", paramtype[i], "and", x, f)
							os.Exit(1)
						}
					}
					if car == "+" {
						return strings.Join(param, v.split), eqrt
					}
				}
				return strings.Join(param, v.split), v.return_type
			} else {
				//op 不存在
				fmt.Println("parser ", datastruct.name, " ", termstruct.name, " filter err: not found op ", car, " ", f)
				os.Exit(1)
			}
		}
	default:
		fmt.Println("parser ", datastruct.name, " ", termstruct.name, " filter err: first item must string ", f)
		os.Exit(1)
	}
	return "", ""
}
func checkTermKey(term models.Term) {
	for _, i := range term.Key {
		if i[:1] == "@" {
			if _, ok := datastruct.request[i[1:]]; !ok {
				fmt.Println("parser ", datastruct.name, " ", term.Name, " key err: Not Found Key ", i)
				os.Exit(1)
			}
		}
	}
}

func parserDataRequest(s interface{}) {
	filestr := ""
	datastruct.request = s.(map[string]interface{})
	for k, t := range datastruct.request {
		if t.(string)[:1] == "$" {
			filestr += strings.ToUpper(k) + "  models." + strings.ToUpper(t.(string)[1:]) + "\n"
		} else {
			filestr += strings.ToUpper(k) + "  " + t.(string) + "\n"
		}
	}
	filestr += "STime int64 `json:\"s_time\"`\n"
	datastruct.requeststr = filestr
}
func writeBaseFile() {
	tm, _ := os.OpenFile(TmpPath+"/base.go", os.O_RDWR|os.O_CREATE, os.ModePerm)
	defer tm.Close()
	if mm, err := ioutil.ReadFile(PWD + "/models/tmp.base.tmpl"); err != nil {
		fmt.Println("parser base.go fail:", err)
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
		baseStrMap["termmap"] += fmt.Sprintf(`"%s":&%s{},`, name+term.Name, strings.ToUpper(name+term.Name))
	} else {
		baseStrMap["termmap"] = fmt.Sprintf(`"%s":&%s{},`, name+term.Name, strings.ToUpper(name+term.Name))
	}
	if _, ok := termConfigMap[name]; !ok {
		termConfigMap[name] = map[string]models.TermConfig{}
	}
	termConfigMap[name][term.Name] = models.TermConfig{Name: term.Name, Key: term.Key, IsSnow: term.IsSnow, Snow: term.Snow}
}

func copyMainFile() {
	tm, _ := os.OpenFile(TmpPath+"/main.go", os.O_RDWR|os.O_CREATE, os.ModePerm)
	defer tm.Close()
	if mm, err := ioutil.ReadFile(PWD + "/models/tmp.main.tmpl"); err != nil {
		fmt.Println("parser main.go fail:", err)
	} else {
		tm.Write(mm)
	}
}
func checkPath() {
	PWD, _ = os.Getwd()
	DataPath = PWD + "/data"
	TmpPath = PWD + "/tmp"
	if !utils.FileOrPathIsExist(DataPath) {
		fmt.Println("data path not found:", DataPath)
		os.Exit(1)
	}
	if utils.FileOrPathIsExist(TmpPath) {
		fmt.Println("clean tmp path:", TmpPath)
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
		json := models.Json{}
		e := utils.JsonDecode(b, &json)
		if e != nil {
			fmt.Println("load json file:", file, " error:", e.Error())
		}
		if v, ok := ParserMap[json.Name]; ok {
			json.Term = v.Term
			ParserMap[json.Name] = json
		} else {
			ParserMap[json.Name] = json
		}
	} else {
		json := models.Term{}
		e := utils.JsonDecode(b, &json)
		if e != nil {
			fmt.Println("load json file:", file, " error:", e.Error())
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
