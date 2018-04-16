# flysnow
## 运行
    配置好配置文件和统计项
    go run parser.go
    go run main.go

## 配置文件

  默认使用sys配置组
  当单独配置数据源配置组时优先使用数据源数据组
  例如：
  
      [sys]
      redis.Host=127.0.0.1
      redis.MaxPoolConn=100               #redis链接池最大链接数 默认为10
      redis.MaxActive=100                 #redis连接池最大活跃链接数 默认为0不限制
      mgo.Host=mongodb://127.0.0.1:27017  #mgo地址
      mgo.Dupl=clone                      #mgo session生成方式 clone copy new 默认为clone
      filter.save =0                      #过滤数据是否永久存储,0 重启清空，1 永久存储 默认0 只能设置sys不能单独设置
      [order]
      redis.Host=127.0.0.1
      redis.MaxPoolConn=100               #redis链接池最大链接数 默认为10
      redis.MaxActive=100                 #redis连接池最大活跃链接数 默认为0不限制
      mgo.Host=mongodb://127.0.0.1:27017  #mgo地址
      mgo.Dupl=clone                      #mgo session生成方式 clone copy new 默认为clone
      
  请求数据源为order时优先使用order组配置，请求数据源为trade时则用sys组配置
  redis，mongo配置项为每组配置单独使用

## 统计项配置

  ./data/
  例如 统计数据源为order
  在data目录下创建order文件夹
  
      data/
        order/                              #数据源json路径，一个数据源对应一个目录
          main.json                         #主json文件，含有请求数据格式，自定义函数等配置
          xx1.json                          #项文件，包含统计结果唯一key设置，计算规则，rotate规则等
          xx2.json                            每个项文件只能设置一种结果唯一key规则，如统计每个店铺订单量，统计结果唯一key为shopid，如果也需要
          xx3.json                            统计所有店铺每天订单量，统计结果唯一key为date，则此为两个项统计，需写两个文件
          xxn.json                            相同唯一key尽量写入同一个项文件
        shop/
          main.json
          xxxx.json
  配置文件解析
   
      main.json
         {
           "name":"order",                  #数据源名称
           "reqdata":{                      #请求数据结构
             "shopid":"string",             #key为字段名，value为字段值类型(所支持的字段类型请看下面)
             "orderid":"string",
             "total":"float64",
             "items":"$rangelist"           #$开头的为系统自定义结构(详情看 数据类型)
             }
           "filter":[                       #自定义过滤器
             {
               "name":"order_filter",       #过滤器名称
               "offset":120,                #数据失效时长 和duration同时使用,设置为0标识永不失效
               "whence":0,                  #时长计算类型 (详情看 过滤器定制)
               "duration":"s"               #时长单位 s 秒 h 小时 d 天 m 月 y 年
               }
             ]
         }
      xxx.json
         {
           "name":"all",                    #项名称
           "key":["counts","@shopid"],      #唯一key 列表  @为取请求数据中的字段值 下同
           "execs":[                        #执行操作
             {                              
               "filter":[
                  "&&",
                  ["==","@status","succ"], #执行条件 采用S-expression规则 即第一个元素为操作符，其余为变量,详情  https://en.wikipedia.org/wiki/S-expression
                  ["$order_filter","@orderid"] # 调用过滤器过滤orderid ，此条件解析为 数据中status的值为succ并且orderid过滤不存在
               ],
               "do":[                       
                 ["+=","succ",1],            # do 为执行操作的具体操作 
                 ["+=","total",1],           # 此操作为 total+1
                 ["+=","@shopid",1]          # 此为将传入字段中shopid作为key值加1
                 ["rangesum","@items"]       # 循环计算(详情看 操作运算说明)
               ]                            
             },                            
             {
               "filter":["!=","@code","0"],
               "do":[
                 ["+","fail",1],
                 ["+","@code",1],
                 ["+","total",1]
               ]
             }
           ],
           "snow":[
             {
               "interval":1,                #每条数据时长
               "intervalduration":"h",      #类型
               "timeout":1,                 #保存时长
               "timeoutduration":"d"        #类型
             },                             # 每个小时一条数据 存储最近1天数据，注意此处interval为whence=0,timeout为whence=1 (详情看 过滤器定制解释中的whence)
              {
               "interval":1,                #一天一条数据，存储最近一个月的数据
               "intervalduration":"d",
               "timeout":1,
               "timeoutduration":"m"
             }
           ]                                #此配置可查询此项统计数据1天(24小时)内任意小时的数据，查询一个月(30天)内任意天的数据，一个月以前的数据只能查看一个总数
         }

## 数据类型

    string  字符串
    float64 数值型（为了方便计算，int int64 float32 float64 统一设置为float64)
    bool    布尔型
    $rangelist 列表型 此类型主要用于循环计算 如计算店铺内所有商品销售总额，数据源为order itmes为$rangelist类型 [{key:key1,value:value1},{key:key2,value:value2}]
                  使用++运算符表示执行循环操作 将统计数据的key1 和key2的值分别加value1,value2

## 自定义函数

过滤器
    
    过滤器是针对数据源使用，一次定义在此数据源中的所有统计项中皆可使用
    过滤器使用可永久存储的btree实现
    whence  0 表示类似自然年的概念，1 则相反
       例：
          offset=1
          whence=0
          duration=d
          表示获取今天0点到现在的数据  表达式 start:now-now%(offset*duration) end:now-now%(offset*duration)+offset*duration
          offset=1
          whence=1
          duration=d
          表示获取昨天此时到现在的数据 表达式 start:now-offset*duration end:now
##  运算符

  条件
  
    操作符        参数                              返回
    +          interface,interface....             interface
    ==         interface,interface                 bool
    &&         bool,bool,bool.....                 bool
    !=         interface,interface                 bool
    ||         bool,bool,bool.....                 bool
    $filter    string                              bool            过滤器类型函数，系统自动加入stime参数

  Do                                                              
  Do 类型操作是针对redis操作 +=表示key.value+value
  
    操作符        参数                            
    rangesum    $rangelist                        
    +=          interface,float64

  示例：
  
    do:["+=","@shopid",["+","@order_total","@order_discount"]]       #表示给key为shopid的值加上（order_total+order_discount)
                                                                         shopid+=order_total+order_discount
                                                                    #只要使用合适，可以无限嵌套

