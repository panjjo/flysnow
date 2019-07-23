package main

import (
	"fmt"
	"github.com/panjjo/flysnow/models"
	"github.com/panjjo/flysnow/utils"
	_ "github.com/panjjo/flysnow/tmp"
	"os"
)

func main() {
	updateToRelesase1200()
}

// 从release1000升级到release1200
func updateToRelesase1200() {
	PWD, _ := os.Getwd()
	fmt.Println(PWD)
	utils.FSConfig = utils.Config{}
	utils.FSConfig.InitConfig(PWD + "/config/base.conf")
	utils.FSConfig.SetMod("sys")
	for tag,terms:=range models.TermConfigMap{
		for term,_:=range terms{
			fmt.Println(tag,term)
		}
	}

}
