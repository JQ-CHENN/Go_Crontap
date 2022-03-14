package settings

import (
	"fmt"

	"github.com/spf13/viper"
)

func Init() (err error) {
	viper.SetConfigName("config")
	viper.AddConfigPath("./settings")

	err = viper.ReadInConfig()
	if err != nil {
		fmt.Println("read config err...")
		return
	}
	return
}