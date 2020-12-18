package config

import (
	"fmt"
	"github.com/spf13/viper"
)

var Config *viper.Viper

func LoadConfig() {
	viper.SetConfigName("config")
	viper.AddConfigPath("/app")
	viper.AddConfigPath(".")
	err := viper.ReadInConfig()
	if err != nil {               // Handle errors reading the config file
		panic(fmt.Errorf("Fatal error config file: %s \n", err))
	}
	Config = viper.GetViper()
}
