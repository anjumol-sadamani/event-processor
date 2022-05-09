package config

import (
	"log"
	"sync"

	"github.com/spf13/viper"
)

type Config struct {
	Env               string `mapstructure:"ENV"`
	DBUserName        string `mapstructure:"DB_USER_NAME"`
	DBPassword        string `mapstructure:"DB_PASSWORD"`
	DBHost            string `mapstructure:"DB_HOST"`
	DBPort            string `mapstructure:"DB_PORT"`
	DBName            string `mapstructure:"DB_NAME"`
	Port              string `mapstructure:"PORT"`
	KafkaServerHost   string `mapstructure:"MSK_SERVER_HOSTS_STRING"`
	ReplicationFactor int    `mapstructure:"REPLICATION_FACTOR"`
	RetryTimeInterval int    `mapstructure:"RETRY_TIME_INTERVAL"`
	KafkaTopic        string `mapstructure:"KAFKA_TOPIC"`
	KafkaRetryTopic   string `mapstructure:"KAFKA_RETRY_TOPIC"`
	KafkaDlqTopic     string `mapstructure:"KAFKA_DLQ_TOPIC"`
}

var config *Config
var once sync.Once

func init() {
	once.Do(func() {
		viper.AutomaticEnv()
		viper.SetConfigFile(".env")
		config = new(Config)
		if err := viper.ReadInConfig(); err != nil {
			log.Printf("Error reading config file, %s", err)

		}
		if err := viper.Unmarshal(config); err != nil {
			log.Printf("Unable to decode into struct, %v", err)

		}
	})
}

func GetConfig() *Config {
	return config
}
