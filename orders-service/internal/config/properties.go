package config

import (
	"strings"

	"github.com/spf13/viper"
)

type Config struct {
	Server ServerConfig `mapstructure:"server"`
	MySQL  MySQLConfig  `mapstructure:"mysql"`
	Redis  RedisConfig  `mapstructure:"redis"`
	Kafka  KafkaConfig  `mapstructure:"kafka"`
	Outbox OutboxConfig `mapstructure:"outbox"`
}

type ServerConfig struct {
	Port string `mapstructure:"port"`
}

type MySQLConfig struct {
	DSN string `mapstructure:"dsn"`
}

type RedisConfig struct {
	Addr string `mapstructure:"addr"`
	Pass string `mapstructure:"pass"`
	DB   int    `mapstructure:"db"`
}

type KafkaConfig struct {
	Brokers                []string `mapstructure:"brokers"`
	OrdersCreatedTopic     string   `mapstructure:"orders_created_topic"`
	OrdersConfirmedTopic   string   `mapstructure:"orders_confirmed_topic"`
	InventoryTopic         string   `mapstructure:"inventory_topic"`
	InventoryReleasedTopic string   `mapstructure:"inventory_released_topic"`
	PaymentsTopic          string   `mapstructure:"payments_topic"`
}

type OutboxConfig struct {
	BatchSize int `mapstructure:"batch_size"`
}

func Load() *Config {
	v := viper.New()

	// yaml file
	v.SetConfigName("config")
	v.SetConfigType("yaml")
	v.AddConfigPath(".")
	if err := v.ReadInConfig(); err != nil {
		panic("failed to read config file: " + err.Error())
	}

	// env var bindings (mesmos nomes de antes)
	v.BindEnv("server.port", "SERVER_PORT")
	v.BindEnv("mysql.dsn", "MYSQL_DSN")
	v.BindEnv("redis.addr", "REDIS_ADDR")
	v.BindEnv("redis.pass", "REDIS_PASS")
	v.BindEnv("kafka.brokers", "KAFKA_BROKERS")
	v.BindEnv("kafka.orders_created_topic", "KAFKA_ORDERS_CREATED_TOPIC")
	v.BindEnv("kafka.orders_confirmed_topic", "KAFKA_ORDERS_CONFIRMED_TOPIC")
	v.BindEnv("kafka.inventory_topic", "KAFKA_INVENTORY_TOPIC")
	v.BindEnv("kafka.inventory_released_topic", "KAFKA_INVENTORY_RELEASED_TOPIC")
	v.BindEnv("kafka.payments_topic", "KAFKA_PAYMENTS_TOPIC")
	v.BindEnv("outbox.batch_size", "OUTBOX_BATCH_SIZE")

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		panic("failed to unmarshal config: " + err.Error())
	}

	// KAFKA_BROKERS como string separada por vírgula (compatibilidade)
	if brokersStr := v.GetString("kafka.brokers"); brokersStr != "" && len(cfg.Kafka.Brokers) == 1 {
		if parts := strings.Split(brokersStr, ","); len(parts) > 1 {
			cfg.Kafka.Brokers = parts
		}
	}

	return &cfg
}
