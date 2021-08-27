package kafka

import (
	"errors"
	"github.com/Shopify/sarama"
	"github.com/caarlos0/env"
	"os"
)

type Config struct {
	Topic   string   `env:"TOPIC"`
	Brokers []string `env:"BROKER" envSeparator:","`
}

func newAsyncProducer(conf *Config) (sarama.AsyncProducer, error) {
	saramaCfg := sarama.NewConfig()
	producer, err := sarama.NewAsyncProducer(conf.Brokers, saramaCfg)
	if err != nil {
		return nil, err
	}

	return producer, nil
}

func Init() (sarama.AsyncProducer, error) {
	conf := Config{}
	if err := env.Parse(&conf); err != nil {
		return nil, errors.New("cloud not load kafka environment variables")
	}

	return newAsyncProducer(&conf)
}

func Topic() string {
	return os.Getenv("TOPIC")
}
