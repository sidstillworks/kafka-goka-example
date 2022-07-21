package main

import (
	"log"

	"github.com/linkedin/goavro"
	"github.com/lovoo/goka"
)

var (
	brokers             = []string{"127.0.0.1:9092"}
	topic   goka.Stream = "random-no-upto-five7"
	group   goka.Group  = "mini-random-group-upto-five7"

	tmc       *goka.TopicManagerConfig
	avrocodec *goavro.Codec
)

func init() {
	tmc = goka.NewTopicManagerConfig()
	tmc.Table.Replication = 1
	tmc.Stream.Replication = 1
}

func setup() error {
	avrocodec, _ = goavro.NewCodec(`
		{
			"type": "record",
			"name": "User",
			"fields": [
				{"name": "EarlierTime", "type" : "string", "default": ""},
				{"name": "LatestTime", "type" : "string", "default": ""},
				{"name": "TotalValue", "type" : "int"}
			]
		}`)

	tm, err := goka.NewTopicManager(brokers, goka.DefaultConfig(), tmc)
	if err != nil {
		log.Fatalf("Error creating topic manager: %v", err)
		return err
	}
	defer tm.Close()
	err = tm.EnsureStreamExists(string(topic), 8)
	if err != nil {
		log.Printf("Error creating kafka topic %s: %v", topic, err)
		return err
	}

	return nil
}
