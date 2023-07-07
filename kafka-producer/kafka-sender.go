package main

import (
	"flag"
	"fmt"
	"log"
	"strings"

	"github.com/Shopify/sarama"
	"time"
)

var config *sarama.Config
var client sarama.SyncProducer
var topic string
var key string
var message string

// kafka sender
func main() {
	client, err := Init()
	if err != nil {
		return
	}

	defer client.Close()

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key: sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(message),
	}

	// 发送消息
	pid, offset, err := client.SendMessage(msg)
	if err != nil {
		fmt.Println("send msg failed, err:", err)
		return
	}
	fmt.Printf("pid:%v offset:%v\n", pid, offset)
}

func Init() (sarama.SyncProducer, error) {
	var addrs string
	flag.StringVar(&topic, "topic", "", "input topic")
	flag.StringVar(&key, "key", "", "key")
	flag.StringVar(&addrs, "addrs", "localhost:9092", "input addrs,separate by comma")
	flag.StringVar(&message, "message", "this is message:"+time.Now().String(), "input group")

	flag.Parse()
	log.Println("topic:", topic)
	log.Println("addresses:", addrs)
	log.Println("message:", message)

	config = sarama.NewConfig()
	// 发送完数据需要leader和follow都确认
	config.Producer.RequiredAcks = sarama.WaitForAll
	// 成功交付的消息将在success channel返回
	config.Producer.Return.Successes = true

	addrsArray := strings.Split(addrs, ",")
	client, err := sarama.NewSyncProducer(addrsArray, config)

	if err != nil {
		fmt.Println("producer closed, err:", err)
		return nil, err
	}
	return client, nil
}
