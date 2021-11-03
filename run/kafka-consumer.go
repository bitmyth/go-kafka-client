package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/Shopify/sarama"
)

var topic string
var addrs string
var fromBeginning bool
var group string
var offset int64

// kafka consumer
func main() {
	flag.StringVar(&topic, "topic", "", "input topic")
	flag.StringVar(&addrs, "addrs", "", "input addrs,separate by comma")
	flag.Int64Var(&offset, "offset", sarama.OffsetNewest, "input offset")
	flag.BoolVar(&fromBeginning, "from-beginning", false, "if read from beginning")
	flag.StringVar(&group, "group", "", "input group")
	flag.Parse()
	if flag.NFlag()==0{
		flag.PrintDefaults()
		return
	}

	if offset == 0 {
		offset = sarama.OffsetNewest
	}
	if fromBeginning {
		offset = sarama.OffsetOldest
	}

	log.Println("topic:", topic)
	log.Println("addresses:", addrs)
	log.Println("offset:", offset)

	addrsArray := strings.Split(addrs, ",")

	if group != "" {
		consumeGroupStart(addrsArray)
		return
	}
	consumer, err := sarama.NewConsumer(addrsArray, nil)
	if err != nil {
		fmt.Printf("fail to start consumer, err:%v\n", err)
		return
	}
	partitionList, err := consumer.Partitions(topic) // 根据topic取到所有的分区
	if err != nil {
		fmt.Printf("fail to get list of partition:err%v\n", err)
		return
	}
	fmt.Println("Partition List:", partitionList)
	for partition := range partitionList { // 遍历所有的分区
		// 针对每个分区创建一个对应的分区消费者
		pc, err := consumer.ConsumePartition(topic, int32(partition), offset)
		if err != nil {
			fmt.Printf("failed to start consumer for partition %d,err:%v\n", partition, err)
			return
		}
		defer pc.AsyncClose()
		// 异步从每个分区消费信息
		go func(sarama.PartitionConsumer) {
			for msg := range pc.Messages() {
				fmt.Printf("Partition:\033[0;32m%d\033[0m Offset:\033[0;36m%d\033[0m Key:\033[0;32m%v\033[0m Value:\n%v\n", msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
			}
		}(pc)
	}

	quit := make(chan os.Signal)
	// kill (no param) default send syscanll.SIGTERM
	// kill -2 is syscall.SIGINT
	// kill -9 is syscall. SIGKILL but can"t be catch, so don't need add it
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
}

func consumeGroupStart(addrsArray []string) {
	consumerGroup, err := sarama.NewConsumerGroup(addrsArray, group, nil)
	if err != nil {
		fmt.Printf("fail to start consumer group, err:%v\n", err)
		return
	}
	consumer := Consumer{
		ready: make(chan bool),
	}
	consumerGroup.Consume(context.Background(), []string{topic}, &consumer)
}

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
	ready chan bool
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/main/consumer_group.go#L27-L29
	for message := range claim.Messages() {
		log.Printf("timestamp : %v, topic : %s, partion : %d", message.Timestamp.Format("2016-01-02 15:04:05"), message.Topic, message.Partition)
		log.Printf("Message claimed: value = %s", string(message.Value))
		session.MarkMessage(message, "")
	}

	return nil
}
