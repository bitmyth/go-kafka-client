package main

import (
	"flag"
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

// kafka consumer

func main() {
	var topic string
	var addrs string
	var offset int64

	flag.StringVar(&topic, "topic", "", "input topic")
	flag.StringVar(&addrs, "addrs", "", "input addrs,separate by comma")
	flag.Int64Var(&offset, "offset", sarama.OffsetNewest, "input addrs,separate by comma")
	flag.Parse()

	log.Println("topic:", topic)
	log.Println("addresses:", addrs)

	addrsArray := strings.Split(addrs, ",")

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
