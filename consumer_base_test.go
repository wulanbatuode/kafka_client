package consumer

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
)

func createTopic(t *testing.T, bootstrapServers, topic string, numParts, replicationFactor int) {
	// t.Helper()
	adm, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": bootstrapServers})
	if err != nil {
		t.Fatalf("Failed to create Admin client: %s\n", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	results, err := adm.CreateTopics(
		ctx,
		[]kafka.TopicSpecification{{
			Topic:             topic,
			NumPartitions:     numParts,
			ReplicationFactor: replicationFactor}},
		// Admin options
		kafka.SetAdminOperationTimeout(time.Second*10))
	if err != nil {
		t.Fatalf("Failed to create topic: %v\n", err)
	}
	for _, result := range results {
		t.Logf("%s\n", result)
	}
	adm.Close()
}
func deleteTopic(t *testing.T, bootstrapServers string, topics []string) {
	// t.Helper()
	adm, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": bootstrapServers})
	if err != nil {
		t.Fatalf("Failed to create Admin client: %s\n", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	results, err := adm.DeleteTopics(
		ctx,
		topics,
		kafka.SetAdminOperationTimeout(time.Second*10))
	if err != nil {
		t.Fatalf("Failed to create topic: %v\n", err)
	}
	for _, result := range results {
		t.Logf("%s\n", result)
	}
	adm.Close()
}
func produceMessage(t *testing.T, bootstrapServers, topic string, msgTag, totalMsgcnt int) {

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": bootstrapServers})

	if err != nil {
		t.Fatalf("Failed to create producer: %s\n", err)
	}

	t.Logf("Created Producer %v\n", p)

	// Listen to all the events on the default events channel
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				m := ev
				if m.TopicPartition.Error != nil {
					t.Logf("Delivery failed: %v\n", m.TopicPartition.Error)
				} else {
					t.Logf("Delivered message %v to topic %s [%d] at offset %v\n",
						string(m.Value), *m.TopicPartition.Topic,
						m.TopicPartition.Partition, m.TopicPartition.Offset)
				}
			case kafka.Error:
				t.Logf("Error: %v\n", ev)
			default:
				t.Logf("Ignored event: %s\n", ev)
			}
		}
	}()

	msgcnt := 0
	for msgcnt < totalMsgcnt {
		value := fmt.Sprintf("msg #%d", msgTag)
		msgTag++

		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(value),
			Headers: []kafka.Header{
				{
					Key:   "myTestHeader",
					Value: []byte(fmt.Sprintf("header values are binary #%v", msgTag)),
				},
			},
		}, nil)

		if err != nil {
			if err.(kafka.Error).Code() == kafka.ErrQueueFull {
				time.Sleep(time.Second)
				continue
			}
			t.Logf("Failed to produce message: %v\n", err)
		}
		msgcnt++
	}

	// Flush and close the producer and the events channel
	for p.Flush(10000) > 0 {
		t.Log("Still waiting to flush outstanding messages\n", err)
	}
	p.Close()
}

func testPollHelper(t *testing.T, consumer *KafkaConsumer, nNil int) int {
	t.Helper()
	msgCnt := 0
	nilCnt := 0
	for {
		msg := consumer.PollKafkaMessage(time.Second)
		// t.Log(msg)
		if msg == nil {
			nilCnt++
			if nilCnt == nNil {
				break
			}
			continue
		}
		msgCnt++
		t.Logf(string(msg.Value))
	}
	return msgCnt
}

func TestCtrlEnum(t *testing.T) {
	assert.Equal(t, "CtrlSeekEnd", CtrlSeekEnd.String())
	assert.Equal(t, "CtrlType(19)", CtrlType(19).String())
}

func TestPreflight(t *testing.T) {

	config := Config{
		"group.id": "consumerGroup",
	}

	assertor := assert.New(t)

	consumer, err := NewConsumer(&config, []string{"topic"})
	assertor.Equal((*KafkaConsumer)(nil), consumer)
	assertor.ErrorIs(ErrParam, err)
}
func TestPoll(t *testing.T) {
	log.SetFlags(log.Lshortfile | log.LstdFlags)

	server := "localhost:9092"
	topic := "test-topic-1"
	numParts := 4
	replica := 1

	consumerGroup := "cg-2"

	config := Config{
		"bootstrap.servers": server,
		"group.id":          consumerGroup,
	}

	nMsg := 10
	nNil := 10

	deleteTopic(t, server, []string{topic})
	createTopic(t, server, topic, numParts, replica)
	t.Cleanup(func() {
		defer deleteTopic(t, server, []string{topic})
	})
	assertor := assert.New(t)

	consumer, err := NewConsumer(&config, []string{topic})
	if err != nil {
		t.Fatalf("new consumer, %v", err)
	}

	t.Run("newconsumer", func(t *testing.T) {
		assertor.Equal((*kafka.Consumer)(nil), consumer.Consumer)
		assertor.Equal((chan *kafka.Message)(nil), consumer.MessageChan)

	})
	t.Run("conect", func(t *testing.T) {
		consumer.connect()
		time.Sleep(time.Second)
		assertor.NotEqual((*kafka.Consumer)(nil), consumer.Consumer)
		assertor.NotEqual((chan *kafka.Message)(nil), consumer.MessageChan)
		assertor.NotEqual((chan int)(nil), consumer.ctrlChan)

	})
	t.Run("get assign", func(t *testing.T) {
		// assignedNum := consumer.GetAssignedNumFromBroker()
		// assertor.Equal(numParts, assignedNum)
	})
	t.Run("poll", func(t *testing.T) {
		produceMessage(t, server, topic, 0, nMsg)
		msgCnt := testPollHelper(t, consumer, nNil)
		assertor.Equal(nMsg, msgCnt)
	})
	t.Run("disconnect", func(t *testing.T) {
		consumer.disconnect()
		assertor.Equal((*kafka.Consumer)(nil), consumer.Consumer)
		assertor.Equal((chan *kafka.Message)(nil), consumer.MessageChan)
		assertor.NotEqual((chan int)(nil), consumer.ctrlChan)
		produceMessage(t, server, topic, 100, nMsg)
		msgCnt := testPollHelper(t, consumer, nNil)
		assertor.Equal(0, msgCnt)
	})

	t.Run("connect", func(t *testing.T) {
		consumer.connect()
		time.Sleep(time.Second)
		assertor.NotEqual((*kafka.Consumer)(nil), consumer.Consumer)
		assertor.NotEqual((chan *kafka.Message)(nil), consumer.MessageChan)
		assertor.NotEqual((chan int)(nil), consumer.ctrlChan)
		produceMessage(t, server, topic, 200, nMsg)
		msgCnt := testPollHelper(t, consumer, nNil)
		assertor.Equal(msgCnt, msgCnt)
	})

	t.Run("seekend", func(t *testing.T) {
		consumer.disconnect()
		produceMessage(t, server, topic, 300, nMsg)
		consumer.connect()
		assertor.Equal(nil, consumer.seekEnd())
		produceMessage(t, server, topic, 500, nMsg/2)
		msgCnt := testPollHelper(t, consumer, nNil)
		assertor.Equal(nMsg/2, msgCnt)
	})
	closed := false
	t.Run("close", func(t *testing.T) {
		consumer.close()
		assertor.Equal((*kafka.Consumer)(nil), consumer.Consumer)
		assertor.Equal((chan *kafka.Message)(nil), consumer.MessageChan)
		assertor.Equal((chan CtrlType)(nil), consumer.ctrlChan)
		closed = true
	})
	t.Cleanup(func() {
		if !closed {
			consumer.close()
		}
	})
}
