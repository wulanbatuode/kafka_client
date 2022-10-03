package consumer

import (
	"errors"
	"log"
	"os"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const (
	MaxCacheMessage = 1000
	MaxCtlSize      = 10
	TimeoutMs       = 1000
	TimeoutSec      = 1
	CommitInterval  = 100
	RetryNum        = 5
)

var (
	ErrParam   = errors.New("err param")
	ErrTimeout = errors.New("err timeout")

	defaultConfigKV = []struct {
		Key string
		Val interface{}
	}{
		{Key: "auto.offset.reset", Val: "earliest"},
		{Key: "broker.address.family", Val: "v4"},
	}

	CtrlStop    = 1
	CtrlSeekEnd = 2
	CtrlCommit  = 3
)

type (
	Config      kafka.ConfigMap
	RebalanceCb kafka.RebalanceCb
)

type KafkaConsumer struct {
	config              *Config
	Consumer            *kafka.Consumer
	topics              []string
	MessageChan         chan *kafka.Message
	MessageToCommitChan chan *kafka.Message
	cache               *Cache
	ctrlChan            chan int
	backChan            chan error
	wg                  sync.WaitGroup
}

func preflight(config Config) error {
	for _, k := range []string{"bootstrap.servers", "group.id"} {
		if _, ok := config[k]; !ok {
			return ErrParam
		}
	}
	config["enable.auto.commit"] = false
	for _, kv := range defaultConfigKV {
		if _, ok := config[kv.Key]; !ok {
			config[kv.Key] = kv.Val
		}
	}
	return nil
}
func NewConsumer(config *Config, topics []string) (*KafkaConsumer, error) {
	consumer := new(KafkaConsumer)
	if err := preflight(*config); err != nil {
		return nil, err
	}
	consumer.topics = topics
	consumer.cache = NewCache(topics)
	consumer.config = config
	consumer.wg = sync.WaitGroup{}
	consumer.ctrlChan = make(chan int, MaxCtlSize)
	consumer.backChan = make(chan error)
	consumer.MessageChan = nil
	consumer.Consumer = nil
	return consumer, nil
}

func (c *KafkaConsumer) newRawConsumer() error {
	rawConsumer, err := kafka.NewConsumer((*kafka.ConfigMap)(c.config))
	if err != nil {
		return err
	}
	c.Consumer = rawConsumer
	return nil
}

func (c *KafkaConsumer) relalanceCallback(rawConsumer *kafka.Consumer,
	event kafka.Event) error {
	switch evt := event.(type) {
	case kafka.AssignedPartitions:
		log.Printf("%s relalanced: %d new partiontions assigned: %v",
			rawConsumer.GetRebalanceProtocol(), len(evt.Partitions), evt.Partitions)
		err := rawConsumer.Assign(evt.Partitions)
		if err != nil {
			log.Println(err)
		}
		c.assign(evt.Partitions)
	case kafka.RevokedPartitions:
		log.Println("assignment lost")
		c.revoke(evt.Partitions)
	}
	return nil
}

func (c *KafkaConsumer) assign(parts []kafka.TopicPartition) {
	for _, part := range parts {
		c.cache.AssignPartition(string(*part.Topic), int64(part.Partition), int64(part.Offset))
	}
}
func (c *KafkaConsumer) revoke(parts []kafka.TopicPartition) {
	for _, part := range parts {
		c.cache.RevokePartition(string(*part.Topic), int64(part.Partition))
	}
}

func (c *KafkaConsumer) subscribe(topics []string) error {
	err := c.Consumer.SubscribeTopics(topics, c.relalanceCallback)
	return err
}

func (c *KafkaConsumer) Connect() {
	c.wg.Add(1)
	go c.run()
}
func (c *KafkaConsumer) Disconnect() {
	c.ctrlChan <- CtrlStop
	c.wg.Wait()
}

func (c *KafkaConsumer) Close() {
	c.Disconnect()
	c.closeControlChannel()
}
func (c *KafkaConsumer) newMessageChannel() {
	c.MessageChan = make(chan *kafka.Message, MaxCacheMessage)
	c.MessageToCommitChan = make(chan *kafka.Message, MaxCtlSize)
}
func (c *KafkaConsumer) closeMessageChannel() {
	close(c.MessageChan)
	c.MessageChan = nil
	close(c.MessageToCommitChan)
	c.MessageToCommitChan = nil
}
func (c *KafkaConsumer) closeControlChannel() {
	close(c.ctrlChan)
	c.ctrlChan = nil
	close(c.backChan)
	c.backChan = nil
}
func (c *KafkaConsumer) rawConsumerClose() {
	c.Consumer.Unsubscribe()
	err := c.Consumer.Unassign()
	if err != nil {
		log.Printf("unassign, %v", err)
	}
	c.Consumer.Close()
	c.Consumer = nil
}

func (c *KafkaConsumer) stop() {
	c.rawConsumerClose()
	c.closeMessageChannel()
}

func (c *KafkaConsumer) seekCacheToHighWater() error {
	c.triggerAssignment()
	assignedTopics := c.cache.GetAssigned()
	for _, tpInfo := range assignedTopics {
		for part, assigned := range tpInfo.Assigned {
			if assigned {
				_, high, err := c.Consumer.GetWatermarkOffsets(tpInfo.Topic, int32(part))
				if err != nil {
					log.Printf("get watermark on %v@[%v], err %v", tpInfo.Topic, part, err)
					return err
				}
				if high == 0 {
					high = int64(OffsetBeginning)
				} else if kafka.Offset(high) == kafka.OffsetInvalid {
					log.Printf("get invalid high watermark on %v@[%v]", tpInfo.Topic, part)
					high = int64(OffsetEnd)
				}
				c.cache.Commit(tpInfo.Topic, int64(part), high)
			}
		}
	}
	return nil
}
func (c *KafkaConsumer) seekConsumerToCache() error {
	assignedTopics := c.cache.GetAssigned()
	for _, tpInfo := range assignedTopics {
		for part, assigned := range tpInfo.Assigned {
			if assigned && tpInfo.Offset[part] >= 0 {
				kfkTp := kafka.TopicPartition{
					Topic:     &tpInfo.Topic,
					Partition: int32(part),
					Offset:    kafka.Offset(tpInfo.Offset[part]),
					Error:     nil,
				}
				if err := c.Consumer.Seek(kfkTp, TimeoutMs); err != nil {
					log.Printf("get invalid high on %v@[%v]", tpInfo.Topic, part)
					return err
				}
			}
		}
	}
	return nil
}

func (c *KafkaConsumer) SeekEnd() error {
	c.ctrlChan <- CtrlSeekEnd
	select {
	case <-time.After(TimeoutSec * time.Second):
		return ErrTimeout
	case err := <-c.backChan:
		return err
	}
}
func (c *KafkaConsumer) triggerAssignment() {
	evt := c.Consumer.Poll(TimeoutMs)
	if evt == nil {
		log.Printf("first poll evt %v.", evt)
		return
	}
	switch msg := evt.(type) {
	case *kafka.Message:
		c.Consumer.Seek(msg.TopicPartition, TimeoutMs)
	default:
		log.Printf("polled %v on first poll, ignore", evt)
	}
}
func (c *KafkaConsumer) seekEndOperation() error {
	c.triggerAssignment()
	var err error = nil
	for i := 0; i < RetryNum; i++ {
		if err = c.seekCacheToHighWater(); err != nil {
			continue
		}
		if err = c.seekConsumerToCache(); err != nil {
			continue
		}
		err = nil
		c.closeMessageChannel()
		c.newMessageChannel()
		break
	}
	return err
}

func (c *KafkaConsumer) run() {
	newErrCnt := 0
	pollErrCnt := 0
outer:
	for {
		err := c.newRawConsumer()
		if err != nil {
			log.Printf("err %v in new raw consumer(%v/%v), retry.\n", err, newErrCnt, RetryNum)
			newErrCnt++
			if newErrCnt == RetryNum {
				c.closeControlChannel()
				os.Exit(-1)
			}
			continue outer
		}
		newErrCnt = 0

		c.newMessageChannel()
		c.subscribe(c.topics)
		c.triggerAssignment()
		c.seekConsumerToCache()
		for {
			select {
			case ctl := <-c.ctrlChan:
				if ctl == CtrlStop {
					log.Println("stop raw consumer sig.")
					break outer
				} else if ctl == CtrlSeekEnd {
					log.Println("seek end sig.")
					c.backChan <- c.seekEndOperation()
				} else if ctl == CtrlCommit {
					if len(c.MessageToCommitChan) > 0 {
						msgToCommit := <-c.MessageToCommitChan
						c.commitMessageWithTimeout(msgToCommit, TimeoutSec*time.Second)
					}
				} else {
					log.Printf("unknown ctrl %v, ignore", ctl)
				}
			default:
				evt := c.Consumer.Poll(TimeoutMs)
				// DEBUG INFO
				// log.Println("polling msg", evt)
				if evt == nil {
					continue
				}
				switch msg := evt.(type) {
				case *kafka.Message:
					if len(c.MessageChan) == MaxCacheMessage {
						c.Consumer.Seek(msg.TopicPartition, TimeoutMs)
					} else {
						c.MessageChan <- msg
					}
					pollErrCnt = 0
				case kafka.Error:
					// Generic client instance-level errors, such as
					// broker connection failures, authentication issues, etc.
					//
					// These errors should generally be considered informational
					// as the underlying client will automatically try to
					// recover from any errors encountered, the application
					// does not need to take action on them.
					log.Printf("polled err %v.", msg)
					pollErrCnt++

					if pollErrCnt == RetryNum {
						c.stop()
						c.closeControlChannel()
						log.Printf("err %v in poll(%v/%v), exit.\n", err, newErrCnt, RetryNum)

						os.Exit(1)
					} else if pollErrCnt < RetryNum {
						// if error count < RetryNum, wait for low
						log.Printf("err %v in poll(%v/%v), retry.\n", err, newErrCnt, RetryNum)
						continue
					}
					continue outer
				default:
					log.Printf("unknown type, ignore. %v\n", msg)
				}
			}
			// TODO: commit cached offset to broker by interval
		}
	}
	c.stop()
	c.wg.Done()
}

func (c *KafkaConsumer) PollKafkaMessage(timeout time.Duration) *kafka.Message {

	select {
	case <-time.After(timeout):
		return nil
	case msg := <-c.MessageChan:
		if msg != nil {
			c.cache.Commit(*msg.TopicPartition.Topic,
				int64(msg.TopicPartition.Partition), int64(msg.TopicPartition.Offset))
			// TODO: COMMIT MSG
			if msg.TopicPartition.Offset%CommitInterval == 0 {
				c.ctrlChan <- CtrlCommit
				c.MessageToCommitChan <- msg
			}

		}
		return msg
	}
}

func (c *KafkaConsumer) commitMessageWithTimeout(msg *kafka.Message, timeout time.Duration) {
	ch := make(chan struct{})
	go func() {
		tpInfo, err := c.Consumer.CommitMessage(msg)
		log.Printf("commited %v, ignore err if any. err: %v", tpInfo, err)
		ch <- struct{}{}
	}()
	select {
	case <-time.After(timeout):
		log.Println("commit timeout")
	case <-ch:
		log.Println("commit done")
	}
}

func (c *KafkaConsumer) GetAssignedNumFromBroker() int {
	c.triggerAssignment()
	return c.cache.AssignedInfoNum
}
