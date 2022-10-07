package consumer

const (
	MaxPartitionNum = 10
	OffsetBeginning = -2
	OffsetEnd       = -1
)

type TopicInfo struct {
	Topic    string
	Offset   [MaxPartitionNum]int64
	Assigned [MaxPartitionNum]bool
}

type Cache struct {
	cache map[string]*TopicInfo
}

func newCache(topics []string) *Cache {
	c := new(Cache)
	c.cache = make(map[string]*TopicInfo, len(topics))
	for _, topic := range topics {
		c.cache[topic] = &TopicInfo{
			Topic:    topic,
			Offset:   [MaxPartitionNum]int64{},
			Assigned: [MaxPartitionNum]bool{},
		}
	}
	return c
}

func (c *Cache) assignPartition(topic string, partiton int64, offset int64) {

	if _, ok := c.cache[topic]; !ok {
		c.cache[topic] = &TopicInfo{
			Topic:    topic,
			Offset:   [MaxPartitionNum]int64{},
			Assigned: [MaxPartitionNum]bool{},
		}
	}
	c.cache[topic].Offset[partiton] = offset
	c.cache[topic].Assigned[partiton] = true
}

func (c *Cache) revokePartition(topic string, partiton int64) {
	if tpInfo, ok := c.cache[topic]; ok {
		part := partiton
		tpInfo.Assigned[part] = false
	}
}

func (c *Cache) commit(topic string, partition int64, offset int64) {

	tpInfo, ok := c.cache[topic]
	if !ok {
		c.assignPartition(topic, partition, offset)
	} else {
		tpInfo.Offset[partition] = offset
		tpInfo.Assigned[partition] = true
	}
}

func (c *Cache) getAssigned() []*TopicInfo {
	assigned := make([]*TopicInfo, 0, len(c.cache))
	for _, partInfo := range c.cache {
		for _, v := range partInfo.Assigned {
			if v {
				assigned = append(assigned, partInfo)
				break
			}
		}
	}
	return assigned
}
