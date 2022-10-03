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
	cache           map[string]*TopicInfo
	AssignedInfoNum int
}

func NewCache(topics []string) *Cache {
	c := new(Cache)
	c.cache = make(map[string]*TopicInfo, len(topics))
	for _, topic := range topics {
		c.cache[topic] = &TopicInfo{
			Topic:    topic,
			Offset:   [MaxPartitionNum]int64{},
			Assigned: [MaxPartitionNum]bool{},
		}
	}
	c.AssignedInfoNum = 0
	return c
}

func (c *Cache) AssignPartition(topic string, partiton int64, offset int64) {

	if _, ok := c.cache[topic]; !ok {
		c.cache[topic] = &TopicInfo{
			Topic:    topic,
			Offset:   [MaxPartitionNum]int64{},
			Assigned: [MaxPartitionNum]bool{},
		}
	}
	c.cache[topic].Offset[partiton] = offset
	c.cache[topic].Assigned[partiton] = true
	c.AssignedInfoNum++
}

func (c *Cache) RevokePartition(topic string, partiton int64) {

	if tpInfo, ok := c.cache[topic]; ok {
		part := partiton
		tpInfo.Assigned[part] = false
		c.AssignedInfoNum--
	}
}

func (c *Cache) Commit(topic string, partition int64, offset int64) {

	tpInfo, ok := c.cache[topic]
	if !ok {
		c.AssignPartition(topic, partition, offset)
	} else {
		tpInfo.Offset[partition] = offset
		tpInfo.Assigned[partition] = true
	}
}

func (c *Cache) GetAssigned() []*TopicInfo {
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
