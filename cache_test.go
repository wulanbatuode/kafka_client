package consumer

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCache(t *testing.T) {
	assertor := assert.New(t)
	cache := NewCache([]string{"A", "B"})
	assertor.Contains(cache.cache, "A")
	assertor.Contains(cache.cache, "B")
	assertor.NotContains(cache.cache, "C")

	cache.AssignPartition("A", 2, 6)
	cache.AssignPartition("C", 4, 7)
	assertor.Contains(cache.cache, "C")
	assertor.Equal(int64(6), cache.cache["A"].Offset[2])
	assertor.Equal(int64(7), cache.cache["C"].Offset[4])
	assertor.Equal(true, cache.cache["C"].Assigned[4])
	cache.AssignPartition("A", 2, 6)

	cache.RevokePartition("A", 2)
	assertor.Equal(false, cache.cache["A"].Assigned[2])

	assigned := cache.GetAssigned()
	assertor.Equal(1, len(assigned))

	cache.Commit("D", 3, 9)
	assertor.Equal(true, cache.cache["D"].Assigned[3])
	assertor.Equal(int64(9), cache.cache["D"].Offset[3])
	cache.Commit("D", 3, 19)
	assertor.Equal(int64(19), cache.cache["D"].Offset[3])

}
