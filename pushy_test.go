package pusharound

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSplitBatches(t *testing.T) {
	const batchLimit = 100

	for _, inputLen := range []int{
		0,
		10,
		batchLimit - 1,
		batchLimit + 1,
		batchLimit * 1,
		batchLimit * 10,
		batchLimit*10 + 42,
	} {
		t.Run(strconv.Itoa(inputLen), func(t *testing.T) {
			input := []int{}
			for i := 0; i < inputLen; i++ {
				input = append(input, i)
			}

			batches := splitBatches(input, batchLimit)

			expectedNumBatches := len(input) / batchLimit
			if len(input)%batchLimit != 0 && len(input) != 0 {
				expectedNumBatches++
			}
			assert.Equal(t, expectedNumBatches, len(batches))

			concatd := make([]int, 0, inputLen)
			for _, b := range batches {
				concatd = append(concatd, b...)
			}

			for i := 0; i < inputLen; i++ {
				assert.Equal(t, i, concatd[i])
			}
		})
	}
}
