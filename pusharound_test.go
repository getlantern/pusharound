package pusharound

import (
	"context"
	"errors"
	"math/rand"
	"slices"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewStream(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		data := randStringData(100000)

		s, err := NewStream(data, streamMsgOverhead+10000)
		require.NoError(t, err)

		msgs := []Message{}
		for m := s.NextMessage(); m != nil; m = s.NextMessage() {
			msgs = append(msgs, m)
		}

		// There should be one extra message: the final completion message.
		assert.Equal(t, 11, len(msgs))
		assert.Equal(t, data, collate(t, msgs))
	})

	t.Run("divisor not a factor", func(t *testing.T) {
		data := randStringData(1000)

		s, err := NewStream(data, streamMsgOverhead+101)
		require.NoError(t, err)

		msgs := []Message{}
		for m := s.NextMessage(); m != nil; m = s.NextMessage() {
			msgs = append(msgs, m)
		}

		// There should be one extra message: the final completion message.
		assert.Equal(t, 11, len(msgs))
		assert.Equal(t, data, collate(t, msgs))
	})

	t.Run("completion includes data", func(t *testing.T) {
		data := randStringData(10000)

		s, err := NewStream(data, streamMsgOverhead+4000)
		require.NoError(t, err)

		msgs := []Message{}
		for m := s.NextMessage(); m != nil; m = s.NextMessage() {
			msgs = append(msgs, m)
		}

		assert.Equal(t, 3, len(msgs))
		assert.Equal(t, data, collate(t, msgs))
	})

	t.Run("small data overhead", func(t *testing.T) {
		data := randStringData(100)

		s, err := NewStream(data, streamMsgOverhead+1)
		require.NoError(t, err)

		msgs := []Message{}
		for m := s.NextMessage(); m != nil; m = s.NextMessage() {
			msgs = append(msgs, m)
		}

		// There should be one extra message: the final completion message.
		assert.Equal(t, len(data)+1, len(msgs))
		assert.Equal(t, data, collate(t, msgs))
	})
}

func TestSendStream(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		data := randStringData(10000)
		target := DeviceTarget("device-token")

		s, err := NewStream(data, 4000)
		require.NoError(t, err)

		mp := newMockProvider(-1)
		require.NoError(t, SendStream(context.Background(), mp, []Target{target}, s))

		roundtripped := collate(t, mp.sent)
		require.Equal(t, data, roundtripped)
	})

	t.Run("send stream error", func(t *testing.T) {
		data := randStringData(10000)
		target := DeviceTarget("device-token")

		s, err := NewStream(data, 4000)
		require.NoError(t, err)

		mp := newMockProvider(1)
		err = SendStream(context.Background(), mp, []Target{target}, s)
		require.Error(t, err)

		sse := new(SendStreamError)
		require.True(t, errors.As(err, sse))

		require.Equal(t, 1, sse.Successful)

		mp.errorAfter = -1
		require.NoError(t, SendStream(context.Background(), mp, []Target{target}, sse.Remaining))

		roundtripped := collate(t, mp.sent)
		require.Equal(t, data, roundtripped)
	})
}

// collate messages. Assumes the messages all come from a single stream and runs some checks against
// the stream contract expected by clients.
func collate(t *testing.T, m []Message) string {
	t.Helper()

	// We know that the messages coming into this function came straight out of a Stream and are in
	// order. However, to thoroughly test the message metadata, we should act like a client and
	// assume no knowledge of message ordering.

	type parsedMessage struct {
		data    string
		counter int
		last    bool
	}

	parsed := make([]parsedMessage, len(m))
	for i := range m {
		require.Contains(t, m[i].Data(), streamIndexKey)
		require.Contains(t, m[i].Data(), streamIDKey)
		require.Equal(t, m[0].Data()[streamIDKey], m[i].Data()[streamIDKey])

		if i < len(m)-1 {
			require.Contains(t, m[i].Data(), streamDataKey)
		}

		counter, err := strconv.Atoi(m[i].Data()[streamIndexKey])
		require.NoError(t, err)

		parsed[i] = parsedMessage{
			data:    m[i].Data()[streamDataKey],
			counter: counter,
		}
		_, parsed[i].last = m[i].Data()[streamCompleteKey]
	}

	slices.SortFunc(parsed, func(a, b parsedMessage) int {
		return a.counter - b.counter
	})

	collated := ""
	for i, pm := range parsed {
		require.Equal(t, i == len(parsed)-1, pm.last)
		require.Equal(t, i, pm.counter)
		collated += pm.data
	}

	return collated
}

type mockProvider struct {
	sent       []Message
	errorAfter int // -1 means disregard
}

// The mockProvider created will return errors after the first errorAfter calls to send. A value
// less than 0 indicates no errors should be returned.
func newMockProvider(errorAfter int) *mockProvider {
	return &mockProvider{[]Message{}, errorAfter}
}

func (mp *mockProvider) Send(_ context.Context, _ []Target, m Message) error {
	if mp.errorAfter >= 0 && len(mp.sent) >= mp.errorAfter {
		return errors.New("mock error")
	}
	mp.sent = append(mp.sent, m)
	return nil
}

func randStringData(n int) string {
	alphabet := `abcdefghijklmnopqrstuvwxyz`

	r := make([]rune, n)
	for i := range r {
		r[i] = rune(alphabet[rand.Intn(len(alphabet))])
	}
	return string(r)
}
