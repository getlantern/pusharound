package pusharound

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPushySend(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("one message", func(t *testing.T) {
		var (
			deviceToken = "token"
			message     = "hello from the pusharound back-end"

			msgs = []Message{
				NewMessage(
					DeviceTarget(deviceToken),
					map[string]string{"message": message},
					0,
				),
			}
		)

		p := newMockPushProvider(func(req pushyPushRequest) (*pushyPushResponse, *pushyErrorResponse) {
			assert.Equal(t, 1, len(req.To))
			assert.Equal(t, deviceToken, req.To[0])
			assert.Equal(t, message, req.Data["message"])

			return &pushyPushResponse{Success: true}, nil
		})

		err := p.Send(ctx, msgs)
		require.NoError(t, err)
	})

	t.Run("same message different targets", func(t *testing.T) {
		var (
			message = "hello from the pusharound back-end"

			msgs = []Message{
				NewMessage(
					DeviceTarget("token-1"),
					map[string]string{"message": message},
					0,
				),
				NewMessage(
					DeviceTarget("token-2"),
					map[string]string{"message": message},
					0,
				),
				NewMessage(
					DeviceTarget("token-3"),
					map[string]string{"message": message},
					0,
				),
			}
		)

		p := newMockPushProvider(func(req pushyPushRequest) (*pushyPushResponse, *pushyErrorResponse) {
			assert.Equal(t, 3, len(req.To))
			assert.Equal(t, message, req.Data["message"])

			return &pushyPushResponse{Success: true}, nil
		})

		err := p.Send(ctx, msgs)
		require.NoError(t, err)
	})

	t.Run("multiple messages different targets", func(t *testing.T) {
		var (
			message1 = "hello from the pusharound back-end"
			message2 = "hello again from the pusharound back-end"

			msgs = []Message{
				NewMessage(
					DeviceTarget("0"),
					map[string]string{"message": message1},
					0,
				),
				NewMessage(
					DeviceTarget("1"),
					map[string]string{"message": message2},
					0,
				),
				NewMessage(
					DeviceTarget("2"),
					map[string]string{"message": message1},
					0,
				),
				NewMessage(
					DeviceTarget("3"),
					map[string]string{"message": message2},
					0,
				),
			}
		)

		p := newMockPushProvider(func(req pushyPushRequest) (*pushyPushResponse, *pushyErrorResponse) {
			for _, target := range req.To {
				token, err := strconv.Atoi(target)
				assert.NoError(t, err)

				if token%2 == 0 {
					assert.Equal(t, message1, req.Data["message"])
				} else {
					assert.Equal(t, message2, req.Data["message"])
				}
			}

			return &pushyPushResponse{Success: true}, nil
		})

		err := p.Send(ctx, msgs)
		require.NoError(t, err)
	})

	t.Run("some successful some failed", func(t *testing.T) {
		var (
			// n.b. These need to each have different messages or they will all be batched together
			// and either all succeed or all fail.
			msgs = []Message{
				NewMessage(
					DeviceTarget("0"),
					map[string]string{"message": "hello0"},
					0,
				),
				NewMessage(
					DeviceTarget("1"),
					map[string]string{"message": "hello1"},
					0,
				),
				NewMessage(
					DeviceTarget("2"),
					map[string]string{"message": "hello2"},
					0,
				),
				NewMessage(
					DeviceTarget("3"),
					map[string]string{"message": "hello3"},
					0,
				),
			}
		)

		p := newMockPushProvider(func(req pushyPushRequest) (*pushyPushResponse, *pushyErrorResponse) {
			assert.Equal(t, 1, len(req.To))
			token, err := strconv.Atoi(req.To[0])
			assert.NoError(t, err)

			if token%2 == 0 {
				return nil, &pushyErrorResponse{Error: "don't like even numbers"}
			}
			return &pushyPushResponse{Success: true}, nil
		})

		err := p.Send(ctx, msgs)
		require.Error(t, err)

		batchErr := new(BatchSendError)
		require.True(t, errors.As(err, batchErr))

		for i := 0; i < 4; i++ {
			if i%2 == 0 {
				require.Error(t, batchErr.ByMessage[i])
			} else {
				require.NoError(t, batchErr.ByMessage[i])
			}
		}
	})

	t.Run("some successful some failed one batch", func(t *testing.T) {
		var (
			message = "hello from the pusharound backend"

			msgs = []Message{
				NewMessage(
					DeviceTarget("0"),
					map[string]string{"message": message},
					0,
				),
				NewMessage(
					DeviceTarget("1"),
					map[string]string{"message": message},
					0,
				),
				NewMessage(
					DeviceTarget("2"),
					map[string]string{"message": message},
					0,
				),
				NewMessage(
					DeviceTarget("3"),
					map[string]string{"message": message},
					0,
				),
			}
		)

		p := newMockPushProvider(func(req pushyPushRequest) (*pushyPushResponse, *pushyErrorResponse) {
			resp := &pushyPushResponse{Success: true}

			for _, target := range req.To {
				token, err := strconv.Atoi(target)
				assert.NoError(t, err)

				if token%2 == 0 {
					resp.Info.Failed = append(resp.Info.Failed, target)
				}
			}

			return resp, nil
		})

		err := p.Send(ctx, msgs)
		require.Error(t, err)

		batchErr := new(BatchSendError)
		require.True(t, errors.As(err, batchErr))

		for i := 0; i < 4; i++ {
			if i%2 == 0 {
				require.Error(t, batchErr.ByMessage[i])
			} else {
				require.NoError(t, batchErr.ByMessage[i])
			}
		}
	})
}

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

type mockPushyProvider struct {
	PushProvider
	closeChan chan struct{}
}

// mockPushyProvider is an implementation of PushProvider using pushyProvider. The input handler
// controls the responses pushyProvider sees. The handler will be invoked in a separate routine
// (and therefore it is inappropriate to call testing.T.FailNow inside this handler).
func newMockPushProvider(handler func(pushyPushRequest) (*pushyPushResponse, *pushyErrorResponse)) mockPushyProvider {
	var (
		reqs   = make(chan pushyPushRequest)
		resps  = make(chan pushyPushResponse)
		errs   = make(chan pushyErrorResponse)
		closed = make(chan struct{})
	)

	tt := testTransport{
		roundTrip: func(req *http.Request) (*http.Response, error) {
			defer req.Body.Close()

			// Send the request.

			parsedBody := new(pushyPushRequest)
			if err := json.NewDecoder(req.Body).Decode(parsedBody); err != nil {
				return nil, fmt.Errorf("failed to parse body: %w", err)
			}

			select {
			case reqs <- *parsedBody:
			case <-closed:
				return nil, errors.New("mock provider closed")
			}

			// Wait for a response.

			var toEncode interface{}
			var statusCode int

			select {
			case resp := <-resps:
				toEncode = resp
				statusCode = 200
			case errResp := <-errs:
				toEncode = errResp
				statusCode = 400
			case <-closed:
				return nil, errors.New("mock provider closed")
			}

			respBody := new(bytes.Buffer)
			if err := json.NewEncoder(respBody).Encode(toEncode); err != nil {
				return nil, fmt.Errorf("failed to marshal response: %w", err)
			}

			return &http.Response{
				Request:    req,
				Body:       noopCloser{respBody},
				StatusCode: statusCode,
			}, nil
		},
	}

	go func() {
		for {
			select {
			case req := <-reqs:
				resp, errResp := handler(req)
				if errResp != nil {
					select {
					case errs <- *errResp:
						continue
					case <-closed:
						return
					}
				}
				select {
				case resps <- *resp:
				case <-closed:
					return
				}
			case <-closed:
				return
			}
		}
	}()

	return mockPushyProvider{
		NewPushyProvider(
			http.Client{Transport: tt},
			"my-api-key",
		),
		closed,
	}
}

func (mpp mockPushyProvider) Close() error {
	close(mpp.closeChan)
	return nil
}

type testTransport struct {
	roundTrip func(*http.Request) (*http.Response, error)
}

func (tt testTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return tt.roundTrip(req)
}

type noopCloser struct {
	io.Reader
}

func (nc noopCloser) Close() error { return nil }
