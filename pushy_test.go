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

	t.Run("single target", func(t *testing.T) {
		var (
			deviceToken = "token"
			message     = "hello from the pusharound back-end"

			targets = []Target{
				DeviceTarget(deviceToken),
			}
			msg = NewTTLMessage(
				map[string]string{"message": message},
				0,
			)
		)

		p := newMockPushProvider(func(req pushyPushRequest) (*pushyPushResponse, *pushyErrorResponse) {
			assert.Equal(t, 1, len(req.To))
			assert.Equal(t, deviceToken, req.To[0])
			assert.Equal(t, message, req.Data["message"])

			return &pushyPushResponse{Success: true}, nil
		})

		err := p.Send(ctx, targets, msg)
		require.NoError(t, err)
	})

	t.Run("multiple targets", func(t *testing.T) {
		var (
			message = "hello from the pusharound back-end"

			targets = []Target{
				DeviceTarget("token-1"),
				DeviceTarget("token-2"),
				DeviceTarget("token-3"),
			}
			msg = NewTTLMessage(
				map[string]string{"message": message},
				0,
			)
		)

		p := newMockPushProvider(func(req pushyPushRequest) (*pushyPushResponse, *pushyErrorResponse) {
			assert.Equal(t, 3, len(req.To))
			assert.Equal(t, message, req.Data["message"])

			return &pushyPushResponse{Success: true}, nil
		})

		err := p.Send(ctx, targets, msg)
		require.NoError(t, err)
	})

	t.Run("some successful some failed", func(t *testing.T) {
		var (
			targets = []Target{
				DeviceTarget("0"),
				DeviceTarget("1"),
				DeviceTarget("2"),
				DeviceTarget("3"),
			}
			msg = NewTTLMessage(
				map[string]string{"message": "hello0"},
				0,
			)
		)

		p := newMockPushProvider(func(req pushyPushRequest) (*pushyPushResponse, *pushyErrorResponse) {
			assert.Equal(t, 4, len(req.To))

			failed := []string{}
			for _, target := range req.To {
				token, err := strconv.Atoi(target)
				assert.NoError(t, err)
				if token%2 == 0 {
					failed = append(failed, target)
				}
			}

			return &pushyPushResponse{
				Info: struct {
					Devices int
					Failed  []string
				}{
					Failed: failed,
				},
			}, nil
		})

		err := p.Send(ctx, targets, msg)
		require.Error(t, err)

		pf := new(PartialFailure)
		require.ErrorAs(t, err, pf)

		require.Len(t, pf.Failed, 2)
		require.Contains(t, pf.Failed, DeviceTarget("0"))
		require.Contains(t, pf.Failed, DeviceTarget("2"))
	})

}

type mockPushyProvider struct {
	PushProvider[TTLMessage]
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
