package pusharound

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
)

const (
	pushyHostname = "api.pushy.me"

	pushyPushEndpoint = pushyHostname + "/push"

	// See the 'to' field under request schema: https://pushy.me/docs/api/send-notifications.
	pushyPushBatchLimit = 100000
)

// pushyProvider implements the PushProvider interface for the Pushy notification system
// (https://pushy.me).
type pushyProvider struct {
	httpClient http.Client
	apiKey     string
}

// NewPushyProvider creates a new push notification provider using Pushy as a push notification
// system. The provided HTTP client will be used to make HTTP requests to the Pushy back-end.
//
// See https://pushy.me.
func NewPushyProvider(client http.Client, apiKey string) PushProvider {
	return pushyProvider{client, apiKey}
}

// Send a batch of messages. Messages with the same data payload will be batched together, up to the
// 100,000 device limit.
func (pp pushyProvider) Send(ctx context.Context, messages []Message) error {
	batchErr := BatchSendError{
		ByMessage: make([]error, len(messages)),
	}

	type message struct {
		Message
		index int
	}

	// Group messages by payload.
	byPayloadHash := map[string][]message{}
	for i, m := range messages {
		if m.Data() == nil {
			batchErr.ByMessage[i] = errors.New("no payload")
			continue
		}
		if !m.Target().valid() {
			batchErr.ByMessage[i] = errors.New("invalid target, must specify either topic or token")
			continue
		}

		hash := hashPayload(m.Data())
		msgsWithHash, ok := byPayloadHash[hash]
		if !ok {
			msgsWithHash = []message{}
		}
		byPayloadHash[hash] = append(msgsWithHash, message{m, i})
	}

	// Each value in the map now represents a batch of messages to send, all with the same payload.
	// For each batch, we build a request to the Pushy API and attempt to send.
	for _, msgs := range byPayloadHash {
		// TODO: split up batches over 100k limit

		req := pushyPushRequest{
			To:   []string{},
			Data: msgs[0].Data(),
		}

		for _, m := range msgs {
			target := m.Target()

			if target.topic != "" {
				req.To = append(req.To, fmt.Sprintf("/topics/%s", target.topic))
			} else {
				req.To = append(req.To, target.deviceToken)
			}
		}

		pushyResp, err := pp.sendPush(ctx, req)
		if err != nil {
			for _, m := range msgs {
				batchErr.ByMessage[m.index] = err
			}
			continue
		}

		if !pushyResp.Success {
			for _, m := range msgs {
				// Pushy does not provide more information in this case.
				batchErr.ByMessage[m.index] = errors.New("API request failed")
			}
		}
		if len(pushyResp.Info.Failed) > 0 {
			for _, deviceToken := range pushyResp.Info.Failed {
				if deviceToken == "" {
					continue
				}
				for _, m := range msgs {
					if deviceToken == m.Target().deviceToken {
						// The Pushy docs say that tokens in the failed list are those which "could
						// not be found in our database registered under... the [provided] API key".
						batchErr.ByMessage[m.index] = errors.New("invalid device token")
					}
				}
			}
		}

	}

	noErrors := true
	for _, err := range batchErr.ByMessage {
		if err != nil {
			noErrors = false
			break
		}
	}

	if noErrors {
		return nil
	}
	if len(batchErr.ByMessage) == 1 {
		return batchErr.ByMessage[0]
	}
	return batchErr
}

func (pp pushyProvider) sendPush(ctx context.Context, req pushyPushRequest) (*pushyPushResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal error: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", "https://"+pushyPushEndpoint, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("failed to construct request: %w", err)
	}

	q := httpReq.URL.Query()
	q.Add("api_key", pp.apiKey)
	httpReq.URL.RawQuery = q.Encode()

	httpReq.Header.Add("Content-Type", "application/json")

	// TODO: strip API key from returned errors; can test with schemeless URL
	resp, err := pp.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("http error: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		errResp, err := unmarshalPushyError(resp.Body)
		if err != nil && errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("status '%v'", resp.Status)
		} else if err != nil {
			return nil, fmt.Errorf("status '%v' but failed to parse response: %w", resp.Status, err)
		}

		return nil, fmt.Errorf("api error: %v", errResp.Error)
	}

	pushyResp := new(pushyPushResponse)
	if err := json.NewDecoder(resp.Body).Decode(pushyResp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	return pushyResp, nil
}

// Pushy API request and response formats are defined at https://pushy.me/docs/api.

// pushyPushRequest is a request to the Pushy API's push endpoint.
type pushyPushRequest struct {
	To   []string          `json:"to"`
	Data map[string]string `json:"data"`
}

// pushyPushResponse is a response from the Pushy API's push endpoint.
type pushyPushResponse struct {
	Success bool
	ID      string
	Info    struct {
		Devices int
		Failed  []string
	}
}

type pushyErrorResponse struct {
	Code  string
	Error string
}

// unmarshalPushyError unmarshals a JSON-encoded pushyErrorResponse from r. Returns io.EOF if r is
// empty.
func unmarshalPushyError(r io.Reader) (*pushyErrorResponse, error) {
	b, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("read error: %w", err)
	}

	if len(b) == 0 {
		return nil, io.EOF
	}

	errResp := new(pushyErrorResponse)
	if err := json.Unmarshal(b, errResp); err != nil {
		return nil, fmt.Errorf("unmarshal error: %w", err)
	}

	return errResp, nil
}

func hashPayload(payload map[string]string) string {
	// We need to write items from the map in a consistent order, so we sort the keys.
	keys := make([]string, 0, len(payload))
	for k := range payload {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// We write each map entry to the hash, using dashes and newlines as separators. It is possible
	// to construct two different maps which collide due to this scheme, but this is extremely
	// unlikely for our input.
	h := md5.New()
	for _, k := range keys {
		fmt.Fprintf(h, "%s-%s", k, payload[k])
		fmt.Fprintln(h)
	}
	return hex.EncodeToString(h.Sum(nil))
}
