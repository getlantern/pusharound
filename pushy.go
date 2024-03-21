package pusharound

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
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
//
// Messages to multiple targets will be batched, up to the 100,000 batch limit. It is an error to
// call Send with more than 100,000 Targets or with a payload over 4KB (the payload size is the
// size of Message.Data marshaled as JSON).
func NewPushyProvider(client http.Client, apiKey string) PushProvider {
	return pushyProvider{client, apiKey}
}

func (pp pushyProvider) Send(ctx context.Context, t []Target, m Message) error {
	if len(t) > pushyPushBatchLimit {
		return fmt.Errorf("number of targets (%d) over batch size limit (%d)", len(t), pushyPushBatchLimit)
	}

	req := pushyPushRequest{
		To:         make([]string, len(t)),
		Data:       m.Data(),
		TTLSeconds: int(m.TTL().Seconds()),
	}

	// TODO: can we mix topic and device targets?
	for i, target := range t {
		if !target.valid() {
			return errors.New("invalid target; must specify one of either device token or target")
		}
		if target.deviceToken != "" {
			req.To[i] = target.deviceToken
		} else {
			req.To[i] = fmt.Sprintf("/topics/%s", target.topic)
		}
	}

	return pp.sendPush(ctx, req)
}

func (pp pushyProvider) sendPush(ctx context.Context, req pushyPushRequest) error {
	body, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal error: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", "https://"+pushyPushEndpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to construct request: %w", err)
	}

	q := httpReq.URL.Query()
	q.Add("api_key", pp.apiKey)
	httpReq.URL.RawQuery = q.Encode()

	httpReq.Header.Add("Content-Type", "application/json")

	// TODO: strip API key from returned errors; can test with schemeless URL
	resp, err := pp.httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("http error: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		errResp, err := unmarshalPushyError(resp.Body)
		if err != nil && errors.Is(err, io.EOF) {
			return fmt.Errorf("status '%v'", resp.Status)
		} else if err != nil {
			return fmt.Errorf("status '%v' but failed to parse response: %w", resp.Status, err)
		}

		return fmt.Errorf("api error: %v", errResp.Error)
	}

	pushyResp := new(pushyPushResponse)
	if err := json.NewDecoder(resp.Body).Decode(pushyResp); err != nil {
		return fmt.Errorf("failed to parse response: %w", err)
	}

	if len(pushyResp.Info.Failed) > 0 {
		failed := make([]Target, len(pushyResp.Info.Failed))
		for i, f := range pushyResp.Info.Failed {
			if topic, ok := strings.CutPrefix(f, "/topics/"); ok {
				failed[i] = TopicTarget(topic)
			} else {
				failed[i] = DeviceTarget(f)
			}
		}

		return PartialFailure{Failed: failed, Cause: errors.New("API request failed")}
	}
	if !pushyResp.Success {
		// Pushy does not provide more information in this case.
		return errors.New("API request failed")
	}

	return nil
}

// Pushy API request and response formats are defined at https://pushy.me/docs/api.

// pushyPushRequest is a request to the Pushy API's push endpoint.
type pushyPushRequest struct {
	To         []string          `json:"to"`
	Data       map[string]string `json:"data"`
	TTLSeconds int               `json:"time_to_live"`
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
