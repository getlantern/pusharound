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

	// PushyPayloadLimit is the maximum size of a payload sent over Pushy when marshaled as JSON.
	// This does not include any of pusharound's own metadata added to the payload. Essentially, if
	// a map called 'data' is used to create a Message and that Message is to be sent over a
	// PushyProvider, then the result of json.Marshal(data) should be no larger than this value.
	//
	// This value was determined via experimentation as Pushy's documentation was found to be
	// slightly inaccurate. The actual limit determined by experimentation has been buffered a bit
	// to account for possible changes to Pushy's back-end or unforeseen complexities in how Pushy
	// calculates payload sizes.
	//
	// Special note for streaming: the final payload size is the result of marshaling the data as
	// JSON and thus may involve escaping certain characters. For data which might involve a large
	// amount of escaping (e.g. a file with many newlines and double-quotes), the stream payload
	// limit may need to be substantially less than this value. To determine the value needed for
	// your data, try marshaling it as JSON to determine the marshaling overhead.
	PushyPayloadLimit = 3900

	// Pushy will reject any requests with a marshaled 'data' payload of greater than 3993 bytes.
	// This value is in slight contradiction to the documentation (see the 'data' field under
	// request schema: https://pushy.me/docs/api/send-notifications), which claims a maximum size of
	// 4096.
	//
	// Unfortunately, these rejections are also silent. The API will respond 200 OK, but the
	// notification will not be sent and an error will be logged in the Pushy portal.
	//
	// Accounting for the stream ID attached to all messages, this leaves 3959 bytes for data
	// provided by users of the pusharound package. In communicating this limit via
	// PushyPayloadLimit, we buffer this 3959-byte limit to 3900 bytes. In guarding against actual
	// errors sending notifications to Pushy, we check the length of the marshaled payload against
	// this value.
	actualPushyPayloadLimit = 3980
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
// call Send with more than 100,000 Targets or with a payload over PushyPayloadLimit. Send may
// return a PartialFailure if the request fails for some targets, but not all.
func NewPushyProvider(client http.Client, apiKey string) PushProvider[TTLMessage] {
	return pushyProvider{client, apiKey}
}

func (pp pushyProvider) Send(ctx context.Context, t []Target, m TTLMessage) error {
	if len(t) > pushyPushBatchLimit {
		return fmt.Errorf("number of targets (%d) over batch size limit (%d)", len(t), pushyPushBatchLimit)
	}

	// Pushy silently fails for payloads over a certain size. This is entirely dependendent on the
	// size of the 'data' field in the request body, after JSON encoding. The contents of this field
	// are represented here by m.Data(). It has proven too complex to reliably predict the size of
	// m.Data() once encoded without actually calculating the escape sequences required for each key
	// and value.
	//
	// As a result of all of this, we encode m.Data() and reject the message ourselves if we believe
	// Pushy will silently fail for this message.
	encodedData, err := json.Marshal(m.Data())
	if err != nil {
		return fmt.Errorf("error encoding message data: %v", err)
	}
	if len(encodedData) > actualPushyPayloadLimit {
		return errors.New("payload after marshaling is over Pushy payload limit")
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
			req.To[i] = "/topics/" + target.topic
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
		if errors.Is(err, io.EOF) {
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
