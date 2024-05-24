package pusharound_test

import (
	"context"
	"net/http"
	"os"
	"time"

	"github.com/getlantern/pusharound"
)

func ExamplePushProvider_pushy() {
	pushyProvider := pusharound.NewPushyProvider(http.Client{}, `my-api-key`)

	msg := pusharound.NewTTLMessage(map[string]string{
		"field1": "foo",
		"field2": "bar",
	}, 24*60*time.Minute)
	tgt1 := pusharound.DeviceTarget(`my-device-token`)
	tgt2 := pusharound.DeviceTarget(`my-other-device-token`)

	err := pushyProvider.Send(context.Background(), []pusharound.Target{tgt1, tgt2}, msg)
	if err != nil {
		panic(err)
	}
}

func ExampleStream_pushy() {
	// If the data requires many escape sequences to marshal as JSON, this limit may need to be
	// reduced. See the comment on PushyPayloadLimit.
	const maxPayloadSize = pusharound.PushyPayloadLimit

	data, err := os.ReadFile(`file-to-stream`)
	if err != nil {
		panic(err)
	}

	ttl := 24 * 60 * time.Minute
	msgConstructor := func(data map[string]string) pusharound.TTLMessage {
		return pusharound.NewTTLMessage(data, ttl)
	}

	s, err := pusharound.NewStream(string(data), maxPayloadSize, msgConstructor)
	if err != nil {
		panic(err)
	}

	pp := pusharound.NewPushyProvider(http.Client{}, `my-api-key`)
	tgt := pusharound.DeviceTarget(`my-device-token`)

	err = s.Send(context.Background(), pp, []pusharound.Target{tgt})
	if err != nil {
		panic(err)
	}
}
