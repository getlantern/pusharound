// Command pusharound-tester can be used to test the Pusharound system. This command-line utility
// can be used to send Pusharound messages or to stream files to Pusharound clients. This utility is
// intended primarily for use with the example application provided in the Pusharound Flutter
// client. See github.com/getlantern/pusharound-flutter.
package main

import (
	"context"
	"crypto/md5"
	"flag"
	"fmt"
	"net/http"
	"os"

	"github.com/getlantern/pusharound"
)

// streamPayloadLimit is the maximum payload of stream messages delivered by this utility. This is
// well below the actual payload limit enforced by Pushy to account for marshaling overhead (see
// pusharound.PushyPayloadLimit).
const streamPayloadLimit = 3000

var (
	apiKey         = flag.String("api-key", "", "your Pushy API key")
	msg            = flag.String("msg", "", "the message you would like to send")
	streamFile     = flag.String("stream-file", "", "a file to stream")
	deviceToken    = flag.String("device-token", "", "the device token you would like to target")
	ttl            = flag.Duration("ttl", 0, "the message or stream TTL; defaults to provider defaults")
	maxPayloadSize = flag.Int("max-payload", streamPayloadLimit, "max payload of stream messages")
)

func exit(fmtMsg string, a ...interface{}) {
	fmt.Fprintf(os.Stderr, fmtMsg, a...)
	fmt.Fprintln(os.Stderr)
	os.Exit(1)
}

func main() {
	flag.Parse()

	if *apiKey == "" {
		exit("api-key is required")
	}
	if *msg == "" && *streamFile == "" {
		exit("one of msg or stream-file is required")
	}
	if *msg != "" && *streamFile != "" {
		exit("only one of msg or stream-file may be specified")
	}
	if *deviceToken == "" {
		exit("device-token is required")
	}

	pp := pusharound.NewPushyProvider(http.Client{}, *apiKey)
	tgt := pusharound.DeviceTarget(*deviceToken)

	if *msg != "" {
		m := pusharound.NewTTLMessage(
			map[string]string{
				// The example client application expects message data under the 'message' key.
				"message": *msg,
			},
			*ttl,
		)
		if err := pp.Send(context.Background(), []pusharound.Target{tgt}, m); err != nil {
			exit("failed to send: %v", err)
		}
	} else {
		b, err := os.ReadFile(*streamFile)
		if err != nil {
			exit("failed to read file: %v", err)
		}

		h := md5.New()
		if _, err := h.Write(b); err != nil {
			exit("failed to hash file: %v", err)
		}
		fmt.Printf("streaming file; md5 checksum: %#x\n", h.Sum(nil))

		newMsg := func(msgData map[string]string) pusharound.TTLMessage {
			return pusharound.NewTTLMessage(msgData, *ttl)
		}

		s, err := pusharound.NewStream(string(b), *maxPayloadSize, newMsg)
		if err != nil {
			exit("failed to create stream: %v", err)
		}

		if err := s.Send(context.Background(), pp, []pusharound.Target{tgt}); err != nil {
			exit("failed to send stream: %v", err)
		}
	}

	fmt.Println("success!")
}
