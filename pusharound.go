// Package pusharound implements a transport on top of push notification systems, as described in
// The Use of Push Notification in Censorship Circumvention by Diwen Xue and Roya Ensafi:
// https://www.petsymposium.org/foci/2023/foci-2023-0009.pdf
//
// This is the back-end side of the transport, intended for use with client libraries like the
// pusharound Flutter library (https://github.com/getlantern/pusharound-flutter).
//
// TODO: expand with use cases and examples.
package pusharound

import (
	"context"
	"fmt"
	"time"
)

// Target is the target for a push notification.
type Target struct {
	// Exactly one of the following will be non-empty.
	topic       string
	deviceToken string
}

// DeviceTarget is a target device for a push notification.
func DeviceTarget(deviceToken string) Target { return Target{deviceToken: deviceToken} }

// TopicTarget is a target topic for a push notification.
func TopicTarget(topic string) Target { return Target{topic: topic} }

func (t Target) valid() bool {
	return (t.topic == "" || t.deviceToken == "") && t.topic != t.deviceToken
}

// Message is a push notification message.
type Message interface {
	// Target is the intended recipient for this message.
	Target() Target

	// Data is the message payload.
	Data() map[string]string

	// TTL specifies how long this message should be stored (on the provider) for delivery. A value
	// of zero indicates that the TTL is unspecified. In this case, provider defaults will be used.
	TTL() time.Duration
}

type message struct {
	target Target
	data   map[string]string
	ttl    time.Duration
}

func (m message) Target() Target          { return m.target }
func (m message) Data() map[string]string { return m.data }
func (m message) TTL() time.Duration      { return m.ttl }

// NewMessage constructs a message with the given target, data, and TTL. A TTL of zero means the
// value is unspecified. In this case, provider defaults will be used.
func NewMessage(t Target, data map[string]string, ttl time.Duration) Message {
	return message{t, data, ttl}
}

// BatchSendError represents an error which may be returned by PushProvider.Send implementations
// when the input message slice contains more than one message.
type BatchSendError struct {
	// ByMessage holds the error for each message in the batch. ByMessage is exactly the length of
	// the input message slice and ByMessage[i] provides the error for message i in the input. Some
	// of these may be nil.
	ByMessage []error

	// ErrorString can be used to override the default implementation of Error().
	ErrorString string
}

func (se BatchSendError) Error() string {
	if se.ErrorString != "" {
		return se.ErrorString
	}

	successes := 0
	for _, err := range se.ByMessage {
		if err == nil {
			successes++
		}
	}
	failures := len(se.ByMessage) - successes

	return fmt.Sprintf("batch send error; %d succeeded, %d failed", successes, failures)
}

// PushProvider is a push notification provider.
type PushProvider interface {
	Send(context.Context, []Message) error
}
