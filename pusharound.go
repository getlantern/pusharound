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
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"
)

// Push notification messages carry custom payload data in a map. Pusharound uses special keys in
// this map to send metadata used by pusharound clients to collate streams and to distinguish
// pusharound messages from standard push notifications.
const (
	// streamIDKey is a key set in the custom data of pusharound notifications. This key will be
	// mapped to an identifier indicating the stream to which the notification belongs. This is used
	// to collate messages split across multiple notifications.
	streamIDKey = "pusharound-stream-id"

	// streamIDLen is the length of the stream ID value. This is a random hex-encoded byte sequence.
	streamIDLen = 8

	// streamIDNull is the stream ID set for one-off messages. When a client sees the null stream
	// ID, it knows not to expect further messages in the stream. Streams, in contrast to one-off
	// messages will use unique stream IDs and will never use the null stream ID.
	streamIDNull = "00000000"

	// streamIndexKey is a key set in the custom data of pusharound notifications. This key will
	// be mapped to an integer indicating this message's position in the stream.
	streamIndexKey = "pusharound-index"

	// streamIndexLen is the length of the stream index value. This is a 3-digit number indicating
	// the position of this message in the stream.
	streamIndexLen = 3

	// streamCompleteKey is a key included in the custom data of pusharound notifications. This key
	// is included only in the last message in the stream and is mapped to an empty string. The
	// presence of this key indicates to the client that this is the last message in the stream.
	streamCompleteKey = "pusharound-ok"

	// streamDataKey is a key included in the custom data of pusharound notifications. This key is
	// included only for notifications which are part of a stream of many messages. One-off messages
	// use custom keys to specify user data.
	streamDataKey = "pusharound-data"

	// streamMsgOverhead is the overhead of the strings included in each message's metadata. This does
	// not include marshaling overhead, which may add additional characters like quotes and commas.
	streamMsgOverhead = len(streamIDKey) + streamIDLen + len(streamIndexKey) + streamIndexLen + len(streamDataKey)
)

func init() {
	if len(streamCompleteKey) > len(streamDataKey) {
		// This is necessary to ensure we can send a final message in certain edge cases.
		// See stream.needsEmptyCompletionMessage
		panic("len(streamCompleteKey) must be <= len(streamDataKey)")
	}
}

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

// Topic returns the topic name if this is a topic target and an empty string otherwise.
func (t Target) Topic() string { return t.topic }

// DeviceToken returns the device token if this is a device target and an empty string otherwise.
func (t Target) DeviceToken() string { return t.deviceToken }

func (t Target) valid() bool {
	return (t.topic == "" || t.deviceToken == "") && t.topic != t.deviceToken
}

// Message is a push notification message.
//
// Custom implementations of Message should embed the implementation defined by this library (in
// NewMessage). This Message implementation contains metadata used to distinguish pusharound
// messages.
type Message interface {
	// Data is the message payload.
	Data() map[string]string

	// TODO: parameterize functions on Message interface; create PushyMessage with TTL
	// or: do all other providers use TTL?

	// TTL specifies how long this message should be stored (on the provider) for delivery. A value
	// of zero indicates that the TTL is unspecified. In this case, provider defaults will be used.
	TTL() time.Duration
}

type message struct {
	data map[string]string
	ttl  time.Duration
}

func (m message) Data() map[string]string { return m.data }
func (m message) TTL() time.Duration      { return m.ttl }

// NewMessage constructs a message with the given data and TTL. A TTL of zero means the value is
// unspecified. In this case, provider defaults will be used.
func NewMessage(data map[string]string, ttl time.Duration) Message {
	data[streamIDKey] = streamIDNull
	return message{data, ttl}
}

// PushProvider is a push notification provider.
type PushProvider interface {
	// Send sends a message to a group of targets.
	Send(context.Context, []Target, Message) error
}

// Stream is a stream of data to be sent via a push notification provider.
//
// Custom implementations of Stream should embed the implementation defined by this library (in
// NewStream). The Messages produced by this Stream implementation contain important metadata needed
// by clients to distinguish pusharound messages and collate streams.
type Stream interface {
	// NextMessage returns the next message in the stream. Returns nil when there are no more
	// messages in the stream.
	NextMessage() Message
}

type stream struct {
	maxPayloadSize int
	streamID       string
	data           string
	currentIndex   int

	// There is an edge case in the form of a highly constrained payload
	// (s.maxPayloadSize > streamMsgOverhead && s.maxPayloadSize < streamMsgOverhead+len(streamCompleteKey))
	// or a payload which divides such that the last message does not leave room for the
	// stream-complete key. In both of these cases, we send the final data payload, then follow with
	// a stream-complete message with no data.
	needsEmptyCompletionMessage bool
}

func (s *stream) NextMessage() Message {
	m := message{
		data: map[string]string{
			streamIDKey:    s.streamID,
			streamIndexKey: fmt.Sprintf("%03d", s.currentIndex),
		},
	}

	if s.needsEmptyCompletionMessage {
		s.needsEmptyCompletionMessage = false
		m.data[streamCompleteKey] = ""
		return m
	}

	if len(s.data) == 0 {
		return nil
	}

	available := s.maxPayloadSize - streamMsgOverhead
	if available >= len(s.data)+len(streamCompleteKey) {
		// We can finish the stream.
		m.data[streamDataKey] = s.data
		m.data[streamCompleteKey] = ""
		s.data = ""
	} else {
		end := min(available, len(s.data))
		m.data[streamDataKey] = s.data[:end]
		s.data = s.data[end:]
	}
	s.currentIndex++

	// Edge case: see stream.needsEmptyCompletionMessage.
	if _, ok := m.data[streamCompleteKey]; s.data == "" && !ok {
		s.needsEmptyCompletionMessage = true
	}

	return m
}

// NewStream initializes a stream of data to be sent via a push notification provider.
//
// maxPayloadSize specifies the maximum total amount of user data the message should contain. The
// sum length of all keys and values in the returned message's Data map will be equal to or less
// than this value.
//
// When choosing a value for maxPayloadSize, consider that marshaling will add overhead to the
// size of the payload on the wire. For example, a payload marshaled as JSON will contain
// additional characters like quotes, colons, and commas.
func NewStream(data string, maxPayloadSize int) (Stream, error) {
	id, err := newStreamID()
	if err != nil {
		return nil, fmt.Errorf("failed to generate stream ID: %w", err)
	}

	if maxPayloadSize <= streamMsgOverhead {
		return nil, fmt.Errorf("payload size limit (%d) <= overhead (%d)", maxPayloadSize, streamMsgOverhead)
	}

	return &stream{
		maxPayloadSize: maxPayloadSize,
		streamID:       id,
		data:           data,
	}, nil
}

// SendStreamError is the error returned by SendStream.
type SendStreamError struct {
	// Remaining is a Stream of all messages which were not successfully sent.
	Remaining Stream

	// Successful is the number of messages sent successfully. This can be used to track whether
	// retries are making progress.
	Successful int

	cause error
}

func (sse SendStreamError) Error() string {
	return fmt.Sprintf("failed to send full stream: %v", sse.cause)
}

func (sse SendStreamError) Unwrap() error {
	return sse.cause
}

type streamWithBufferedMessage struct {
	buffered Message
	s        Stream
}

func (s *streamWithBufferedMessage) NextMessage() Message {
	if s.buffered != nil {
		m := s.buffered
		s.buffered = nil
		return m
	}
	return s.s.NextMessage()
}

// SendStream sends a Stream of Messages using the specified PushProvider. Stops after the first
// error; retries are left to the caller. Returned errors are always SendStreamError.
func SendStream(ctx context.Context, p PushProvider, t []Target, s Stream) error {
	successful := 0

	for msg := s.NextMessage(); msg != nil; msg = s.NextMessage() {
		if err := p.Send(ctx, t, msg); err != nil {
			return SendStreamError{
				Remaining: &streamWithBufferedMessage{
					buffered: msg,
					s:        s,
				},
				Successful: successful,
				cause:      err,
			}
		}
		successful++
	}

	return nil
}

// PartialFailure is an error returned when a message is successfully sent for some targets, but not
// others.
type PartialFailure struct {
	Failed []Target
	Cause  error
}

func (pf PartialFailure) Error() string {
	return fmt.Sprintf("failed for %d targets: %v", len(pf.Failed), pf.Cause.Error())
}

func (pf PartialFailure) Unwrap() error {
	return pf.Cause
}

var nullStreamID = []byte{0, 0, 0, 0}

func init() {
	if streamIDLen%2 != 0 {
		// Necessary for the byte-length to hex-length conversion below.
		panic("streamIDLen must be divisible by 2")
	}
}

func newStreamID() (string, error) {
	b := make([]byte, streamIDLen/2)
	_, err := rand.Read(b)
	if err != nil {
		return "", fmt.Errorf("rand read error: %w", err)
	}
	if bytes.Equal(b, nullStreamID) {
		return newStreamID()
	}
	return hex.EncodeToString(b), nil
}
