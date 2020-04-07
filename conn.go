package grpc_net_conn

import (
	"net"
	"time"

	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
)

type Conn struct {
	// Stream is the stream to wrap into a Conn. This can be either a client
	// or server stream and we will perform correctly.
	Stream grpc.Stream

	// Request is the request type to use for sending data and Response
	// is the Response type.
	//
	// Both are required. When set, these must be non-nil allocated values.
	// These will be reused for each Read/Write operation.
	Request  proto.Message
	Response proto.Message

	// Encode is given the Request value and expects you to encode the
	// given byte slice into the message. You may use the slice directly.
	//
	// You do not have to encode the full byte slice in one packet. You can
	// choose to chunk your packets by returning 0 < n < len(p) and the
	// Conn will repeatedly send subsequent messages by slicing into the
	// byte slice.
	Encode func(proto.Message, []byte) (int, error)

	// Decode is given a Response value and expects you to decode the
	// response value into the byte slice given. You MUST decode up to
	// len(p) if available.
	//
	// This should return the data slice directly from m. The length of this
	// is used to determine if there is more data and the offset for the next
	// read.
	Decode func(m proto.Message, offset int, p []byte) ([]byte, error)

	readOffset int
}

func (c *Conn) Read(p []byte) (int, error) {
	// Attempt to read a value only if we're not still decoding a
	// partial read value from the last result.
	if c.readOffset == 0 {
		if err := c.Stream.RecvMsg(c.Response); err != nil {
			return 0, err
		}
	}

	// Decode into our slice
	data, err := c.Decode(c.Response, c.readOffset, p)

	// If we have an error or we've decoded the full amount then we're done.
	// The error case is obvious. The case where we've read the full amount
	// is also a terminating condition and err == nil (we know this since its
	// checking that case) so we return the full amount and no error.
	if err != nil || len(data) <= (len(p)+c.readOffset) {
		// Reset the read offset since we're done.
		n := len(data) - c.readOffset
		c.readOffset = 0

		// Reset our response value for the next read and so that we
		// don't potentially store a large response structure in memory.
		c.Response.Reset()

		return n, err
	}

	// We didn't read the full amount so we need to store this for future reads
	c.readOffset += len(p)

	return len(p), nil
}

func (c *Conn) Write([]byte) (int, error) {
	return 0, nil
}

func (c *Conn) Close() error {
	return nil
}

func (c *Conn) LocalAddr() net.Addr              { return nil }
func (c *Conn) RemoteAddr() net.Addr             { return nil }
func (c *Conn) SetDeadline(time.Time) error      { return nil }
func (c *Conn) SetReadDeadline(time.Time) error  { return nil }
func (c *Conn) SetWriteDeadline(time.Time) error { return nil }

var _ net.Conn = (*Conn)(nil)
