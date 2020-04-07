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

	// Encode encodes messages into the Request. See Encoder for more information.
	Encode Encoder

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

func (c *Conn) Write(p []byte) (int, error) {
	total := len(p)
	for {
		// Encode our data into the request. Any error means we abort.
		n, err := c.Encode(c.Request, p)
		if err != nil {
			return 0, err
		}

		// Send our message. Any error we also just abort out.
		if err := c.Stream.SendMsg(c.Request); err != nil {
			return 0, err
		}

		// If we sent the full amount of data, we're done. We respond with
		// "total" in case we sent across multiple frames.
		if n == len(p) {
			return total, nil
		}

		// We sent partial data so we continue writing the remainder
		p = p[n:]
	}
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
