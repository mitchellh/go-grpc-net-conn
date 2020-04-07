package grpc_net_conn

import (
	"github.com/golang/protobuf/proto"
)

// Encoder encodes a byte slice to write into the destination proto.Message.
// You do not need to copy the slice; you may use it directly.
//
// You do not have to encode the full byte slice in one packet. You can
// choose to chunk your packets by returning 0 < n < len(p) and the
// Conn will repeatedly send subsequent messages by slicing into the
// byte slice.
type Encoder func(proto.Message, []byte) (int, error)

// ChunkedEncoder ensures that data to encode is chunked at the proper size.
func ChunkedEncoder(enc Encoder, size int) Encoder {
	return func(msg proto.Message, p []byte) (int, error) {
		if len(p) > size {
			p = p[:size]
		}

		return enc(msg, p)
	}
}
