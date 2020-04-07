package grpc_net_conn

import (
	"context"
	"net"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/mitchellh/go-grpc-net-conn/testproto"
)

func testStreamConn(
	t *testing.T,
	stream grpc.Stream,
) *Conn {
	return &Conn{
		Stream:   stream,
		Request:  &testproto.Bytes{},
		Response: &testproto.Bytes{},
		Encode: func(m proto.Message, p []byte) (int, error) {
			m.(*testproto.Bytes).Data = p
			return len(p), nil
		},

		Decode: func(m proto.Message, offset int, p []byte) ([]byte, error) {
			bs := m.(*testproto.Bytes).Data
			copy(p, bs[offset:])
			return bs, nil
		},
	}
}

// testStreamClient returns a fully connected stream client.
func testStreamClient(
	t *testing.T,
	impl testproto.TestServiceServer,
) testproto.TestService_StreamClient {
	// Get our gRPC client/server
	conn, server := testGRPCConn(t, func(s *grpc.Server) {
		testproto.RegisterTestServiceServer(s, impl)
	})
	t.Cleanup(func() { server.Stop() })
	t.Cleanup(func() { conn.Close() })

	// Connect for streaming
	resp, err := testproto.NewTestServiceClient(conn).Stream(
		context.Background())
	require.NoError(t, err)

	// Return our client
	return resp
}

// testGRPCConn returns a gRPC client conn and grpc server that are connected
// together and configured. The register function is used to register services
// prior to the Serve call. This is used to test gRPC connections.
func testGRPCConn(t *testing.T, register func(*grpc.Server)) (*grpc.ClientConn, *grpc.Server) {
	t.Helper()

	// Create a listener
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	server := grpc.NewServer()
	register(server)
	go server.Serve(l)

	// Connect to the server
	conn, err := grpc.Dial(
		l.Addr().String(),
		grpc.WithBlock(),
		grpc.WithInsecure())
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	// Connection successful, close the listener
	l.Close()

	return conn, server
}
