package grpc_net_conn

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/mitchellh/go-grpc-net-conn/testproto"
)

func TestConn(t *testing.T) {
	impl := &testServer{
		Send: [][]byte{
			[]byte("hello"),
		},
	}

	t.Run("full data", func(t *testing.T) {
		require := require.New(t)

		conn := testStreamConn(t, testStreamClient(t, impl))
		data := make([]byte, 1024)
		n, err := conn.Read(data)
		require.NoError(err)
		require.Equal(len("hello"), n)
		require.Equal("hello", string(data[:n]))
	})

	t.Run("partial read", func(t *testing.T) {
		require := require.New(t)

		conn := testStreamConn(t, testStreamClient(t, impl))
		data := make([]byte, 3)

		// Read first time partial
		n, err := conn.Read(data)
		require.NoError(err)
		require.Equal(3, n)
		require.Equal("hel", string(data[:n]))

		// Read again full result
		n, err = conn.Read(data)
		require.NoError(err)
		require.Equal(2, n)
		require.Equal("lo", string(data[:n]))
	})
}

type testServer struct {
	Send [][]byte
}

func (s *testServer) Stream(stream testproto.TestService_StreamServer) error {
	for _, data := range s.Send {
		if err := stream.Send(&testproto.Bytes{Data: data}); err != nil {
			return err
		}
	}

	<-stream.Context().Done()
	return nil
}

var _ testproto.TestServiceServer = (*testServer)(nil)
