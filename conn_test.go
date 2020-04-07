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

		conn := testStreamConn(testStreamClient(t, impl))
		data := make([]byte, 1024)
		n, err := conn.Read(data)
		require.NoError(err)
		require.Equal(len("hello"), n)
		require.Equal("hello", string(data[:n]))
	})

	t.Run("partial read", func(t *testing.T) {
		require := require.New(t)

		conn := testStreamConn(testStreamClient(t, impl))
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

func TestConn_chunkedWrites(t *testing.T) {
	impl := &testServer{
		Chunk: 3,
		Send: [][]byte{
			[]byte("hello"),
		},
	}

	require := require.New(t)

	conn := testStreamConn(testStreamClient(t, impl))
	data := make([]byte, 1024)

	// We expect two chunks
	n, err := conn.Read(data)
	require.NoError(err)
	require.Equal(3, n)
	require.Equal("hel", string(data[:n]))

	n, err = conn.Read(data)
	require.NoError(err)
	require.Equal(2, n)
	require.Equal("lo", string(data[:n]))
}

type testServer struct {
	Send  [][]byte
	Chunk int
}

func (s *testServer) Stream(stream testproto.TestService_StreamServer) error {
	// Get our conn
	conn := testStreamConn(stream)
	if s.Chunk > 0 {
		conn.Encode = ChunkedEncoder(conn.Encode, s.Chunk)
	}

	for _, data := range s.Send {
		if _, err := conn.Write(data); err != nil {
			return err
		}
	}

	<-stream.Context().Done()
	return nil
}

var _ testproto.TestServiceServer = (*testServer)(nil)
