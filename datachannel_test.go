package datachannel

import (
	"encoding/binary"
	"reflect"
	"testing"
	"time"

	"github.com/pion/logging"
	"github.com/pion/sctp"
	"github.com/pion/transport/test"
	"github.com/stretchr/testify/assert"
)

// Check we implement our ReadWriteCloser
var _ ReadWriteCloser = (*DataChannel)(nil)

func createNewAssociationPair(br *test.Bridge) (*sctp.Association, *sctp.Association, error) {
	var a0, a1 *sctp.Association
	var err0, err1 error
	loggerFactory := logging.NewDefaultLoggerFactory()

	handshake0Ch := make(chan bool)
	handshake1Ch := make(chan bool)

	go func() {
		a0, err0 = sctp.Client(sctp.Config{
			NetConn:       br.GetConn0(),
			LoggerFactory: loggerFactory,
		})
		handshake0Ch <- true
	}()
	go func() {
		a1, err1 = sctp.Client(sctp.Config{
			NetConn:       br.GetConn1(),
			LoggerFactory: loggerFactory,
		})
		handshake1Ch <- true
	}()

	a0handshakeDone := false
	a1handshakeDone := false
loop1:
	for i := 0; i < 100; i++ {
		time.Sleep(10 * time.Millisecond)
		br.Tick()

		select {
		case a0handshakeDone = <-handshake0Ch:
			if a1handshakeDone {
				break loop1
			}
		case a1handshakeDone = <-handshake1Ch:
			if a0handshakeDone {
				break loop1
			}
		default:
		}
	}

	if err0 != nil {
		return nil, nil, err0
	}
	if err1 != nil {
		return nil, nil, err1
	}

	return a0, a1, nil
}

func closeAssociationPair(br *test.Bridge, a0, a1 *sctp.Association) {
	close0Ch := make(chan bool)
	close1Ch := make(chan bool)

	go func() {
		//nolint:errcheck,gosec
		a0.Close()
		close0Ch <- true
	}()
	go func() {
		//nolint:errcheck,gosec
		a1.Close()
		close1Ch <- true
	}()

	a0closed := false
	a1closed := false
loop1:
	for i := 0; i < 100; i++ {
		time.Sleep(10 * time.Millisecond)
		br.Tick()

		select {
		case a0closed = <-close0Ch:
			if a1closed {
				break loop1
			}
		case a1closed = <-close1Ch:
			if a0closed {
				break loop1
			}
		default:
		}
	}
}

func prOrderedTest(t *testing.T, channelType ChannelType) {
	sbuf := make([]byte, 1000)
	rbuf := make([]byte, 2000)

	br := test.NewBridge()
	loggerFactory := logging.NewDefaultLoggerFactory()

	a0, a1, err := createNewAssociationPair(br)
	if !assert.Nil(t, err, "failed to create associations") {
		assert.FailNow(t, "failed due to earlier error")
	}

	cfg := &Config{
		ChannelType:          channelType,
		ReliabilityParameter: 0,
		Label:                "data",
		LoggerFactory:        loggerFactory,
	}

	dc0, err := Dial(a0, 100, cfg)
	assert.Nil(t, err, "Dial() should succeed")
	br.Process()

	dc1, err := Accept(a1, &Config{
		LoggerFactory: loggerFactory,
	})
	assert.Nil(t, err, "Accept() should succeed")
	br.Process()

	assert.True(t, reflect.DeepEqual(dc0.Config, *cfg), "local config should match")
	assert.True(t, reflect.DeepEqual(dc1.Config, *cfg), "remote config should match")

	var n int

	binary.BigEndian.PutUint32(sbuf, 1)
	n, err = dc0.WriteDataChannel(sbuf, true)
	assert.Nil(t, err, "Read() should succeed")
	assert.Equal(t, len(sbuf), n, "data length should match")

	binary.BigEndian.PutUint32(sbuf, 2)
	n, err = dc0.WriteDataChannel(sbuf, true)
	assert.Nil(t, err, "Read() should succeed")
	assert.Equal(t, len(sbuf), n, "data length should match")

	time.Sleep(100 * time.Millisecond)
	br.Drop(0, 0, 1) // drop the first packet on the wire
	br.Process()

	var isString bool

	n, isString, err = dc1.ReadDataChannel(rbuf)
	assert.Nil(t, err, "Read() should succeed")
	assert.True(t, isString, "should return isString being true")
	assert.Equal(t, uint32(2), binary.BigEndian.Uint32(rbuf[:n]), "data should match")

	//nolint:errcheck,gosec
	dc0.Close()
	//nolint:errcheck,gosec
	dc1.Close()
	br.Process()

	closeAssociationPair(br, a0, a1)
}

func prUnorderedTest(t *testing.T, channelType ChannelType) {
	sbuf := make([]byte, 1000)
	rbuf := make([]byte, 2000)

	br := test.NewBridge()
	loggerFactory := logging.NewDefaultLoggerFactory()

	a0, a1, err := createNewAssociationPair(br)
	if !assert.Nil(t, err, "failed to create associations") {
		assert.FailNow(t, "failed due to earlier error")
	}

	cfg := &Config{
		ChannelType:          channelType,
		ReliabilityParameter: 0,
		Label:                "data",
		LoggerFactory:        loggerFactory,
	}

	dc0, err := Dial(a0, 100, cfg)
	assert.Nil(t, err, "Dial() should succeed")
	br.Process()

	dc1, err := Accept(a1, &Config{
		LoggerFactory: loggerFactory,
	})
	assert.Nil(t, err, "Accept() should succeed")
	br.Process()

	assert.True(t, reflect.DeepEqual(dc0.Config, *cfg), "local config should match")
	assert.True(t, reflect.DeepEqual(dc1.Config, *cfg), "remote config should match")

	var n int

	binary.BigEndian.PutUint32(sbuf, 1)
	n, err = dc0.WriteDataChannel(sbuf, true)
	assert.Nil(t, err, "Read() should succeed")
	assert.Equal(t, len(sbuf), n, "data length should match")

	binary.BigEndian.PutUint32(sbuf, 2)
	n, err = dc0.WriteDataChannel(sbuf, true)
	assert.Nil(t, err, "Read() should succeed")
	assert.Equal(t, len(sbuf), n, "data length should match")

	binary.BigEndian.PutUint32(sbuf, 3)
	n, err = dc0.WriteDataChannel(sbuf, true)
	assert.Nil(t, err, "Read() should succeed")
	assert.Equal(t, len(sbuf), n, "data length should match")

	time.Sleep(100 * time.Millisecond)
	br.Drop(0, 0, 1)    // drop the first packet on the wire
	err = br.Reorder(0) // reorder the rest of the packet
	assert.Nil(t, err, "reorder failed")
	br.Process()

	var isString bool

	n, isString, err = dc1.ReadDataChannel(rbuf)
	assert.Nil(t, err, "Read() should succeed")
	assert.True(t, isString, "should return isString being true")
	assert.Equal(t, uint32(3), binary.BigEndian.Uint32(rbuf[:n]), "data should match")

	n, isString, err = dc1.ReadDataChannel(rbuf)
	assert.Nil(t, err, "Read() should succeed")
	assert.True(t, isString, "should return isString being true")
	assert.Equal(t, uint32(2), binary.BigEndian.Uint32(rbuf[:n]), "data should match")

	//nolint:errcheck,gosec
	dc0.Close()
	//nolint:errcheck,gosec
	dc1.Close()
	br.Process()

	closeAssociationPair(br, a0, a1)
}

func TestDataChannel(t *testing.T) {
	loggerFactory := logging.NewDefaultLoggerFactory()

	sbuf := make([]byte, 1000)
	rbuf := make([]byte, 1500)

	t.Run("ChannelTypeReliableOrdered", func(t *testing.T) {
		br := test.NewBridge()

		a0, a1, err := createNewAssociationPair(br)
		if !assert.Nil(t, err, "failed to create associations") {
			assert.FailNow(t, "failed due to earlier error")
		}

		cfg := &Config{
			ChannelType:          ChannelTypeReliable,
			ReliabilityParameter: 123,
			Label:                "data",
			LoggerFactory:        loggerFactory,
		}

		dc0, err := Dial(a0, 100, cfg)
		assert.Nil(t, err, "Dial() should succeed")
		br.Process()

		dc1, err := Accept(a1, &Config{
			LoggerFactory: loggerFactory,
		})
		assert.Nil(t, err, "Accept() should succeed")
		br.Process()

		assert.True(t, reflect.DeepEqual(dc0.Config, *cfg), "local config should match")
		assert.True(t, reflect.DeepEqual(dc1.Config, *cfg), "remote config should match")

		br.ReorderNextNWrites(0, 2) // reordering on the wire

		var n int
		binary.BigEndian.PutUint32(sbuf, uint32(1))
		n, err = dc0.Write(sbuf)
		assert.Nil(t, err, "Write() should succeed")
		assert.Equal(t, len(sbuf), n, "data length should match")

		binary.BigEndian.PutUint32(sbuf, uint32(2))
		n, err = dc0.Write(sbuf)
		assert.Nil(t, err, "Write() should succeed")
		assert.Equal(t, len(sbuf), n, "data length should match")

		assert.Nil(t, err, "reorder failed")
		br.Process()

		n, err = dc1.Read(rbuf)
		assert.Nil(t, err, "Read() should succeed")
		assert.Equal(t, uint32(1), binary.BigEndian.Uint32(rbuf[:n]), "data should match")

		n, err = dc1.Read(rbuf)
		assert.Nil(t, err, "Read() should succeed")
		assert.Equal(t, uint32(2), binary.BigEndian.Uint32(rbuf[:n]), "data should match")

		//nolint:errcheck,gosec
		dc0.Close()
		//nolint:errcheck,gosec
		dc1.Close()
		br.Process()

		closeAssociationPair(br, a0, a1)
	})

	t.Run("ChannelTypeReliableUnordered", func(t *testing.T) {
		sbuf := make([]byte, 1000)
		rbuf := make([]byte, 1500)

		br := test.NewBridge()

		a0, a1, err := createNewAssociationPair(br)
		if !assert.Nil(t, err, "failed to create associations") {
			assert.FailNow(t, "failed due to earlier error")
		}

		cfg := &Config{
			ChannelType:          ChannelTypeReliableUnordered,
			ReliabilityParameter: 123,
			Label:                "data",
			LoggerFactory:        loggerFactory,
		}

		dc0, err := Dial(a0, 100, cfg)
		assert.Nil(t, err, "Dial() should succeed")
		br.Process()

		dc1, err := Accept(a1, &Config{
			LoggerFactory: loggerFactory,
		})
		assert.Nil(t, err, "Accept() should succeed")
		br.Process()

		assert.True(t, reflect.DeepEqual(dc0.Config, *cfg), "local config should match")
		assert.True(t, reflect.DeepEqual(dc1.Config, *cfg), "remote config should match")

		var n int

		binary.BigEndian.PutUint32(sbuf, 1)
		n, err = dc0.WriteDataChannel(sbuf, true)
		assert.Nil(t, err, "Read() should succeed")
		assert.Equal(t, len(sbuf), n, "data length should match")

		binary.BigEndian.PutUint32(sbuf, 2)
		n, err = dc0.WriteDataChannel(sbuf, true)
		assert.Nil(t, err, "Read() should succeed")
		assert.Equal(t, len(sbuf), n, "data length should match")

		time.Sleep(100 * time.Millisecond)
		err = br.Reorder(0) // reordering on the wire
		assert.Nil(t, err, "reorder failed")
		br.Process()

		var isString bool

		n, isString, err = dc1.ReadDataChannel(rbuf)
		assert.Nil(t, err, "Read() should succeed")
		assert.True(t, isString, "should return isString being true")
		assert.Equal(t, uint32(2), binary.BigEndian.Uint32(rbuf[:n]), "data should match")

		n, isString, err = dc1.ReadDataChannel(rbuf)
		assert.Nil(t, err, "Read() should succeed")
		assert.True(t, isString, "should return isString being true")
		assert.Equal(t, uint32(1), binary.BigEndian.Uint32(rbuf[:n]), "data should match")

		//nolint:errcheck,gosec
		dc0.Close()
		//nolint:errcheck,gosec
		dc1.Close()
		br.Process()

		closeAssociationPair(br, a0, a1)
	})

	t.Run("ChannelTypePartialReliableRexmit", func(t *testing.T) {
		prOrderedTest(t, ChannelTypePartialReliableRexmit)
	})

	t.Run("ChannelTypePartialReliableRexmitUnordered", func(t *testing.T) {
		prUnorderedTest(t, ChannelTypePartialReliableRexmitUnordered)
	})

	t.Run("ChannelTypePartialReliableTimed", func(t *testing.T) {
		prOrderedTest(t, ChannelTypePartialReliableTimed)
	})

	t.Run("ChannelTypePartialReliableTimedUnordered", func(t *testing.T) {
		prUnorderedTest(t, ChannelTypePartialReliableTimedUnordered)
	})

}

func TestDataChannelBufferedAmount(t *testing.T) {
	var nCbs int
	sData := make([]byte, 1000)
	rData := make([]byte, 1000)
	br := test.NewBridge()
	loggerFactory := logging.NewDefaultLoggerFactory()

	a0, a1, err := createNewAssociationPair(br)
	if !assert.Nil(t, err, "failed to create associations") {
		assert.FailNow(t, "failed due to earlier error")
	}

	dc0, err := Dial(a0, 100, &Config{
		Label:         "data",
		LoggerFactory: loggerFactory,
	})
	assert.Nil(t, err, "Dial() should succeed")
	br.Process()

	dc1, err := Accept(a1, &Config{
		LoggerFactory: loggerFactory,
	})
	assert.Nil(t, err, "Accept() should succeed")

	for dc0.BufferedAmount() > 0 {
		br.Process()
	}

	dc0.SetBufferedAmountLowThreshold(1500)
	assert.Equal(t, uint64(1500), dc0.BufferedAmountLowThreshold(), "incorrect bufferedAmountLowThreshold")
	dc0.OnBufferedAmountLow(func() {
		nCbs++
	})

	// Write 10 1000-byte packets (total 10,000 bytes)
	for i := 0; i < 10; i++ {
		var n int
		n, err = dc0.Write(sData)
		assert.Nil(t, err, "Write() should succeed")
		assert.Equal(t, len(sData), n, "data length should match")
		assert.Equal(t, uint64(len(sData)*(i+1)), dc0.BufferedAmount(), "incorrect bufferedAmount")
	}

	go func() {
		for {
			n, err := dc1.Read(rData)
			if err != nil {
				break
			}
			assert.Equal(t, n, len(rData), "received length should match")
		}
	}()

	since := time.Now()
	for {
		br.Tick()
		time.Sleep(10 * time.Millisecond)
		if time.Since(since).Seconds() > 0.5 {
			break
		}
	}

	//nolint:errcheck,gosec
	dc0.Close()
	//nolint:errcheck,gosec
	dc1.Close()

	br.Process()

	assert.True(t, nCbs > 0, "should make at least one callback")

	closeAssociationPair(br, a0, a1)
}
