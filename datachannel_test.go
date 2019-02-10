package datachannel

import (
	"fmt"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/pions/sctp"
	"github.com/stretchr/testify/assert"
)

////////////////////////////////////////////////////////////////////////////////
// testConn, connBridge for emulating dtls.Conn

type testConn struct {
	br     *connBridge
	id     int
	readCh chan []byte
}

func (conn *testConn) Read(b []byte) (int, error) {
	if data, ok := <-conn.readCh; ok {
		n := copy(b, data)
		return n, nil
	}
	return 0, fmt.Errorf("testConn closed")
}

func (conn *testConn) Write(b []byte) (int, error) {
	n := len(b)
	conn.br.push(b, conn.id)
	return n, nil
}

func (conn *testConn) Close() error {
	close(conn.readCh)
	return nil
}

// Unused
func (conn *testConn) LocalAddr() net.Addr                { return nil }
func (conn *testConn) RemoteAddr() net.Addr               { return nil }
func (conn *testConn) SetDeadline(t time.Time) error      { return nil }
func (conn *testConn) SetReadDeadline(t time.Time) error  { return nil }
func (conn *testConn) SetWriteDeadline(t time.Time) error { return nil }

type connBridge struct {
	mutex sync.RWMutex
	conn0 *testConn
	conn1 *testConn

	queue0to1 [][]byte
	queue1to0 [][]byte
}

func inverse(s [][]byte) error {
	if len(s) < 2 {
		return fmt.Errorf("inverse requires more than one item in the array")
	}

	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
	return nil
}

// drop n packets from the slice starting from offset
func drop(s [][]byte, offset, n int) [][]byte {
	if offset+n > len(s) {
		n = len(s) - offset
	}
	return append(s[:offset], s[offset+n:]...)
}

func newConnBridge() *connBridge {
	br := &connBridge{
		queue0to1: make([][]byte, 0),
		queue1to0: make([][]byte, 0),
	}

	br.conn0 = &testConn{
		br:     br,
		id:     0,
		readCh: make(chan []byte),
	}
	br.conn1 = &testConn{
		br:     br,
		id:     1,
		readCh: make(chan []byte),
	}

	return br
}

func (br *connBridge) push(d []byte, fromID int) {
	br.mutex.Lock()
	defer br.mutex.Unlock()

	if fromID == 0 {
		br.queue0to1 = append(br.queue0to1, d)
	} else {
		br.queue1to0 = append(br.queue1to0, d)
	}
}

func (br *connBridge) reorder(fromID int) error {
	br.mutex.Lock()
	defer br.mutex.Unlock()

	var err error

	if fromID == 0 {
		err = inverse(br.queue0to1)
	} else {
		err = inverse(br.queue1to0)
	}

	return err
}

//nolint:unparam
func (br *connBridge) drop(fromID, offset, n int) {
	br.mutex.Lock()
	defer br.mutex.Unlock()

	if fromID == 0 {
		br.queue0to1 = drop(br.queue0to1, offset, n)
	} else {
		br.queue1to0 = drop(br.queue1to0, offset, n)
	}
}

func (br *connBridge) tick() int {
	br.mutex.Lock()
	defer br.mutex.Unlock()

	var n int

	if len(br.queue0to1) > 0 {
		select {
		case br.conn1.readCh <- br.queue0to1[0]:
			n++
			br.queue0to1 = br.queue0to1[1:]
		default:
		}
	}

	if len(br.queue1to0) > 0 {
		select {
		case br.conn0.readCh <- br.queue1to0[0]:
			n++
			br.queue1to0 = br.queue1to0[1:]
		default:
		}
	}

	return n
}

// Repeat tick() call until no more outstanding inflight packet
func (br *connBridge) process() {
	for {
		time.Sleep(10 * time.Millisecond)
		n := br.tick()
		if n == 0 {
			break
		}
	}
}

func createNewAssociationPair(br *connBridge) (*sctp.Association, *sctp.Association, error) {
	var a0, a1 *sctp.Association
	var err0, err1 error

	handshake0Ch := make(chan bool)
	handshake1Ch := make(chan bool)

	go func() {
		a0, err0 = sctp.Client(br.conn0)
		handshake0Ch <- true
	}()
	go func() {
		a1, err1 = sctp.Client(br.conn1)
		handshake1Ch <- true
	}()

	a0handshakeDone := false
	a1handshakeDone := false
loop1:
	for i := 0; i < 100; i++ {
		time.Sleep(10 * time.Millisecond)
		br.tick()

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

func closeAssociationPair(br *connBridge, a0, a1 *sctp.Association) {
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
		br.tick()

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
	const msg1 = "ABC"
	const msg2 = "DEF"
	br := newConnBridge()

	a0, a1, err := createNewAssociationPair(br)
	if !assert.Nil(t, err, "failed to create associations") {
		assert.FailNow(t, "failed due to earlier error")
	}

	cfg := &Config{
		ChannelType:          channelType,
		ReliabilityParameter: 0,
		Label:                "data",
	}

	dc0, err := Dial(a0, 100, cfg)
	assert.Nil(t, err, "Dial() should succeed")
	br.process()

	dc1, err := Accept(a1)
	assert.Nil(t, err, "Accept() should succeed")
	br.process()

	assert.True(t, reflect.DeepEqual(dc0.Config, *cfg), "local config should match")
	assert.True(t, reflect.DeepEqual(dc1.Config, *cfg), "remote config should match")

	var n int

	n, err = dc0.WriteDataChannel([]byte(msg1), true)
	assert.Nil(t, err, "Read() should succeed")
	assert.Equal(t, len(msg1), n, "data length should match")

	n, err = dc0.WriteDataChannel([]byte(msg2), true)
	assert.Nil(t, err, "Read() should succeed")
	assert.Equal(t, len(msg2), n, "data length should match")

	br.drop(0, 0, 1) // drop the first packet on the wire
	br.process()

	buf := make([]byte, 16)
	var isString bool

	n, isString, err = dc1.ReadDataChannel(buf)
	assert.Nil(t, err, "Read() should succeed")
	assert.True(t, isString, "should return isString being true")
	assert.Equal(t, string(buf[:n]), msg2, "data should match")

	//nolint:errcheck,gosec
	dc0.Close()
	//nolint:errcheck,gosec
	dc1.Close()
	br.process()

	closeAssociationPair(br, a0, a1)
}

func prUnorderedTest(t *testing.T, channelType ChannelType) {
	const msg1 = "ABC"
	const msg2 = "DEF"
	const msg3 = "GHI"
	br := newConnBridge()

	a0, a1, err := createNewAssociationPair(br)
	if !assert.Nil(t, err, "failed to create associations") {
		assert.FailNow(t, "failed due to earlier error")
	}

	cfg := &Config{
		ChannelType:          channelType,
		ReliabilityParameter: 0,
		Label:                "data",
	}

	dc0, err := Dial(a0, 100, cfg)
	assert.Nil(t, err, "Dial() should succeed")
	br.process()

	dc1, err := Accept(a1)
	assert.Nil(t, err, "Accept() should succeed")
	br.process()

	assert.True(t, reflect.DeepEqual(dc0.Config, *cfg), "local config should match")
	assert.True(t, reflect.DeepEqual(dc1.Config, *cfg), "remote config should match")

	var n int

	n, err = dc0.WriteDataChannel([]byte(msg1), true)
	assert.Nil(t, err, "Read() should succeed")
	assert.Equal(t, len(msg1), n, "data length should match")

	n, err = dc0.WriteDataChannel([]byte(msg2), true)
	assert.Nil(t, err, "Read() should succeed")
	assert.Equal(t, len(msg2), n, "data length should match")

	n, err = dc0.WriteDataChannel([]byte(msg3), true)
	assert.Nil(t, err, "Read() should succeed")
	assert.Equal(t, len(msg3), n, "data length should match")

	br.drop(0, 0, 1)    // drop the first packet on the wire
	err = br.reorder(0) // reorder the rest of the packet
	assert.Nil(t, err, "reorder failed")
	br.process()

	buf := make([]byte, 16)
	var isString bool

	n, isString, err = dc1.ReadDataChannel(buf)
	assert.Nil(t, err, "Read() should succeed")
	assert.True(t, isString, "should return isString being true")
	assert.Equal(t, string(buf[:n]), msg3, "data should match")

	n, isString, err = dc1.ReadDataChannel(buf)
	assert.Nil(t, err, "Read() should succeed")
	assert.True(t, isString, "should return isString being true")
	assert.Equal(t, string(buf[:n]), msg2, "data should match")

	//nolint:errcheck,gosec
	dc0.Close()
	//nolint:errcheck,gosec
	dc1.Close()
	br.process()

	closeAssociationPair(br, a0, a1)
}

func TestDataChannel(t *testing.T) {
	t.Run("ChannelTypeReliable", func(t *testing.T) {
		const msg1 = "ABC"
		const msg2 = "DEF"
		br := newConnBridge()

		a0, a1, err := createNewAssociationPair(br)
		if !assert.Nil(t, err, "failed to create associations") {
			assert.FailNow(t, "failed due to earlier error")
		}

		cfg := &Config{
			ChannelType:          ChannelTypeReliable,
			ReliabilityParameter: 123,
			Label:                "data",
		}

		dc0, err := Dial(a0, 100, cfg)
		assert.Nil(t, err, "Dial() should succeed")
		br.process()

		dc1, err := Accept(a1)
		assert.Nil(t, err, "Accept() should succeed")
		br.process()

		assert.True(t, reflect.DeepEqual(dc0.Config, *cfg), "local config should match")
		assert.True(t, reflect.DeepEqual(dc1.Config, *cfg), "remote config should match")

		var n int

		n, err = dc0.Write([]byte(msg1))
		assert.Nil(t, err, "Write() should succeed")
		assert.Equal(t, len(msg1), n, "data length should match")

		n, err = dc0.Write([]byte(msg2))
		assert.Nil(t, err, "Write() should succeed")
		assert.Equal(t, len(msg2), n, "data length should match")

		err = br.reorder(0) // reordering on the wire
		assert.Nil(t, err, "reorder failed")
		br.process()

		buf := make([]byte, 16)

		n, err = dc1.Read(buf)
		assert.Nil(t, err, "Read() should succeed")
		assert.Equal(t, string(buf[:n]), msg1, "data should match")

		n, err = dc1.Read(buf)
		assert.Nil(t, err, "Read() should succeed")
		assert.Equal(t, string(buf[:n]), msg2, "data should match")

		//nolint:errcheck,gosec
		dc0.Close()
		//nolint:errcheck,gosec
		dc1.Close()
		br.process()

		closeAssociationPair(br, a0, a1)
	})

	t.Run("ChannelTypeReliableUnordered", func(t *testing.T) {
		const msg1 = "ABC"
		const msg2 = "DEF"
		br := newConnBridge()

		a0, a1, err := createNewAssociationPair(br)
		if !assert.Nil(t, err, "failed to create associations") {
			assert.FailNow(t, "failed due to earlier error")
		}

		cfg := &Config{
			ChannelType:          ChannelTypeReliableUnordered,
			ReliabilityParameter: 123,
			Label:                "data",
		}

		dc0, err := Dial(a0, 100, cfg)
		assert.Nil(t, err, "Dial() should succeed")
		br.process()

		dc1, err := Accept(a1)
		assert.Nil(t, err, "Accept() should succeed")
		br.process()

		assert.True(t, reflect.DeepEqual(dc0.Config, *cfg), "local config should match")
		assert.True(t, reflect.DeepEqual(dc1.Config, *cfg), "remote config should match")

		var n int

		n, err = dc0.WriteDataChannel([]byte(msg1), true)
		assert.Nil(t, err, "Read() should succeed")
		assert.Equal(t, len(msg1), n, "data length should match")

		n, err = dc0.WriteDataChannel([]byte(msg2), true)
		assert.Nil(t, err, "Read() should succeed")
		assert.Equal(t, len(msg2), n, "data length should match")

		err = br.reorder(0) // reordering on the wire
		assert.Nil(t, err, "reorder failed")
		br.process()

		buf := make([]byte, 16)
		var isString bool

		n, isString, err = dc1.ReadDataChannel(buf)
		assert.Nil(t, err, "Read() should succeed")
		assert.True(t, isString, "should return isString being true")
		assert.Equal(t, string(buf[:n]), msg2, "data should match")

		n, isString, err = dc1.ReadDataChannel(buf)
		assert.Nil(t, err, "Read() should succeed")
		assert.True(t, isString, "should return isString being true")
		assert.Equal(t, string(buf[:n]), msg1, "data should match")

		//nolint:errcheck,gosec
		dc0.Close()
		//nolint:errcheck,gosec
		dc1.Close()
		br.process()

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
