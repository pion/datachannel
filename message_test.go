// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package datachannel

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestChannelOpenMarshal(t *testing.T) {
	msg := channelOpen{
		ChannelType:          ChannelTypeReliable,
		Priority:             0,
		ReliabilityParameter: 0,

		Label:    []byte("foo"),
		Protocol: []byte("bar"),
	}

	rawMsg, err := msg.Marshal()
	assert.NoError(t, err)

	result := []byte{
		0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x03, 0x00, 0x03, 0x66, 0x6f, 0x6f, 0x62, 0x61, 0x72,
	}

	assert.Equal(t, len(result), len(rawMsg))

	for i, v := range rawMsg {
		assert.Equal(t, result[i], v)
	}
}

func TestChannelAckMarshal(t *testing.T) {
	msg := channelAck{}
	rawMsg, err := msg.Marshal()
	assert.NoError(t, err)

	result := []byte{0x02, 0x00, 0x00, 0x00}
	assert.Equal(t, len(result), len(rawMsg))

	for i, v := range rawMsg {
		assert.Equal(t, result[i], v)
	}
}

func TestChannelOpenUnmarshal(t *testing.T) {
	rawMsg := []byte{
		0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x03, 0x00, 0x03, 0x66, 0x6f, 0x6f, 0x62, 0x61, 0x72,
	}

	msgUncast, err := parse(rawMsg)

	msg, ok := msgUncast.(*channelOpen)
	assert.True(t, ok, "Failed to cast to ChannelOpen")

	assert.NoError(t, err, "Unmarshal failed, ChannelOpen")
	assert.Equal(t, msg.ChannelType, ChannelTypeReliable, "ChannelType should be 0")
	assert.Equal(t, msg.Priority, uint16(0), "Priority should be 0")
	assert.Equal(t, msg.ReliabilityParameter, uint32(0), "ReliabilityParameter should be 0")
	assert.Equal(t, msg.Label, []uint8("foo"), "msg Label should be 'foo'")
	assert.Equal(t, msg.Protocol, []uint8("bar"), "msg protocol should be 'bar'")
}

func TestChannelAckUnmarshal(t *testing.T) {
	rawMsg := []byte{0x02}
	msgUncast, err := parse(rawMsg)
	assert.NoError(t, err)

	_, ok := msgUncast.(*channelAck)
	assert.True(t, ok, "Failed to cast to ChannelAck")
}

func TestChannelString(t *testing.T) {
	channelString := channelOpen{
		ChannelType:          ChannelTypeReliable,
		Priority:             0,
		ReliabilityParameter: 0,

		Label:    []byte("foo"),
		Protocol: []byte("bar"),
	}.String()

	assert.Equal(
		t,
		channelString,
		"Open ChannelType(ReliableOrdered) Priority(0) ReliabilityParameter(0) Label(foo) Protocol(bar)",
	)
	assert.Equal(t, channelAck{}.String(), "ACK")
}
