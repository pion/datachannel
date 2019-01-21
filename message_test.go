package datachannel

import (
	"testing"

	"github.com/pkg/errors"
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
	if err != nil {
		t.Errorf("Failed to marshal: %v", err)
		return
	}

	result := []byte{
		0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x03, 0x00, 0x03, 0x66, 0x6f, 0x6f, 0x62, 0x61, 0x72,
	}

	if len(rawMsg) != len(result) {
		t.Errorf("%q != %q", rawMsg, result)
		return
	}

	for i, v := range rawMsg {
		if v != result[i] {
			t.Errorf("%q != %q", rawMsg, result)
			break
		}
	}
}

func TestChannelAckMarshal(t *testing.T) {
	msg := channelAck{}
	rawMsg, err := msg.Marshal()
	if err != nil {
		t.Errorf("Failed to marshal: %v", err)
		return
	}
	result := []byte{0x02, 0x00, 0x00, 0x00}

	if len(rawMsg) != len(result) {
		t.Errorf("%q != %q", rawMsg, result)
		return
	}

	for i, v := range rawMsg {
		if v != result[i] {
			t.Errorf("%q != %q", rawMsg, result)
			break
		}
	}
}

func TestChannelOpenUnmarshal(t *testing.T) {
	rawMsg := []byte{
		0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x03, 0x00, 0x03, 0x66, 0x6f, 0x6f, 0x62, 0x61, 0x72,
	}

	msgUncast, err := parse(rawMsg)

	msg, ok := msgUncast.(*channelOpen)
	if !ok {
		t.Error(errors.Errorf("Failed to cast to ChannelOpen"))
	}

	if err != nil {
		t.Fatal(errors.Wrap(err, "Unmarshal failed, ChannelOpen"))
	}
	if msg.ChannelType != ChannelTypeReliable {
		t.Fatal(errors.Errorf("ChannelType should be 0"))
	}
	if msg.Priority != 0 {
		t.Fatal(errors.Errorf("Priority should be 0"))
	}
	if msg.ReliabilityParameter != 0 {
		t.Fatal(errors.Errorf("ReliabilityParameter should be 0"))
	}
	if string(msg.Label) != "foo" {
		t.Fatal(errors.Errorf("msg Label should be 'foo'"))
	}
	if string(msg.Protocol) != "bar" {
		t.Fatal(errors.Errorf("msg protocol should be 'bar'"))
	}
}

func TestChannelAckUnmarshal(t *testing.T) {
	rawMsg := []byte{0x02}
	msgUncast, err := parse(rawMsg)
	if err != nil {
		t.Errorf("Failed to parse: %v", err)
		return
	}

	_, ok := msgUncast.(*channelAck)
	if !ok {
		t.Error(errors.Errorf("Failed to cast to ChannelAck"))
	}
}
