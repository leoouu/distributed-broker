package protocol

import (
	"bytes"
	"errors"
	"testing"
)

func TestEncodeDecode_Success(t *testing.T) {
	original := &Message{
		OpCode:  OpPublish,
		Topic:   "orders.created",
		Payload: []byte(`{"id": 42, "item": "book"}`),
	}

	var buf bytes.Buffer
	if err := original.Encode(&buf); err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := Decode(&buf)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.OpCode != original.OpCode {
		t.Errorf("expected OpCode %v, got %v", original.OpCode, decoded.OpCode)
	}
	if decoded.Topic != original.Topic {
		t.Errorf("expected Topic %q, got %q", original.Topic, decoded.Topic)
	}
	if !bytes.Equal(decoded.Payload, original.Payload) {
		t.Errorf("expected Payload %q, got %q", original.Payload, decoded.Payload)
	}
}

func TestDecode_InvalidMagic(t *testing.T) {
	corrupted := []byte{0xAA, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	buf := bytes.NewReader(corrupted)

	_, err := Decode(buf)
	if !errors.Is(err, ErrInvalidMagic) {
		t.Fatalf("expected ErrInvalidMagic, got %v", err)
	}
}