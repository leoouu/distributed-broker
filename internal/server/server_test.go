package server

import (
	"context"
	"encoding/binary"
	"net"
	"testing"
	"time"

	"distributed-broker/internal/protocol"
	"distributed-broker/internal/storage"
)

func TestServer_E2E_PublishAndPoll(t *testing.T) {
	engine := storage.NewEngine()
	cfg := Config{
		Addr: "127.0.0.1:0", // Aloca porta efêmera disponível
	}
	srv := NewServer(cfg, engine)

	if err := srv.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Stop(ctx)
	}()

	addr := srv.listener.Addr().String()

	// 1. Conectar como Produtor e Publicar mensagem
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("failed to connect to server: %v", err)
	}
	defer conn.Close()

	pubMsg := &protocol.Message{
		OpCode:  protocol.OpPublish,
		Topic:   "orders.events",
		Payload: []byte("order_id_500"),
	}
	if err := pubMsg.Encode(conn); err != nil {
		t.Fatalf("failed to send publish message: %v", err)
	}

	pubResp, err := protocol.Decode(conn)
	if err != nil {
		t.Fatalf("failed to decode publish response: %v", err)
	}
	if pubResp.OpCode != protocol.OpResponse {
		t.Fatalf("expected OpResponse, got %v (payload: %s)", pubResp.OpCode, string(pubResp.Payload))
	}
	offset := binary.BigEndian.Uint64(pubResp.Payload)
	if offset != 0 {
		t.Errorf("expected offset 0, got %d", offset)
	}

	// 2. Enviar comando POLL
	// Payload: GroupLen (1 byte) + Group ("worker-1") + BatchSize (2 bytes uint16 = 5)
	groupID := "worker-1"
	pollPayload := make([]byte, 1+len(groupID)+2)
	pollPayload[0] = byte(len(groupID))
	copy(pollPayload[1:1+len(groupID)], []byte(groupID))
	binary.BigEndian.PutUint16(pollPayload[1+len(groupID):], 5)

	pollMsg := &protocol.Message{
		OpCode:  protocol.OpPoll,
		Topic:   "orders.events",
		Payload: pollPayload,
	}
	if err := pollMsg.Encode(conn); err != nil {
		t.Fatalf("failed to send poll message: %v", err)
	}

	pollResp, err := protocol.Decode(conn)
	if err != nil {
		t.Fatalf("failed to decode poll response: %v", err)
	}

	// Deserializa resposta de lote
	count := binary.BigEndian.Uint16(pollResp.Payload[0:2])
	if count != 1 {
		t.Fatalf("expected 1 record from poll, got %d", count)
	}

	recOffset := binary.BigEndian.Uint64(pollResp.Payload[2:10])
	recPayloadLen := binary.BigEndian.Uint32(pollResp.Payload[10:14])
	recPayload := string(pollResp.Payload[14 : 14+recPayloadLen])

	if recOffset != 0 || recPayload != "order_id_500" {
		t.Errorf("record mismatch: offset=%d payload=%s", recOffset, recPayload)
	}
}
