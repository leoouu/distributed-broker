package storage

import (
	"errors"
	"sync"
	"testing"
)

func TestTopic_AppendAndRead(t *testing.T) {
	topic := NewTopic("telemetry")

	off1, err := topic.Append([]byte("msg-1"))
	if err != nil || off1 != 0 {
		t.Fatalf("expected offset 0, got %d (err: %v)", off1, err)
	}

	off2, err := topic.Append([]byte("msg-2"))
	if err != nil || off2 != 1 {
		t.Fatalf("expected offset 1, got %d (err: %v)", off2, err)
	}

	records, err := topic.ReadFrom(0, 10)
	if err != nil {
		t.Fatalf("failed to read records: %v", err)
	}

	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}
	if string(records[0].Payload) != "msg-1" || string(records[1].Payload) != "msg-2" {
		t.Errorf("payload mismatch in batch: %+v", records)
	}
}

func TestEngine_PollAndCommitOffset(t *testing.T) {
	engine := NewEngine()
	topicName := "orders"
	groupID := "payment-worker"

	topic, err := engine.GetOrCreateTopic(topicName)
	if err != nil {
		t.Fatalf("unexpected error creating topic: %v", err)
	}

	topic.Append([]byte("order-1001"))
	topic.Append([]byte("order-1002"))

	// Primeiro Poll: deve trazer order-1001 e order-1002
	batch, err := engine.Poll(groupID, topicName, 5)
	if err != nil {
		t.Fatalf("poll failed: %v", err)
	}
	if len(batch) != 2 {
		t.Fatalf("expected 2 items, got %d", len(batch))
	}

	// Comita o offset 2 (ou seja, consumiu até o 1)
	err = engine.CommitOffset(groupID, topicName, 2)
	if err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	// Segundo Poll: não deve ter novas mensagens
	_, err = engine.Poll(groupID, topicName, 5)
	if !errors.Is(err, ErrNoNewMessages) {
		t.Fatalf("expected ErrNoNewMessages, got: %v", err)
	}
}

func TestTopic_ConcurrentAppends(t *testing.T) {
	topic := NewTopic("concurrency-test")
	const numGoroutines = 50
	const msgsPerGoroutine = 20

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < msgsPerGoroutine; j++ {
				_, _ = topic.Append([]byte("data"))
			}
		}()
	}

	wg.Wait()

	expectedTotal := uint64(numGoroutines * msgsPerGoroutine)
	if topic.LatestOffset() != expectedTotal {
		t.Fatalf("expected next offset to be %d, got %d", expectedTotal, topic.LatestOffset())
	}
}
