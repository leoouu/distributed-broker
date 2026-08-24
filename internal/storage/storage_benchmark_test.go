package storage

import (
	"strconv"
	"testing"
)

func BenchmarkTopic_Append(b *testing.B) {
	topic := NewTopic("bench-topic")
	payload := []byte(`{"event": "user_signup", "id": 123456, "timestamp": 1710000000}`)

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		_, _ = topic.Append(payload)
	}
}

func BenchmarkTopic_Append_Parallel(b *testing.B) {
	topic := NewTopic("bench-parallel-topic")
	payload := []byte(`{"metric": "cpu_usage", "host": "srv-prod-01", "val": 87.4}`)

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = topic.Append(payload)
		}
	})
}

func BenchmarkTopic_ReadFrom(b *testing.B) {
	topic := NewTopic("bench-read-topic")
	payload := []byte(`{"status": "ok"}`)

	for i := 0; i < 100000; i++ {
		_, _ = topic.Append(payload)
	}

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		start := uint64(0)
		for pb.Next() {
			_, _ = topic.ReadFrom(start, 50)
			start = (start + 50) % 99000
		}
	})
}

func BenchmarkEngine_PollAndCommit(b *testing.B) {
	engine := NewEngine()
	topicName := "engine-bench"
	t, _ := engine.GetOrCreateTopic(topicName)
	payload := []byte("benchmark-payload-data")

	for i := 0; i < 50000; i++ {
		_, _ = t.Append(payload)
	}

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		gid := "group-" + strconv.Itoa(b.N%10)
		for pb.Next() {
			records, _ := engine.Poll(gid, topicName, 20)
			if len(records) > 0 {
				lastOffset := records[len(records)-1].Offset + 1
				_ = engine.CommitOffset(gid, topicName, lastOffset)
			}
		}
	})
}
