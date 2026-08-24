package storage

import (
	"errors"
	"sync"
	"time"
)

var (
	ErrOffsetOutOfRange = errors.New("storage: offset out of range")
	ErrNoNewMessages    = errors.New("storage: no new messages")
)

// Record encapsula o payload da mensagem junto com metadados de auditoria.
type Record struct {
	Offset    uint64
	Timestamp time.Time
	Payload   []byte
}

// Topic gerencia o log de mensagens em memória com concorrência segura.
type Topic struct {
	name    string
	mu      sync.RWMutex
	records []Record
	nextOff uint64
}

// NewTopic inicializa um tópico vazio com offset inicial em 0.
func NewTopic(name string) *Topic {
	return &Topic{
		name:    name,
		records: make([]Record, 0, 1024), // pré-aloca capacidade inicial
		nextOff: 0,
	}
}

// Name retorna o identificador do tópico.
func (t *Topic) Name() string {
	return t.name
}

// Append insere um payload no final do log e retorna o offset atribuído.
// Utiliza Lock exclusivo apenas durante a operação de inserção.
func (t *Topic) Append(payload []byte) (uint64, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	offset := t.nextOff
	record := Record{
		Offset:    offset,
		Timestamp: time.Now().UTC(),
		Payload:   payload,
	}

	t.records = append(t.records, record)
	t.nextOff++

	return offset, nil
}

// ReadFrom lê até maxCount mensagens a partir de startOffset.
// Utiliza RLock permitindo múltiplos consumidores lendo simultaneamente.
func (t *Topic) ReadFrom(startOffset uint64, maxCount int) ([]Record, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	total := uint64(len(t.records))
	if startOffset >= total {
		return nil, ErrNoNewMessages
	}

	end := startOffset + uint64(maxCount)
	if end > total {
		end = total
	}

	// Cria uma fatia defensiva (cópia superficial da slice de records)
	batch := make([]Record, end-startOffset)
	copy(batch, t.records[startOffset:end])

	return batch, nil
}

// LatestOffset retorna o próximo offset que será atribuído.
func (t *Topic) LatestOffset() uint64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.nextOff
}
