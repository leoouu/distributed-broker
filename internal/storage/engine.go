package storage

import (
	"errors"
	"fmt"
	"sync"
)

var (
	ErrTopicNotFound = errors.New("storage: topic not found")
	ErrEmptyTopic    = errors.New("storage: topic name cannot be empty")
)

// Engine coordena os tópicos e os offsets dos grupos de consumidores.
type Engine struct {
	topicsMu sync.RWMutex
	topics   map[string]*Topic

	groupsMu sync.RWMutex
	// offsets mapeia: groupID -> (topicName -> committedOffset)
	offsets map[string]map[string]uint64
}

// NewEngine instancia um novo motor de armazenamento in-memory.
func NewEngine() *Engine {
	return &Engine{
		topics:  make(map[string]*Topic),
		offsets: make(map[string]map[string]uint64),
	}
}

// GetOrCreateTopic busca um tópico existente ou cria um novo de forma thread-safe.
func (e *Engine) GetOrCreateTopic(name string) (*Topic, error) {
	if name == "" {
		return nil, ErrEmptyTopic
	}

	// Tentativa rápida com lock de leitura
	e.topicsMu.RLock()
	t, exists := e.topics[name]
	e.topicsMu.RUnlock()
	if exists {
		return t, nil
	}

	// Adquire lock de escrita para criação
	e.topicsMu.Lock()
	defer e.topicsMu.Unlock()

	// Double-check após adquirir o lock de escrita
	if t, exists = e.topics[name]; exists {
		return t, nil
	}

	t = NewTopic(name)
	e.topics[name] = t
	return t, nil
}

// CommitOffset persiste o progresso de leitura de um grupo consumidor em um tópico.
func (e *Engine) CommitOffset(groupID, topic string, offset uint64) error {
	if groupID == "" || topic == "" {
		return errors.New("storage: groupID and topic must be specified")
	}

	e.groupsMu.Lock()
	defer e.groupsMu.Unlock()

	if _, exists := e.offsets[groupID]; !exists {
		e.offsets[groupID] = make(map[string]uint64)
	}

	e.offsets[groupID][topic] = offset
	return nil
}

// GetOffset recupera o próximo offset a ser lido pelo grupo de consumidores.
// Caso o grupo seja novo, retorna 0 (início do log).
func (e *Engine) GetOffset(groupID, topic string) uint64 {
	e.groupsMu.RLock()
	defer e.groupsMu.RUnlock()

	if topicMap, ok := e.offsets[groupID]; ok {
		if off, found := topicMap[topic]; found {
			return off
		}
	}
	return 0
}

// Poll lê mensagens de um tópico avançando a partir do último offset comitado pelo grupo.
func (e *Engine) Poll(groupID, topicName string, maxCount int) ([]Record, error) {
	topic, err := e.GetOrCreateTopic(topicName)
	if err != nil {
		return nil, fmt.Errorf("failed to get topic: %w", err)
	}

	currentOffset := e.GetOffset(groupID, topicName)
	records, err := topic.ReadFrom(currentOffset, maxCount)
	if err != nil {
		return nil, err
	}

	return records, nil
}
