package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// MagicByte identifica pacotes válidos do nosso protocolo.
const MagicByte byte = 0xBF

// HeaderSize representa o tamanho fixo do cabeçalho em bytes (1 + 1 + 2 + 4).
const HeaderSize = 8

// Limites defensivos para evitar estouro de memória (DoS / buffer overflow).
const (
	MaxTopicLength   = 256             // 256 B
	MaxPayloadLength = 10 * 1024 * 1024 // 10 MB
)

// OpCode define o tipo de operação solicitada pelo frame.
type OpCode byte

const (
	OpPublish   OpCode = 0x01 // Produtor envia mensagem
	OpSubscribe OpCode = 0x02 // Consumidor se inscreve em tópico
	OpPoll      OpCode = 0x03 // Consumidor requisita mensagens
	OpAck       OpCode = 0x04 // Confirmação de processamento
	OpResponse  OpCode = 0x05 // Resposta genérica do broker
	OpError     OpCode = 0xFF // Resposta de erro
)

// Erros sentinela para tratamento robusto.
var (
	ErrInvalidMagic   = errors.New("protocol: invalid magic byte")
	ErrTopicTooLong   = errors.New("protocol: topic length exceeds maximum allowed")
	ErrPayloadTooLong = errors.New("protocol: payload length exceeds maximum allowed")
	ErrInvalidOpCode  = errors.New("protocol: unknown opcode")
)

// Message representa a estrutura desacoplada em memória de um frame do protocolo.
type Message struct {
	OpCode  OpCode
	Topic   string
	Payload []byte
}

// Encode serializa uma Message no formato binário e escreve no io.Writer.
func (m *Message) Encode(w io.Writer) error {
	topicBytes := []byte(m.Topic)
	topicLen := len(topicBytes)
	payloadLen := len(m.Payload)

	if topicLen > MaxTopicLength {
		return ErrTopicTooLong
	}
	if payloadLen > MaxPayloadLength {
		return ErrPayloadTooLong
	}

	// Aloca buffer exato para o frame completo, minimizando syscalls de escrita
	totalSize := HeaderSize + topicLen + payloadLen
	buf := make([]byte, totalSize)

	buf[0] = MagicByte
	buf[1] = byte(m.OpCode)
	binary.BigEndian.PutUint16(buf[2:4], uint16(topicLen))
	binary.BigEndian.PutUint32(buf[4:8], uint32(payloadLen))

	// Copia tópico e payload para o buffer
	copy(buf[HeaderSize:HeaderSize+topicLen], topicBytes)
	copy(buf[HeaderSize+topicLen:], m.Payload)

	_, err := w.Write(buf)
	return err
}

// Decode lê um frame binário completo a partir de um io.Reader e preenche a Message.
func Decode(r io.Reader) (*Message, error) {
	header := make([]byte, HeaderSize)

	// io.ReadFull garante a leitura dos 8 bytes exatos do cabeçalho
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, err
	}

	if header[0] != MagicByte {
		return nil, fmt.Errorf("%w: received 0x%X", ErrInvalidMagic, header[0])
	}

	opCode := OpCode(header[1])
	topicLen := binary.BigEndian.Uint16(header[2:4])
	payloadLen := binary.BigEndian.Uint32(header[4:8])

	if int(topicLen) > MaxTopicLength {
		return nil, ErrTopicTooLong
	}
	if int(payloadLen) > MaxPayloadLength {
		return nil, ErrPayloadTooLong
	}

	// Leitura do Topic
	topicBytes := make([]byte, topicLen)
	if topicLen > 0 {
		if _, err := io.ReadFull(r, topicBytes); err != nil {
			return nil, fmt.Errorf("failed to read topic: %w", err)
		}
	}

	// Leitura do Payload
	payloadBytes := make([]byte, payloadLen)
	if payloadLen > 0 {
		if _, err := io.ReadFull(r, payloadBytes); err != nil {
			return nil, fmt.Errorf("failed to read payload: %w", err)
		}
	}

	return &Message{
		OpCode:  opCode,
		Topic:   string(topicBytes),
		Payload: payloadBytes,
	}, nil
}