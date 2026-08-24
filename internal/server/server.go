package server

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"distributed-broker/internal/protocol"
	"distributed-broker/internal/storage"
)

// Config contém parâmetros de inicialização e timeouts do servidor.
type Config struct {
	Addr         string
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

// Server orquestra o listener TCP, ciclo de vida das conexões e engine de armazenamento.
type Server struct {
	cfg      Config
	engine   *storage.Engine
	listener net.Listener

	mu       sync.Mutex
	conns    map[net.Conn]struct{}
	wg       sync.WaitGroup
	shutdown chan struct{}
}

// NewServer inicializa uma instância do servidor TCP.
func NewServer(cfg Config, engine *storage.Engine) *Server {
	if cfg.ReadTimeout == 0 {
		cfg.ReadTimeout = 30 * time.Second
	}
	if cfg.WriteTimeout == 0 {
		cfg.WriteTimeout = 5 * time.Second
	}

	return &Server{
		cfg:      cfg,
		engine:   engine,
		conns:    make(map[net.Conn]struct{}),
		shutdown: make(chan struct{}),
	}
}

// Start inicializa o listener TCP e começa a aceitar conexões.
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return fmt.Errorf("failed to bind listener on %s: %w", s.cfg.Addr, err)
	}
	s.listener = ln

	s.wg.Add(1)
	go s.acceptLoop()

	return nil
}

// Stop realiza o graceful shutdown do servidor, fechando listeners e conexões ativas.
func (s *Server) Stop(ctx context.Context) error {
	close(s.shutdown)

	var err error
	if s.listener != nil {
		err = s.listener.Close()
	}

	// Fecha conexões ativas existentes
	s.mu.Lock()
	for c := range s.conns {
		_ = c.Close()
	}
	s.mu.Unlock()

	// Aguarda goroutines de conexões finalizarem respeitando o contexto
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Server) acceptLoop() {
	defer s.wg.Done()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.shutdown:
				return // shutdown esperado
			default:
				log.Printf("[server] error accepting connection: %v", err)
				continue
			}
		}

		s.trackConn(conn, true)
		s.wg.Add(1)
		go func(c net.Conn) {
			defer func() {
				s.trackConn(c, false)
				_ = c.Close()
				s.wg.Done()
			}()
			s.handleConnection(c)
		}(conn)
	}
}

func (s *Server) trackConn(c net.Conn, add bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if add {
		s.conns[c] = struct{}{}
	} else {
		delete(s.conns, c)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	for {
		select {
		case <-s.shutdown:
			return
		default:
		}

		// Configura deadline de leitura defensivo
		_ = conn.SetReadDeadline(time.Now().Add(s.cfg.ReadTimeout))
		msg, err := protocol.Decode(conn)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
				return // cliente desconectou normalmente
			}
			s.writeError(conn, "malformed or invalid packet")
			return
		}

		// Processa o comando recebido
		if err := s.dispatch(conn, msg); err != nil {
			return
		}
	}
}

func (s *Server) dispatch(conn net.Conn, msg *protocol.Message) error {
	switch msg.OpCode {
	case protocol.OpPublish:
		return s.handlePublish(conn, msg)
	case protocol.OpPoll:
		return s.handlePoll(conn, msg)
	case protocol.OpAck:
		return s.handleAck(conn, msg)
	default:
		return s.writeError(conn, "unsupported opcode")
	}
}

// handlePublish processa o salvamento do payload e retorna o uint64 offset gerado.
func (s *Server) handlePublish(conn net.Conn, msg *protocol.Message) error {
	topic, err := s.engine.GetOrCreateTopic(msg.Topic)
	if err != nil {
		return s.writeError(conn, err.Error())
	}

	offset, err := topic.Append(msg.Payload)
	if err != nil {
		return s.writeError(conn, err.Error())
	}

	// Resposta: Payload contém o uint64 do Offset em BigEndian (8 bytes)
	respPayload := make([]byte, 8)
	binary.BigEndian.PutUint64(respPayload, offset)

	resp := &protocol.Message{
		OpCode:  protocol.OpResponse,
		Topic:   msg.Topic,
		Payload: respPayload,
	}

	_ = conn.SetWriteDeadline(time.Now().Add(s.cfg.WriteTimeout))
	return resp.Encode(conn)
}

// handlePoll processa a busca de mensagens para um groupID informado no início do payload.
// Formato esperado no Payload de Poll: [GroupIDLen (1 byte)] + [GroupID (string)] + [MaxBatch (2 bytes uint16)]
func (s *Server) handlePoll(conn net.Conn, msg *protocol.Message) error {
	if len(msg.Payload) < 3 {
		return s.writeError(conn, "invalid poll payload format")
	}

	groupLen := int(msg.Payload[0])
	if len(msg.Payload) < 1+groupLen+2 {
		return s.writeError(conn, "poll payload truncated")
	}

	groupID := string(msg.Payload[1 : 1+groupLen])
	maxBatch := int(binary.BigEndian.Uint16(msg.Payload[1+groupLen : 1+groupLen+2]))
	if maxBatch <= 0 {
		maxBatch = 10
	}

	records, err := s.engine.Poll(groupID, msg.Topic, maxBatch)
	if err != nil {
		if errors.Is(err, storage.ErrNoNewMessages) {
			// Retorna resposta vazia (0 registros)
			resp := &protocol.Message{
				OpCode:  protocol.OpResponse,
				Topic:   msg.Topic,
				Payload: []byte{0x00, 0x00}, // uint16 count = 0
			}
			_ = conn.SetWriteDeadline(time.Now().Add(s.cfg.WriteTimeout))
			return resp.Encode(conn)
		}
		return s.writeError(conn, err.Error())
	}

	// Serializa o lote de mensagens:
	// [Count (uint16)] + para cada item: [Offset (8 bytes)] + [PayloadLen (4 bytes)] + [Payload bytes]
	buf := make([]byte, 2)
	binary.BigEndian.PutUint16(buf[0:2], uint16(len(records)))

	for _, rec := range records {
		recHeader := make([]byte, 12)
		binary.BigEndian.PutUint64(recHeader[0:8], rec.Offset)
		binary.BigEndian.PutUint32(recHeader[8:12], uint32(len(rec.Payload)))
		buf = append(buf, recHeader...)
		buf = append(buf, rec.Payload...)
	}

	resp := &protocol.Message{
		OpCode:  protocol.OpResponse,
		Topic:   msg.Topic,
		Payload: buf,
	}

	_ = conn.SetWriteDeadline(time.Now().Add(s.cfg.WriteTimeout))
	return resp.Encode(conn)
}

// handleAck confirma o offset processado para um dado groupID.
// Formato esperado no Payload de Ack: [GroupIDLen (1 byte)] + [GroupID] + [Offset (8 bytes uint64)]
func (s *Server) handleAck(conn net.Conn, msg *protocol.Message) error {
	if len(msg.Payload) < 9 {
		return s.writeError(conn, "invalid ack payload format")
	}

	groupLen := int(msg.Payload[0])
	if len(msg.Payload) < 1+groupLen+8 {
		return s.writeError(conn, "ack payload truncated")
	}

	groupID := string(msg.Payload[1 : 1+groupLen])
	commitOffset := binary.BigEndian.Uint64(msg.Payload[1+groupLen : 1+groupLen+8])

	if err := s.engine.CommitOffset(groupID, msg.Topic, commitOffset); err != nil {
		return s.writeError(conn, err.Error())
	}

	resp := &protocol.Message{
		OpCode:  protocol.OpResponse,
		Topic:   msg.Topic,
		Payload: []byte("OK"),
	}

	_ = conn.SetWriteDeadline(time.Now().Add(s.cfg.WriteTimeout))
	return resp.Encode(conn)
}

func (s *Server) writeError(conn net.Conn, reason string) error {
	resp := &protocol.Message{
		OpCode:  protocol.OpError,
		Payload: []byte(reason),
	}
	_ = conn.SetWriteDeadline(time.Now().Add(s.cfg.WriteTimeout))
	return resp.Encode(conn)
}
