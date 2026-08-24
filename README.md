⚡ Distributed In-Memory Message Broker

Um sistema de mensageria Pub/Sub in-memory de alta performance desenvolvido do zero em Go, projetado para baixa latência, semântica de entrega confiável (At-Least-Once Delivery) e concorrência granular sem frameworks externos ou dependências de terceiros.

🏛️ Arquitetura do Sistema

O sistema adota o modelo cliente-servidor com comunicação baseada em sockets TCP puros e um protocolo binário customizado de tamanho de cabeçalho fixo (Wire Protocol).

      [ Producer ]                   [ Consumer ]
           │                              │
    (TCP)  │ OpPublish             (TCP)  │ OpPoll / OpAck
           ▼                              ▼
   ┌──────────────────────────────────────────────┐
   │             BROKER ENGINE CORE               │
   │  ┌────────────────────────────────────────┐  │
   │  │ Binary Protocol Framer & Dispatcher    │  │
   │  └───────────────────┬────────────────────┘  │
   │                      ▼                       │
   │  ┌────────────────────────────────────────┐  │
   │  │ Granular Topic Registry (RWMutex)      │  │
   │  │ ├─ Topic "orders"   -> [Record Log]    │  │
   │  │ └─ Topic "telemetry"-> [Record Log]    │  │
   │  └───────────────────┬────────────────────┘  │
   │                      ▼                       │
   │  ┌────────────────────────────────────────┐  │
   │  │ Consumer Group Offset Manager          │  │
   │  │ └─ [GroupID, Topic] -> CommittedOffset │  │
   │  └────────────────────────────────────────┘  │
   └──────────────────────────────────────────────┘


🎯 Decisões Técnicas e Tradeoffs de Engenharia

Zero Lock Contention Global (Lock Sharding): Em vez de utilizar uma trava global para todo o broker, a concorrência é isolada no nível do tópico via sync.RWMutex. Leituras concorrentes e publicações em tópicos distintos operam de forma paralela.

Commit Log Imutável e Indexado: Cada partição armazena mensagens sequencialmente em memória. Cada registro recebe um Offset de 64 bits (uint64) monotônico incremental gerado via sincronização controlada.

Semântica At-Least-Once Delivery: O broker rastreia o offset comitado por GroupID. Mensagens entregues em lote por meio de requisições POLL só avançam o cursor de leitura do consumidor após o recebimento explícito do comando ACK.

Zero Overhead de Parsing: Utilização de encoding binário em ordem de rede (Big-Endian), eliminando o custo de serialização/deserialização de formatos baseados em texto (como JSON ou XML) no tráfego de rede do broker.

Graceful Shutdown Defensivo: Tratamento cooperativo com context.Context e captura de sinais do SO (SIGINT/SIGTERM) para encerramento limpo do listener TCP sem abortar conexões ativas.

📦 Wire Protocol Specification

Cada frame TCP trafega com um cabeçalho fixo de 8 bytes seguido pelo corpo de tamanho variável:

+--------+--------+---------------+------------------+---------------------+---------------------+
| Magic  | OpCode | TopicLen(u16) | PayloadLen(u32)  | TopicName (bytes)   | Payload (bytes)     |
| 1 byte | 1 byte | 2 bytes (BE)  | 4 bytes (BE)     | [TopicLen] bytes    | [PayloadLen] bytes  |
+--------+--------+---------------+------------------+---------------------+---------------------+
|<------------------- HEADER FIXO (8 bytes) ----------------------------->|<--- CORPO VARIÁVEL ->|


Campo

Tipo

Tamanho

Descrição

Magic Byte

uint8

1 byte

Identificador de validação (0xBF) para integridade rápida

OpCode

uint8

1 byte

Operação (0x01=PUB, 0x03=POLL, 0x04=ACK, 0x05=RESP, 0xFF=ERR)

Topic Length

uint16

2 bytes

Comprimento do nome do tópico em bytes

Payload Length

uint32

4 bytes

Comprimento do corpo em bytes

Topic Name

[]byte

Variável

Identificador do tópico codificado em UTF-8

Payload

[]byte

Variável

Conteúdo da mensagem ou payload de controle

📂 Estrutura do Projeto

distributed-broker/
├── cmd/
│   ├── broker/                  # Servidor TCP Daemon (main.go)
│   └── cli/                     # Linha de comando para Publish/Poll/Ack (main.go)
├── internal/
│   ├── protocol/                # Framing binário, encoders e decoders de pacotes
│   │   ├── protocol.go
│   │   └── protocol_test.go
│   ├── storage/                 # Engine de armazenamento in-memory, tópicos e offsets
│   │   ├── topic.go
│   │   ├── engine.go
│   │   ├── engine_test.go
│   │   └── storage_benchmark_test.go
│   └── server/                  # Servidor TCP concorrente e connection lifecycle
│       ├── server.go
│       └── server_test.go
├── go.mod
└── README.md


🚀 Como Executar

Pré-requisitos

Go 1.21+ instalado.

1. Iniciar o Broker Server

go run cmd/broker/main.go -addr :9092


2. Publicar Mensagens (Producer)

go run cmd/cli/main.go produce -addr 127.0.0.1:9092 -topic transacoes -msg '{"id": 1001, "amount": 250.0}'
go run cmd/cli/main.go produce -addr 127.0.0.1:9092 -topic transacoes -msg '{"id": 1002, "amount": 1800.0}'


3. Ler Mensagens (Consumer Group Poll)

go run cmd/cli/main.go poll -addr 127.0.0.1:9092 -topic transacoes -group worker-financeiro -max 5


4. Confirmar Processamento (ACK de Offset)

go run cmd/cli/main.go ack -addr 127.0.0.1:9092 -topic transacoes -group worker-financeiro -offset 2


🧪 Testes e Benchmarks

Executar Testes Unitários e de Integração

go test -v ./...


Executar Testes com Detecção de Race Conditions

go test -v -race ./...


Executar Benchmarks de Throughput e Alocação de Memória

go test -bench=. -benchmem .\internal\storage\


📄 Licença

Este projeto está sob a licença MIT.
