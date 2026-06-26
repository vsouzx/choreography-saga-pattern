# Payments Service

Microservico de pagamentos puramente orientado a eventos. Consome `inventory.reserved` do Kafka, avalia regras de pagamento e produz `payments.authorized` ou `payments.denied` via **Transactional Outbox**. Nao expoe endpoints de criacao — opera exclusivamente via Kafka.

## Stack

- **Go 1.25** (Gin HTTP framework)
- **MySQL** — persistencia de pagamentos e outbox
- **Kafka** (`segmentio/kafka-go`) — mensageria
- **OpenTelemetry** — tracing distribuido
- **Zap** — logging estruturado

## Arquitetura

Servico puramente event-driven — sem endpoints REST de usuario (apenas `/health`). Arquitetura em camadas: `consumer → service → repository`, com `relay` em background.

```
payments-service/
├── cmd/api/main.go              # Entrypoint
├── internal/
│   ├── config/                  # Configuracao via Viper (config.yaml + env vars)
│   ├── service/                 # Logica de avaliacao de pagamento
│   ├── repository/              # Acesso a dados (MySQL)
│   ├── domain/                  # Modelos de dominio e enums
│   ├── consumer/                # Kafka consumer (inventory.reserved)
│   ├── relay/                   # Outbox relay (polling a cada 5s, batch 10)
│   └── logger/                  # Logger context-aware (Zap + OTel)
├── INIT.sql                     # Schema do banco
├── Dockerfile
└── go.mod
```

## Participacao na Saga

| Topico | Direcao | Acao |
|--------|---------|------|
| `inventory.reserved` | Consome | Dispara processamento de pagamento |
| `payments.authorized` | Produz | Pagamento autorizado — saga continua |
| `payments.denied` | Produz | Pagamento negado — dispara compensacao |

## Regras de Pagamento

| Tipo | Regra | Resultado |
|------|-------|-----------|
| `PIX` | Sempre | `AUTHORIZED` |
| `CREDIT_CARD` | Valor ≤ 10000 centavos | `AUTHORIZED` |
| `CREDIT_CARD` | Valor > 10000 centavos | `DENIED` |
| `BOLETO` | Sempre | `DENIED` |

## API

| Metodo | Rota | Descricao |
|--------|------|-----------|
| `GET` | `/health` | Health check |

> Nenhum endpoint de criacao — opera exclusivamente via eventos Kafka.

## Executando

### Pre-requisitos

- Go 1.25+
- Infraestrutura via `docker compose up -d` no root do repositorio (MySQL porta 3308, Kafka porta 29092)

### Build e execucao

```bash
go build -o payments-service ./cmd/api    # Build
go run cmd/api/main.go                    # Executa o servico
```

### Testes

```bash
go test ./...                                             # Todos os testes
go test ./internal/service/... -run TestProcessPayment    # Teste especifico
```

## Banco de Dados (MySQL, porta 3308)

Schema em `INIT.sql`. Duas tabelas:

- **payments** — pagamentos com constraint `UNIQUE(order_id)` para idempotencia
- **outbox_events** — eventos pendentes para publicacao via relay (`PENDING → PROCESSING → SENT` ou `→ FAILED → DEAD_LETTER`)

## Configuracao

Configuracao via `config.yaml` com override por variaveis de ambiente (Viper).

| Variavel | Default | Descricao |
|----------|---------|-----------|
| `SERVER_PORT` | `:8083` | Porta do servidor HTTP |
| `MYSQL_DSN` | `root:root@tcp(localhost:3308)/payments?parseTime=true` | DSN do MySQL |
| `KAFKA_BROKERS` | `localhost:29092` | Brokers Kafka |
| `KAFKA_INVENTORY_RESERVED_TOPIC` | `inventory.reserved` | Topico de estoque reservado |
| `KAFKA_PAYMENT_AUTHORIZED_TOPIC` | `payments.authorized` | Topico de pagamento autorizado |
| `KAFKA_PAYMENT_DENIED_TOPIC` | `payments.denied` | Topico de pagamento negado |
| `OUTBOX_BATCH_SIZE` | `10` | Tamanho do batch do relay |
| `OTEL_EXPORTER_ENDPOINT` | `localhost:4317` | Endpoint gRPC do OTel Collector |
