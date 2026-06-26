# Orders Service

Ponto de entrada do sistema de pedidos. Recebe requisicoes HTTP, cria pedidos com status `PENDING` e inicia a saga publicando eventos via **Transactional Outbox**. Consome eventos de confirmacao e cancelamento dos demais servicos.

## Stack

- **Go 1.25** (Gin HTTP framework)
- **MySQL** — persistencia de pedidos e outbox
- **Redis** — idempotencia via `Idempotency-Key` header
- **Kafka** (`segmentio/kafka-go`) — mensageria
- **OpenTelemetry** — tracing distribuido
- **Zap** — logging estruturado

## Arquitetura

Arquitetura em camadas: `handler → service → repository`, com `consumer` e `relay` como componentes de background.

```
orders-service/
├── cmd/api/main.go              # Entrypoint
├── internal/
│   ├── config/                  # Configuracao via Viper (config.yaml + env vars)
│   ├── handler/                 # HTTP handlers (Gin)
│   ├── service/                 # Logica de negocio
│   ├── repository/              # Acesso a dados (MySQL)
│   ├── domain/                  # Modelos de dominio e enums
│   ├── consumer/                # Kafka consumers (saga)
│   │   ├── payments.go          # Consome payments.authorized → ConfirmOrder
│   │   ├── inventory_insufficient_stock.go  # Consome inventory.insufficient-stock → CancelOrder
│   │   └── inventory_released.go            # Consome inventory.released → CancelOrder
│   ├── relay/                   # Outbox relay (polling a cada 5s, batch 10)
│   ├── middleware/              # Idempotencia (Redis) e error handler
│   └── logger/                  # Logger context-aware (Zap + OTel)
├── INIT.sql                     # Schema do banco
├── Dockerfile
└── go.mod
```

## Participacao na Saga

| Topico | Direcao | Acao |
|--------|---------|------|
| `orders.created` | Produz | Novo pedido criado — inicia a saga |
| `orders.confirmed` | Produz | Pedido confirmado — notifica Inventory |
| `payments.authorized` | Consome | Confirma o pedido (`CONFIRMED`) |
| `inventory.insufficient-stock` | Consome | Cancela o pedido (`CANCELED`) |
| `inventory.released` | Consome | Cancela o pedido (`CANCELED`) |

## API

| Metodo | Rota | Descricao |
|--------|------|-----------|
| `GET` | `/health` | Health check |
| `GET` | `/v1/orders` | Lista todos os pedidos |
| `POST` | `/v1/orders` | Cria um pedido (requer header `Idempotency-Key`) |

### Criar Pedido

```bash
curl -X POST http://localhost:8081/v1/orders \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: $(uuidgen)" \
  -d '{
    "userId": "550e8400-e29b-41d4-a716-446655440000",
    "productId": 1,
    "quantity": 2,
    "paymentType": "PIX"
  }'
```

## Executando

### Pre-requisitos

- Go 1.25+
- Infraestrutura via `docker compose up -d` no root do repositorio (MySQL porta 3307, Redis porta 6379, Kafka porta 29092)

### Build e execucao

```bash
go build -o orders-service ./cmd/api    # Build
go run cmd/api/main.go                  # Executa o servico
```

### Testes

```bash
go test ./...                                          # Todos os testes
go test ./internal/service/... -run TestCreateOrder     # Teste especifico
```

## Banco de Dados (MySQL, porta 3307)

Schema em `INIT.sql`. Duas tabelas:

- **orders** — pedidos com status (`PENDING`, `CONFIRMED`, `CANCELED`) e constraint `UNIQUE(idempotency_key)`
- **outbox** — eventos pendentes para publicacao via relay (`PENDING → PROCESSING → SENT` ou `→ FAILED → DEAD_LETTER`)

## Configuracao

Configuracao via `config.yaml` com override por variaveis de ambiente (Viper).

| Variavel | Default | Descricao |
|----------|---------|-----------|
| `SERVER_PORT` | `:8081` | Porta do servidor HTTP |
| `MYSQL_DSN` | `root:root@tcp(localhost:3307)/orders?parseTime=true` | DSN do MySQL |
| `REDIS_ADDR` | `localhost:6379` | Endereco do Redis |
| `REDIS_PASS` | (vazio) | Senha do Redis |
| `REDIS_DB` | `0` | Database do Redis |
| `KAFKA_BROKERS` | `localhost:29092` | Brokers Kafka |
| `KAFKA_ORDERS_CREATED_TOPIC` | `orders.created` | Topico de pedidos criados |
| `KAFKA_ORDERS_CONFIRMED_TOPIC` | `orders.confirmed` | Topico de pedidos confirmados |
| `KAFKA_INVENTORY_TOPIC` | `inventory.insufficient-stock` | Topico de estoque insuficiente |
| `KAFKA_INVENTORY_RELEASED_TOPIC` | `inventory.released` | Topico de estoque liberado |
| `KAFKA_PAYMENTS_TOPIC` | `payments.authorized` | Topico de pagamentos autorizados |
| `OUTBOX_BATCH_SIZE` | `10` | Tamanho do batch do relay |
| `OTEL_EXPORTER_ENDPOINT` | `localhost:4317` | Endpoint gRPC do OTel Collector |
