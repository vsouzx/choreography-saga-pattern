# Choreography Saga Pattern - Event-Driven Microservices

Implementacao de um sistema de pedidos distribuido utilizando o **Saga Pattern (Choreography-based)** com **Transactional Outbox** para garantia de entrega de eventos. O projeto demonstra como coordenar transacoes distribuidas entre microservicos de forma resiliente, idempotente e observavel.

## Desenho arquitetura

<img width="1839" height="725" alt="Image" src="https://github.com/user-attachments/assets/bff3d0ea-cfa3-478b-81eb-f3846e9aa101" />

## Indice

- [Tecnologias](#tecnologias)
- [Servicos](#servicos)
  - [Orders Service](#orders-service)
  - [Inventory Service](#inventory-service)
  - [Payments Service](#payments-service)
- [Fluxos da Saga](#fluxos-da-saga)
  - [Happy Path](#happy-path---pedido-confirmado)
  - [Estoque Insuficiente](#compensacao---estoque-insuficiente)
  - [Pagamento Negado](#compensacao---pagamento-negado)
- [Topicos Kafka](#topicos-kafka)
- [Schemas de Banco de Dados](#schemas-de-banco-de-dados)
- [Patterns Implementados](#patterns-implementados)
- [API Endpoints](#api-endpoints)
- [Como Executar](#como-executar)
- [Observabilidade](#observabilidade)
  - [Stack](#stack)
  - [Fluxo de Dados](#fluxo-de-dados)
  - [Correlacao Log-Trace](#correlacao-log-trace)
- [Variaveis de Ambiente](#variaveis-de-ambiente)

---

## Tecnologias

| Componente | Tecnologia |
|---|---|
| Orders Service | Go 1.25, Gin, MySQL, Redis |
| Inventory Service | Kotlin, Spring Boot 3.4, PostgreSQL 17 |
| Payments Service | Go 1.25, Gin, MySQL |
| Mensageria | Apache Kafka (Confluent 7.6) |
| Cache/Idempotencia | Redis |
| Observabilidade | OpenTelemetry Collector, Jaeger, Loki, Grafana |
| Logging | Zap + OTel Bridge (Go), SLF4J/Logback (Kotlin) |
| Kafka Client (Go) | segmentio/kafka-go |
| Kafka Client (Kotlin) | Spring Kafka |
| UI Kafka | Kafka UI (Provectus) |

---

## Servicos

### Orders Service

**Porta:** 8081 | **Banco:** MySQL | **Linguagem:** Go

Ponto de entrada do sistema. Recebe requisicoes HTTP para criacao de pedidos e coordena o inicio da saga. Consome eventos de confirmacao e cancelamento.

**Responsabilidades:**
- Criar pedidos com status `PENDING`
- Publicar evento `orders.created` via Outbox
- Confirmar pedido (`CONFIRMED`) ao receber `payments.authorized`
- Cancelar pedido (`CANCELED`) ao receber `inventory.insufficient-stock` ou `inventory.released`
- Publicar evento `orders.confirmed` via Outbox
- Garantir idempotencia via Redis (`Idempotency-Key` header)

**Estrutura:**
```
orders-service/
├── cmd/api/main.go              # Entrypoint
├── internal/
│   ├── config/                  # Configuracao via env vars
│   ├── handler/                 # HTTP handlers (Gin)
│   ├── service/                 # Logica de negocio
│   ├── repository/              # Acesso a dados (MySQL)
│   ├── domain/                  # Modelos e enums
│   ├── consumer/                # Kafka consumers
│   │   ├── inventory_insufficient_stock.go
│   │   ├── inventory_released.go
│   │   └── payments.go
│   ├── relay/                   # Outbox relay (polling)
│   ├── middleware/              # Idempotencia (Redis)
│   └── logger/                  # Logging (Zap)
└── INIT.sql                     # Schema do banco
```

### Inventory Service

**Porta:** 8082 | **Banco:** PostgreSQL | **Linguagem:** Kotlin

Gerencia produtos, estoque e reservas. Implementa **Arquitetura Hexagonal** com ports e adapters.

**Responsabilidades:**
- CRUD de produtos e estoque
- Reservar estoque ao receber `orders.created`
- Liberar estoque (compensacao) ao receber `payments.denied`
- Confirmar reserva ao receber `orders.confirmed`
- Publicar `inventory.reserved`, `inventory.insufficient-stock` ou `inventory.released`
- Pessimistic locking (`SELECT FOR UPDATE`) para controle de concorrencia

**Estrutura (Hexagonal):**
```
inventory-service/src/main/kotlin/br/com/souza/inventory_service/
├── adapter/
│   ├── in/
│   │   ├── web/                 # REST controllers
│   │   └── consumer/           # Kafka consumers
│   │       ├── order/          # OrderCreated, OrderConfirmed
│   │       └── payments/       # PaymentsDenied
│   └── out/
│       ├── product/            # Product persistence
│       ├── stock/              # Stock persistence
│       ├── reservation/        # Reservation persistence
│       ├── relay/              # OutboxRelayScheduler
│       └── models/             # JPA entities
├── application/
│   ├── domain/
│   │   ├── model/              # Domain models
│   │   └── service/            # Use cases
│   │       ├── ReserveStockService
│   │       ├── ReleaseStockService
│   │       └── ConfirmReservationService
│   └── ports/                  # Interfaces (in/out)
└── infrastructure/             # Observability, config
```

### Payments Service

**Porta:** 8083 | **Banco:** MySQL | **Linguagem:** Go

Processa pagamentos com base em regras de negocio. Nao expoe endpoints de criacao — opera exclusivamente via eventos Kafka.

**Responsabilidades:**
- Consumir `inventory.reserved` e processar pagamento
- Avaliar regras de pagamento
- Publicar `payments.authorized` ou `payments.denied` via Outbox
- Idempotencia via constraint de banco (`order_id UNIQUE`)


**Estrutura:**
```
payments-service/
├── cmd/api/main.go
├── internal/
│   ├── config/
│   ├── handler/
│   ├── service/                 # Logica de avaliacao de pagamento
│   ├── repository/
│   ├── domain/
│   ├── consumer/                # Consome inventory.reserved
│   ├── relay/                   # Outbox relay
│   └── logger/
└── INIT.sql
```

---

## Fluxos da Saga

### Happy Path - Pedido Confirmado

```
Client                Orders           Inventory          Payments
  │                     │                  │                  │
  │  POST /v1/orders    │                  │                  │
  │────────────────────>│                  │                  │
  │     201 Created     │                  │                  │
  │<────────────────────│                  │                  │
  │                     │                  │                  │
  │                     │ orders.created   │                  │
  │                     │─────────────────>│                  │
  │                     │                  │                  │
  │                     │                  │ Reserva estoque  │
  │                     │                  │                  │
  │                     │                  │inventory.reserved│
  │                     │                  │─────────────────>│
  │                     │                  │                  │
  │                     │                  │   Aprova pgto    │
  │                     │                  │                  │
  │                     │payments.authorized                  │
  │                     │<────────────────────────────────────│
  │                     │                  │                  │
  │                     │ Confirma pedido  │                  │
  │                     │                  │                  │
  │                     │orders.confirmed  │                  │
  │                     │─────────────────>│                  │
  │                     │                  │                  │
  │                     │                  │Confirma reserva  │
  │                     │                  │                  │
```

**Status do pedido:** `PENDING` -> `CONFIRMED`

### Compensacao - Estoque Insuficiente

```
Client                Orders           Inventory
  │                     │                  │
  │  POST /v1/orders    │                  │
  │────────────────────>│                  │
  │     201 Created     │                  │
  │<────────────────────│                  │
  │                     │ orders.created   │
  │                     │─────────────────>│
  │                     │                  │
  │                     │                  │ Estoque insuficiente
  │                     │                  │
  │                     │ inventory.       │
  │                     │ insufficient-    │
  │                     │ stock            │
  │                     │<─────────────────│
  │                     │                  │
  │                     │ Cancela pedido   │
  │                     │                  │
```

**Status do pedido:** `PENDING` -> `CANCELED` (reason: estoque insuficiente)

### Compensacao - Pagamento Negado

```
Client                Orders           Inventory          Payments
  │                     │                  │                  │
  │  POST /v1/orders    │                  │                  │
  │────────────────────>│                  │                  │
  │     201 Created     │                  │                  │
  │<────────────────────│                  │                  │
  │                     │ orders.created   │                  │
  │                     │─────────────────>│                  │
  │                     │                  │inventory.reserved│
  │                     │                  │─────────────────>│
  │                     │                  │                  │
  │                     │                  │   Nega pgto      │
  │                     │                  │                  │
  │                     │                  │ payments.denied  │
  │                     │                  │<─────────────────│
  │                     │                  │                  │
  │                     │                  │ Libera estoque   │
  │                     │                  │                  │
  │                     │inventory.released│                  │
  │                     │<─────────────────│                  │
  │                     │                  │                  │
  │                     │ Cancela pedido   │                  │
  │                     │                  │                  │
```

**Status do pedido:** `PENDING` -> `CANCELED` (reason: pagamento negado)
**Status da reserva:** `RESERVED` -> `RELEASED`

---

## Topicos Kafka

Todos os topicos sao criados com **3 particoes** e **retencao de 7 dias**.

| Topico | Produtor | Consumidor(es) | Descricao |
|---|---|---|---|
| `orders.created` | Orders | Inventory | Novo pedido criado |
| `orders.confirmed` | Orders | Inventory | Pedido confirmado |
| `inventory.reserved` | Inventory | Payments | Estoque reservado com sucesso |
| `inventory.insufficient-stock` | Inventory | Orders | Estoque insuficiente |
| `inventory.released` | Inventory | Orders | Estoque liberado (compensacao) |
| `payments.authorized` | Payments | Orders | Pagamento autorizado |
| `payments.denied` | Payments | Inventory | Pagamento negado |

---

## Schemas de Banco de Dados

### Orders (MySQL)

```sql
orders (
    id              CHAR(36) PK,
    user_id         CHAR(36),
    product_id      INT,
    quantity         INT,
    payment_type    VARCHAR(20),         -- PIX, CREDIT_CARD, BOLETO
    status          VARCHAR(20),         -- PENDING, CONFIRMED, CANCELED
    idempotency_key VARCHAR(255) UNIQUE,
    reason          TEXT,
    created_at      DATETIME(3),
    updated_at      DATETIME(3)
)
```

### Payments (MySQL)

```sql
payments (
    id              CHAR(36) PK,
    order_id        CHAR(36) UNIQUE,
    amount          INT,
    payment_type    VARCHAR(20),         -- PIX, CREDIT_CARD, BOLETO
    status          VARCHAR(20),         -- AUTHORIZED, DENIED
    reason          VARCHAR(100),
    created_at      DATETIME(3)
)
```

### Inventory (PostgreSQL)

```sql
products (
    id    INT PK AUTO_INCREMENT,
    name  VARCHAR(255),
    price INTEGER                        -- em centavos
)

stocks (
    id                  VARCHAR(36) PK,
    product_id          INTEGER UNIQUE FK,
    quantity_available   INTEGER,
    updated_at          TIMESTAMP
)

stock_reservations (
    id          VARCHAR(36) PK,
    order_id    VARCHAR(36),
    product_id  INTEGER FK,
    quantity    INTEGER,
    status      VARCHAR(50),             -- RESERVED, CONFIRMED, RELEASED
    created_at  TIMESTAMP
)
```

### Outbox (todos os servicos)

Cada servico possui uma tabela de outbox com a mesma estrutura:

```sql
outbox_events (
    id              CHAR(36) PK,
    aggregate_type  VARCHAR(50),         -- ORDER, STOCK_RESERVATION, PAYMENT
    aggregate_id    CHAR(36),
    event_type      VARCHAR(100),        -- nome do topico Kafka
    payload         JSON/TEXT,
    trace_parent    VARCHAR(255),        -- contexto de tracing (W3C)
    status          VARCHAR(20),         -- PENDING, PROCESSING, SENT, FAILED, DEAD_LETTER
    retries_count   INT DEFAULT 0,
    max_retries     INT DEFAULT 5,
    created_at      DATETIME/TIMESTAMP,
    sent_at         DATETIME/TIMESTAMP,
    locked_at       DATETIME/TIMESTAMP
)
```

---

## Patterns Implementados

### 1. Saga Pattern (Choreography)

Cada servico publica eventos de dominio e reage a eventos de outros servicos. Nao existe um orquestrador central — a coordenacao emerge da coreografia dos eventos.

**Vantagens:**
- Baixo acoplamento entre servicos
- Cada servico e autonomo
- Facil de adicionar novos consumidores

### 2. Transactional Outbox

A operacao de negocio e o evento sao gravados na **mesma transacao** do banco de dados, garantindo consistencia atomica. Um relay process separado faz polling na tabela outbox e publica no Kafka.

```
┌──────────────────────────────────────┐
│           Transacao DB               │
│                                      │
│  1. INSERT/UPDATE tabela de negocio  │
│  2. INSERT tabela outbox (PENDING)   │
│                                      │
│  COMMIT                              │
└──────────────────────────────────────┘
           │
           ▼
┌──────────────────────────────────────┐
│         Outbox Relay (polling)       │
│                                      │
│  1. SELECT ... WHERE status=PENDING  │
│     FOR UPDATE SKIP LOCKED           │
│  2. Publica no Kafka                 │
│  3. UPDATE status = SENT             │
└──────────────────────────────────────┘
```

### 3. Idempotencia

Protecao contra processamento duplicado em tres niveis:

| Nivel | Servico | Mecanismo |
|---|---|---|
| API | Orders | Redis + header `Idempotency-Key` |
| Banco | Payments | Constraint `UNIQUE(order_id)` |
| Outbox | Inventory | `existsByAggregateId()` antes de processar |

### 4. Pessimistic Locking

O Inventory Service usa `SELECT FOR UPDATE` ao reservar estoque, prevenindo race conditions em cenarios de alta concorrencia.

### 5. Distributed Tracing & Centralized Logging (OpenTelemetry)

Contexto de tracing (`traceparent`) e propagado via headers do Kafka, permitindo rastreamento ponta a ponta de um pedido atraves de todos os servicos. Logs sao enviados ao Loki via OTel Collector com correlacao automatica de `trace_id` e `span_id`.

```
Orders (span) ──> Kafka ──> Inventory (child span) ──> Kafka ──> Payments (child span)
```

### 6. Hexagonal Architecture (Inventory Service)

O Inventory Service implementa arquitetura hexagonal com separacao clara entre:
- **Ports** (interfaces): definem contratos de entrada e saida
- **Adapters**: implementacoes concretas (web, Kafka, JPA)
- **Domain**: regras de negocio isoladas de frameworks

---

## API Endpoints

### Orders Service (:8081)

| Metodo | Path | Descricao |
|---|---|---|
| `POST` | `/v1/orders` | Cria um novo pedido |
| `GET` | `/health` | Health check |

### Inventory Service (:8082)

| Metodo | Path | Descricao |
|---|---|---|
| `GET` | `/v1/products` | Lista todos os produtos |
| `POST` | `/v1/products` | Cria um produto |
| `GET` | `/v1/products/stocks` | Lista todos os estoques |
| `POST` | `/v1/products/{id}/stock` | Cria estoque para um produto |
| `PATCH` | `/v1/products/{id}/stock/quantity` | Atualiza quantidade do estoque |

### Payments Service (:8083)

| Metodo | Path | Descricao |
|---|---|---|
| `GET` | `/health` | Health check |

> O Payments Service nao expoe endpoints de criacao — opera exclusivamente via eventos Kafka.

---

## Como Executar

### Pre-requisitos

- Docker e Docker Compose
- Go 1.25+ (para rodar os servicos Go localmente)
- JDK 21+ e Maven (para rodar o Inventory Service localmente)

### 1. Subir a infraestrutura

```bash
docker compose up -d
```

Isso inicia: MySQL (x2), PostgreSQL, Redis, Zookeeper, Kafka, Kafka UI, OTel Collector, Jaeger, Loki, Grafana e cria automaticamente os topicos Kafka.

### 2. Verificar se os topicos foram criados

Acesse o Kafka UI em [http://localhost:8080](http://localhost:8080) ou:

```bash
docker exec -it saga-pattern-kafka-1 kafka-topics --bootstrap-server localhost:9092 --list
```

### 3. Rodar os servicos

**Orders Service:**
```bash
cd orders-service
go run cmd/api/main.go
```

**Inventory Service:**
```bash
cd inventory-service
./mvnw spring-boot:run
```

**Payments Service:**
```bash
cd payments-service
go run cmd/api/main.go
```

### 4. Criar produto e estoque

```bash
# Criar produto
curl -X POST http://localhost:8082/v1/products \
  -H "Content-Type: application/json" \
  -d '{"name": "Camiseta", "price": 5000}'

# Criar estoque (product_id = 1)
curl -X POST http://localhost:8082/v1/products/1/stock \
  -H "Content-Type: application/json" \
  -d '{"quantityAvailable": 100}'
```

### 5. Criar um pedido

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

---

## Observabilidade

### Stack

| Componente | Tecnologia | Porta | Descricao |
|---|---|---|---|
| **OTel Collector** | OpenTelemetry Collector Contrib | `4317` (gRPC), `4318` (HTTP) | Recebe traces e logs dos servicos via OTLP e roteia para Jaeger e Loki |
| **Jaeger** | Jaeger All-in-One | `16686` | Armazenamento e visualizacao de distributed traces |
| **Loki** | Grafana Loki | `3100` | Agregacao e indexacao de logs |
| **Grafana** | Grafana | `3000` | Dashboards e visualizacao unificada de traces e logs |

### Fluxo de Dados

```
┌──────────────────────────────────────────────────────────────┐
│  Go Services (orders-service, payments-service)              │
│  ├─ Gin HTTP Handlers (otelgin middleware)                   │
│  ├─ Kafka Consumers (trace context extraction)               │
│  ├─ Zap Logger (JSON → stdout)                               │
│  └─ OTel SDK (TracerProvider + LoggerProvider)               │
└─────────────────────┬────────────────────────────────────────┘
                      │ OTLP gRPC (:4317)
                      │ traces + logs
                      ▼
┌──────────────────────────────────────────────────────────────┐
│  OpenTelemetry Collector                                     │
│  ├─ Receivers: otlp (gRPC + HTTP)                            │
│  ├─ Processors: batch, transform/logs, resource              │
│  └─ Exporters:                                               │
│       ├─ otlp_grpc/jaeger → Jaeger (:4317)                  │
│       └─ otlp_http/loki   → Loki (:3100/otlp)               │
└──────────┬──────────────────────────────────┬────────────────┘
           │                                  │
           ▼                                  ▼
    ┌────────────┐                     ┌──────────┐
    │   Jaeger   │                     │   Loki   │
    │  (Traces)  │                     │  (Logs)  │
    └──────┬─────┘                     └────┬─────┘
           │                                │
           └──────────────┬─────────────────┘
                          ▼
                  ┌──────────────┐
                  │   Grafana    │
                  │   (:3000)    │
                  │              │
                  │ Datasources: │
                  │ ├─ Jaeger    │
                  │ └─ Loki      │
                  └──────────────┘
```

**OTel Collector** atua como ponto central de coleta. Os servicos Go enviam traces e logs via OTLP gRPC. O Collector processa os dados (batching, transformacao de logs com `severity_text`, extracao de `trace_id`/`span_id`) e roteia traces para o **Jaeger** e logs para o **Loki**.

### Correlacao Log-Trace

Os logs enviados ao Loki incluem `trace_id` e `span_id` como atributos. O Grafana esta configurado com **Derived Fields** no datasource do Loki, permitindo clicar em um `trace_id` no log e navegar diretamente para o trace correspondente no Jaeger.

**Instrumentacao nos servicos Go:**
- **HTTP:** middleware `otelgin` cria spans automaticamente para cada request
- **Kafka:** trace context e extraido dos headers das mensagens (`traceparent`)
- **Logger:** `logger.FromContext(ctx)` enriquece cada log com `trace_id` e `span_id` do span ativo
- **Dual output:** cada log e enviado tanto para stdout (JSON/Zap) quanto para o OTel Collector (via `otelzap` bridge)

### Acessando as UIs

| Servico | URL | Descricao |
|---|---|---|
| Grafana | [http://localhost:3000](http://localhost:3000) | Dashboards, logs (Loki) e traces (Jaeger) |
| Jaeger | [http://localhost:16686](http://localhost:16686) | UI nativa do Jaeger para busca de traces |
| Loki | `http://localhost:3100` | API de logs (acessado via Grafana) |

> O Grafana ja vem provisionado com os datasources do Jaeger e Loki. Acesso anonimo esta habilitado para desenvolvimento.

---

## Variaveis de Ambiente

### Orders Service

| Variavel | Default | Descricao |
|---|---|---|
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

### Payments Service

| Variavel | Default | Descricao |
|---|---|---|
| `SERVER_PORT` | `:8083` | Porta do servidor HTTP |
| `MYSQL_DSN` | `root:root@tcp(localhost:3308)/payments?parseTime=true` | DSN do MySQL |
| `KAFKA_BROKERS` | `localhost:29092` | Brokers Kafka |
| `KAFKA_INVENTORY_RESERVED_TOPIC` | `inventory.reserved` | Topico de estoque reservado |
| `KAFKA_PAYMENT_AUTHORIZED_TOPIC` | `payments.authorized` | Topico de pagamento autorizado |
| `KAFKA_PAYMENT_DENIED_TOPIC` | `payments.denied` | Topico de pagamento negado |
| `OUTBOX_BATCH_SIZE` | `10` | Tamanho do batch do relay |
| `OTEL_EXPORTER_ENDPOINT` | `localhost:4317` | Endpoint gRPC do OTel Collector |

### Inventory Service

Configurado via `application.yaml`:

| Propriedade | Default | Descricao |
|---|---|---|
| `spring.datasource.url` | `jdbc:postgresql://localhost:5432/inventory_db` | URL do PostgreSQL |
| `spring.datasource.username` | `inventory` | Usuario do banco |
| `spring.datasource.password` | `inventory` | Senha do banco |
| `spring.kafka.bootstrap-servers` | `localhost:29092` | Brokers Kafka |
| `server.port` | `8082` | Porta do servidor |
