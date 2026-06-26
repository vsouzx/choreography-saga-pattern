# Inventory Service

Microservico responsavel por gerenciar produtos, estoque e reservas de estoque. Participa da saga de pedidos consumindo e produzindo eventos via Kafka com **Transactional Outbox**.

## Stack

- **Kotlin** 2.1.10 com **Java 21**
- **Spring Boot** 3.4.5 (Web MVC, Data JPA, Kafka)
- **PostgreSQL** — persistencia
- **Apache Kafka** (Spring Kafka) — mensageria
- **OpenTelemetry** — tracing distribuido

## Arquitetura

Arquitetura Hexagonal com separacao clara entre dominio, aplicacao (use cases/ports) e adapters.

```
src/main/kotlin/br/com/souza/inventory_service/
├── adapter/
│   ├── in/
│   │   ├── consumer/             # Kafka consumers
│   │   │   ├── order/            # OrderCreated, OrderConfirmed
│   │   │   └── payments/         # PaymentsDenied
│   │   └── web/                  # REST controllers
│   └── out/
│       ├── product/              # Product persistence (JPA)
│       ├── stock/                # Stock persistence (JPA)
│       ├── reservation/          # Reservation persistence (JPA)
│       ├── relay/                # OutboxRelayScheduler (polling a cada 1s, batch 50)
│       └── models/               # JPA entities
├── application/
│   ├── domain/
│   │   ├── model/                # Modelos de dominio
│   │   └── service/              # Use cases
│   │       ├── ReserveStockService
│   │       ├── ReleaseStockService
│   │       └── ConfirmReservationService
│   └── ports/
│       ├── in/                   # Ports de entrada (interfaces dos use cases)
│       └── out/                  # Ports de saida (interfaces dos repositorios)
└── infrastructure/
    ├── kafka/                    # Configuracao Kafka consumer/producer
    └── observability/            # Configuracao OpenTelemetry
```

## Participacao na Saga

| Topico | Direcao | Acao |
|--------|---------|------|
| `orders.created` | Consome | Reserva estoque para o pedido |
| `orders.confirmed` | Consome | Confirma a reserva (etapa final do happy path) |
| `payments.denied` | Consome | Libera estoque reservado (compensacao) |
| `inventory.reserved` | Produz | Notifica que o estoque foi reservado |
| `inventory.insufficient-stock` | Produz | Notifica que o estoque e insuficiente |
| `inventory.released` | Produz | Notifica que o estoque foi liberado |

## API

| Metodo | Rota | Descricao |
|--------|------|-----------|
| `GET` | `/v1/products` | Lista todos os produtos |
| `POST` | `/v1/products` | Cria um produto |
| `GET` | `/v1/products/stocks` | Lista todos os estoques |
| `POST` | `/v1/products/{id}/stock` | Cria estoque para um produto |
| `PATCH` | `/v1/products/{id}/stock/quantity` | Atualiza quantidade do estoque |

### Criar Produto e Estoque

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

## Executando

### Pre-requisitos

- Java 21+
- Infraestrutura via `docker compose up -d` no root do repositorio (PostgreSQL porta 5432, Kafka porta 29092)

### Build

```bash
./mvnw package
```

### Testes

```bash
./mvnw test                                       # Todos os testes
./mvnw test -Dtest=ReserveStockServiceTest        # Teste especifico
```

### Executar a aplicacao

```bash
./mvnw spring-boot:run
```

## Banco de Dados (PostgreSQL, porta 5432)

Schema em `INIT.sql`. Tabelas:

- **products** — catalogo de produtos com preco em centavos
- **stocks** — estoque por produto com constraint `UNIQUE(product_id)`
- **stock_reservations** — reservas de estoque (`RESERVED`, `CONFIRMED`, `RELEASED`)
- **outbox_events** — eventos pendentes para publicacao via relay

### Controle de Concorrencia

Utiliza **Pessimistic Locking** (`SELECT FOR UPDATE`) ao reservar estoque, prevenindo race conditions em cenarios de alta concorrencia.

### Idempotencia

Verifica `existsByAggregateId()` antes de processar um evento, evitando processamento duplicado.

## Configuracao

Configuracao via `application.yaml` com override por variaveis de ambiente (Spring Boot).

| Propriedade | Default | Descricao |
|-------------|---------|-----------|
| `server.port` | `8082` | Porta do servidor |
| `spring.datasource.url` | `jdbc:postgresql://localhost:5432/inventory_db` | URL do PostgreSQL |
| `spring.datasource.username` | `inventory` | Usuario do banco |
| `spring.datasource.password` | `inventory` | Senha do banco |
| `spring.kafka.bootstrap-servers` | `localhost:29092` | Brokers Kafka |
