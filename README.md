# Event-Driven Order / Inventory System

Small Java/Spring system with two services communicating through Kafka.

The goal is not to model a full order lifecycle, but to show a practical event-driven integration:

- `order-api` accepts order requests and publishes `OrderCreated` events.
- `inventory-service` consumes those events and simulates stock reservation.
- Kafka is used as the integration boundary between the services.
- State is kept in memory, which is acceptable for the scope of the assignment.

## Running Locally

```bash
mvn clean package -DskipTests
docker compose up --build -d
```

Health checks:

```bash
curl http://localhost:8080/health
docker compose exec inventory-service wget -qO- http://localhost:8081/health
```

Create an order:

```bash
curl -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"orderId":"ORD-001","itemId":"item-1","quantity":10}'
```

Check inventory processing:

```bash
docker compose logs -f inventory-service
```

## Running Tests

```bash
mvn test
```

The test suite covers:

- Order API validation and error handling.
- Kafka publish mapping: topic key, event envelope and headers.
- Inventory processing with Embedded Kafka.
- Duplicate order handling in the inventory consumer.
- Dead-letter routing for malformed messages.

## Architecture

```
Client
  |
  | POST /orders
  v
Order API
  |
  | Kafka topic: orders
  | key: itemId
  v
Inventory Service
  |
  | reserve or reject in memory
  v
logs / in-memory state
```

Kafka topics created by `docker-compose.yml`:

- `orders` - order events
- `orders-dlt` - messages that cannot be deserialized or processed

## API

### `POST /orders`

Request:

```json
{
  "orderId": "ORD-001",
  "itemId": "item-1",
  "quantity": 10
}
```

Response:

```json
{
  "orderId": "ORD-001",
  "correlationId": "uuid",
  "status": "accepted"
}
```

`202 Accepted` means the order event was accepted by Kafka. It does not mean inventory has already been reserved. Inventory reservation happens asynchronously in `inventory-service`.

Errors:

- `400 Bad Request` for invalid input
- `503 Service Unavailable` when the event cannot be published to Kafka within the configured timeout

### `GET /health`

Liveness endpoint. Returns `{"status":"UP"}` while the service process is running.

### `GET /ready`

Readiness endpoint. For `order-api`, this checks whether Kafka topic metadata can be read. For `inventory-service`, this checks whether the consumer loop is running. Inventory Service is not exposed on a host port because it is only consumed through Kafka; its health endpoint is used by the container healthcheck.

## Event Format

Kafka message key is `itemId`, so orders for the same item are routed to the same partition.

Message value:

```json
{
  "eventId": "uuid",
  "eventType": "OrderCreated",
  "eventVersion": "1.0",
  "producer": "order-api",
  "occurredAt": "2026-05-22T12:00:00Z",
  "correlationId": "uuid",
  "metadata": {},
  "payload": {
    "orderId": "ORD-001",
    "itemId": "item-1",
    "quantity": 10
  }
}
```

The producer also writes the main envelope fields to Kafka headers:

- `correlationId`
- `eventId`
- `eventType`
- `eventVersion`
- `producer`

## Design Decisions

### Partition key: `itemId`

Inventory is item-centric. If messages for the same item go to the same Kafka partition, one consumer instance processes them in order within the consumer group. This keeps the reservation logic simple and reduces the chance of concurrent updates for the same item.

The tradeoff is that a very popular item can become a hot partition. For this task, that is acceptable.

### Consumer group and scaling

`inventory-service` uses a Kafka consumer group (`inventory-service`). If multiple inventory instances are running, Kafka can distribute topic partitions between them. The important part is that the message key stays `itemId`: different items can be processed in parallel, while events for the same item stay ordered within one partition.

The current `docker-compose.yml` starts one inventory instance. To experiment with scaling locally, remove the fixed `container_name` for `inventory-service`, then run:

```bash
docker compose up -d --scale inventory-service=2
```

With the current in-memory stock map, this is only a consumer-group demonstration. A production version would need external state, or a partition-owned state model with a changelog/recovery strategy, so that state survives restarts and rebalances.

Important limitation: in-memory stock is local to each running `inventory-service` instance. Scaling the service does not copy or synchronize that state between containers. In this demo, scaling is useful to show Kafka consumer group behavior and partition assignment, not to provide a correct distributed inventory store.

### Synchronous publish from Order API

The API waits for Kafka to acknowledge the produced event before returning `202`. If Kafka is unavailable or the send times out, the API returns `503` with `Retry-After`.

This keeps the API response honest: the client knows whether the command was handed to the broker.

### Manual commit in Inventory Service

The consumer uses manual offset commit. It commits after a batch has been handled, instead of relying on Kafka auto-commit.

This is still a small demo implementation, but the important idea is that message acknowledgement should be tied to processing, not to polling alone.

The consumer also sets a few operational Kafka options explicitly: `auto.offset.reset=earliest`, `max.poll.records=100`, `max.poll.interval.ms=300000`, `session.timeout.ms=30000`, `heartbeat.interval.ms=10000` and `CooperativeStickyAssignor`.

### Idempotency

The inventory consumer tracks processed commands in memory using `eventType:orderId`. This catches simple client retries where the same order is submitted again and a new event id is generated.

In a larger system, this would probably move to a persistent store and include conflict detection for "same order id, different payload".

### Dead-letter topic

Malformed messages and processing failures are sent to `orders-dlt` with headers describing the original topic, partition, offset and failure reason.

This keeps the main consumer from looping forever on a bad message, while still preserving the failed payload for inspection.

## Reservation Result Flow

For this assignment, inventory results are logged by `inventory-service`.

A natural next step would be to publish a follow-up event such as `InventoryReserved` or `InventoryRejected` to a separate topic. `order-api` could consume those events into a small in-memory order-status read model. A UI or client could then poll `GET /orders/{orderId}` and see the order move from `PENDING` to `RESERVED` or `REJECTED`.

I kept this out of the current implementation to avoid adding a second event flow before the core producer-consumer integration was tested and easy to reason about.

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BROKERS` | `localhost:9092` | Kafka bootstrap servers |
| `KAFKA_ORDERS_TOPIC` | `orders` | Main orders topic |
| `KAFKA_DLT_TOPIC` | `orders-dlt` | Dead-letter topic |
| `CONSUMER_GROUP_ID` | `inventory-service` | Inventory consumer group |
| `PRODUCER_TIMEOUT_MS` | `5000` | Kafka publish timeout |
| `HTTP_PORT` | `8080` / `8081` | Service HTTP port |
| `LOG_LEVEL` | `INFO` | Logging level |

## Initial Stock

`inventory-service` loads stock from `stock-init.json`:

```json
{
  "item-1": 100,
  "item-2": 50,
  "item-3": 200
}
```

## Manual Scenarios

Successful reservation:

```bash
curl -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"orderId":"ORD-001","itemId":"item-1","quantity":10}'
```

Validation error:

```bash
curl -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"orderId":"","itemId":"item-1","quantity":-1}'
```

Duplicate order:

```bash
curl -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"orderId":"ORD-001","itemId":"item-1","quantity":10}'
```

Check inventory logs for `Skipping duplicate`.

Stock exhaustion:

```bash
for i in 1 2 3 4 5 6; do
  curl -X POST http://localhost:8080/orders \
    -H "Content-Type: application/json" \
    -d "{\"orderId\":\"ORD-X-$i\",\"itemId\":\"item-2\",\"quantity\":10}"
done
```

The sixth order should be rejected because `item-2` starts with 50 units.

## Scope And Tradeoffs

- Results are logged by Inventory Service instead of being returned synchronously to Order API.
- Inventory and processed-event state are in memory and are lost on restart.
- There is no database and no outbox pattern because there is no local database write to coordinate with Kafka.
- The DLT implementation is intentionally simple and meant for local inspection, not full production operations.
- Multi-item orders are out of scope. They would need a different consistency model, usually a saga or order-level reservation workflow.

## Cleanup

```bash
docker compose down -v
```
