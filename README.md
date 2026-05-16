# Kafka Demo — Event-Driven Order Notification System

A simple event-driven system built with Spring Boot and Apache Kafka.
Demonstrates producer/consumer architecture with decoupled services.

## Architecture

```
HTTP POST /orders
      ↓
OrderController
      ↓
OrderProducer → Kafka ("orders" topic)
                              ↓
        ┌─────────────────────┼────────────────────┐
        ↓                     ↓                    ↓
NotificationConsumer    PaymentConsumer      InventoryConsumer
(notification-group)    (payment-group)      (inventory-group)
        ↓                     ↓                    ↓
   Send email           Process payment         Update stock
```

## Key Kafka Concepts Demonstrated

- **Topic**: `orders` — single topic, multiple consumers
- **Consumer Groups**: each service has its own group — fan-out pattern
- **Partition Key**: same orderId always goes to same partition — ordering guaranteed
- **Offset**: Kafka tracks how far each consumer has read via `__consumer_offsets`
- **Retry + DLQ**: failed messages are retried automatically, then sent to Dead Letter Topic
- **Idempotency**: duplicate messages are detected and skipped using orderId tracking

## What is NOT in scope (but important to know)

- **At-least-once delivery**: Kafka guarantees a message is delivered at least once.
  Duplicates are possible — that's why idempotency matters.
- **Outbox Pattern**: to guarantee DB write and Kafka publish happen together atomically,
  write to an outbox table first, then a poller sends to Kafka.
  See: notes/distributed/19-kafka.md

## Tech Stack

- Java 21
- Spring Boot
- Apache Kafka
- Docker Compose

## Run Locally

```bash
# Start Kafka
docker compose up -d

# Run Spring Boot
./mvnw spring-boot:run

# Send an order
curl -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d "Order#1 - iPhone"
```

## Expected Output

```
Order sent to Kafka: Order#1 - iPhone
Notification received for order: Order#1 - iPhone
Email sent for: Order#1 - iPhone
Payment processing for order: Order#1 - iPhone
Payment completed for: Order#1 - iPhone
Inventory processing for order: Order#1 - iPhone
Inventory completed for: Order#1 - iPhone
```