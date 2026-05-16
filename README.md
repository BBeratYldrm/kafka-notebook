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
- **Consumer Groups**: each service has its own group — same message delivered to all groups independently
- **Fan-out pattern**: one event triggers multiple downstream services
- **Decoupling**: producers and consumers are fully independent

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