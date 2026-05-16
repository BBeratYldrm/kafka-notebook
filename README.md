# Kafka Demo — Event-Driven Order Notification System

A simple event-driven system built with Spring Boot and Apache Kafka.
Demonstrates producer/consumer architecture with decoupled services.

## Architecture
HTTP POST /orders
↓
OrderController
↓
OrderProducer → Kafka ("orders" topic)
↓
NotificationConsumer
↓
Email/SMS simulation

## How It Works

1. Client sends a POST request to `/orders`
2. OrderProducer publishes the event to Kafka
3. NotificationConsumer reads from Kafka and simulates sending a notification
4. Services are fully decoupled — producer doesn't know about consumer

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
Order sent to Kafka: Order#1 - iPhone
Notification received for order: Order#1 - iPhone
Email sent for: Order#1 - iPhone