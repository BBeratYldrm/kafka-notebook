package com.berat.kafkademo.notification;

import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.Set;

/**
 * Listens to order events and simulates sending notifications.
 * Fully decoupled from order creation — doesn't know who sent the event.
 *
 * WHAT IS A CONSUMER GROUP?
 * Each service has its own groupId.
 * Kafka delivers the same message to every group independently.
 * This is the fan-out pattern — one event, multiple services react.
 *
 * WHY "notification-group"?
 * If we scale this service to 3 instances, Kafka distributes
 * messages across them automatically. No duplicate processing.
 *
 * WHAT IS RETRY + DLQ?
 * If processing fails, Kafka retries automatically.
 * After all retries are exhausted, message goes to DLQ (Dead Letter Queue).
 * DLQ = a separate topic where failed messages wait for manual inspection.
 * This prevents one bad message from blocking the entire queue.
 *
 * WHAT IS IDEMPOTENCY?
 * Same message may arrive twice due to retry or network issues.
 * We track processed orderIds to ensure each order is handled exactly once.
 * In production, use Redis or DB instead of in-memory Set.
 */
@Service
public class NotificationConsumer {

    // NOTE: In production, use Redis or DB — this is in-memory, resets on restart
    private final Set<String> processedOrders = new HashSet<>();

    @RetryableTopic(
            attempts = "3",
            // retry topics: orders-retry-0, orders-retry-1
            // dead letter topic: orders-dlt
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE
    )
    @KafkaListener(topics = "orders", groupId = "notification-group")
    public void handleOrder(String orderDetails,
                            @Header(KafkaHeaders.RECEIVED_KEY) String orderId) {

        // IDEMPOTENCY CHECK
        if (processedOrders.contains(orderId)) {
            System.out.println("Duplicate detected — skipping: " + orderId);
            return;
        }
        processedOrders.add(orderId);

        System.out.println("Notification received for order: " + orderDetails);

        // simulate failure for testing
        if (orderDetails.contains("FAIL")) {
            throw new RuntimeException("Simulated failure — will trigger retry");
        }

        System.out.println("Email sent for: " + orderDetails);
    }

    @DltHandler
    public void handleDlt(String orderDetails) {
        // DLQ handler — called after all retries are exhausted
        System.out.println("DLQ received — manual action needed for: " + orderDetails);
    }
}