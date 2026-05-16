package com.berat.kafkademo.notification;

import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.stereotype.Service;

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
 */
@Service
public class NotificationConsumer {

    @RetryableTopic(
            attempts = "3",
            // retry topics: orders-retry-0, orders-retry-1
            // dead letter topic: orders-dlt
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE
    )
    @KafkaListener(topics = "orders", groupId = "notification-group")
    public void handleOrder(String orderDetails) {
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