package com.berat.kafkademo.notification;

import org.springframework.kafka.annotation.KafkaListener;
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
 */
@Service
public class NotificationConsumer {

    @KafkaListener(topics = "orders", groupId = "notification-group")
    public void handleOrder(String orderDetails) {
        System.out.println("Notification received for order: " + orderDetails);
        // simulate sending email/SMS
        System.out.println("Email sent for: " + orderDetails);
    }
}