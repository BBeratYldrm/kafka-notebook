package com.berat.kafkademo.notification;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

/**
 * Listens to order events and simulates sending notifications — fully decoupled from order creation.
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