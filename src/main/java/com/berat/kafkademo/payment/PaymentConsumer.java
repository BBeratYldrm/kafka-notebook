package com.berat.kafkademo.payment;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

/**
 * Listens to order events and processes payments.
 * Fully decoupled — reacts to Kafka events, doesn't call any other service.
 * <p>
 * WHY "payment-group"?
 * Separate consumer group means this service gets every message
 * independently from inventory and notification services.
 */
@Service
public class PaymentConsumer {

    @KafkaListener(topics = "orders", groupId = "payment-group")
    public void handleOrder(String orderDetails) {
        System.out.println("Payment processing for order: " + orderDetails);
        System.out.println("Payment completed for: " + orderDetails);
    }
}
