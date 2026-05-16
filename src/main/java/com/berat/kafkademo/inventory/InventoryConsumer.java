package com.berat.kafkademo.inventory;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

/**
 * Listens to order events and updates stock levels.
 * Fully decoupled — reacts to Kafka events, doesn't call any other service.
 * <p>
 * WHY "inventory-group"?
 * Separate consumer group means this service gets every message
 * independently from payment and notification services.
 */
@Service
public class InventoryConsumer {

    @KafkaListener(topics = "orders", groupId = "inventory-group")
    public void handleOrder(String orderDetails) {
        System.out.println("Inventory processing for order: " + orderDetails);
        System.out.println("Inventory completed for: " + orderDetails);
    }
}
