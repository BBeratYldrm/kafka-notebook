package com.berat.kafkademo.inventory;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class InventoryConsumer {

    @KafkaListener(topics = "orders", groupId = "inventory-group")
    public void handleOrder(String orderDetails) {
        System.out.println("Inventory processing for order: " + orderDetails);
        System.out.println("Inventory completed for: " + orderDetails);
    }
}
