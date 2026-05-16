package com.berat.kafkademo.order;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * Publishes order events to Kafka — decouples order creation from downstream processing.
 */
@Service
public class OrderProducer {

    private static final String TOPIC = "orders";

    private final KafkaTemplate<String, String> kafkaTemplate;

    public OrderProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * WHY partition key?
     * Kafka guarantees ordering only within the same partition.
     * By using orderId as the key, all events for the same order
     * always go to the same partition — ordering is preserved.
     */
    public void sendOrder(String orderId, String orderDetails) {
        kafkaTemplate.send(TOPIC, orderId, orderDetails);
        System.out.println("Order sent to Kafka | key: " + orderId + " | value: " + orderDetails);
    }
}