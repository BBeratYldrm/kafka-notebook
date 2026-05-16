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

    public void sendOrder(String orderDetails) {
        kafkaTemplate.send(TOPIC, orderDetails);
        System.out.println("Order sent to Kafka: " + orderDetails);
    }
}