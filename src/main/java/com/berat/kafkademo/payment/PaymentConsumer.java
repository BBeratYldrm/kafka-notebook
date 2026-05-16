package com.berat.kafkademo.payment;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class PaymentConsumer {

    @KafkaListener(topics = "orders", groupId = "payment-group")
    public void handleOrder(String orderDetails) {
        System.out.println("Payment processing for order: " + orderDetails);
        System.out.println("Payment completed for: " + orderDetails);
    }
}
