package com.berat.kafkademo.order;

import org.springframework.web.bind.annotation.*;

/**
 * REST endpoint that receives order requests and triggers Kafka events.
 */
@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderProducer orderProducer;

    public OrderController(OrderProducer orderProducer) {
        this.orderProducer = orderProducer;
    }

    @PostMapping
    public String placeOrder(@RequestParam String orderId,
                             @RequestBody String orderDetails) {
        orderProducer.sendOrder(orderId, orderDetails);
        return "Order placed: " + orderDetails;
    }
}