package com.example.orderstatusservice.service;


import com.example.orderservice.model.OrderEvent;
import com.example.orderservice.model.OrderStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Service
@RequiredArgsConstructor
@Slf4j
public class KafkaMessageService {


    private final KafkaTemplate<String, OrderStatus> kafkaTemplate;

    private final List<OrderStatus> messages = new ArrayList<>();

    @Value("${app.kafka.kafkaMessageTopic}")
    private String topicName;

    @Value("${app.kafka.kafkaMessageTopicStatus}")
    private String topicStatusName;

    public void add(OrderStatus message){
        messages.add(message);
        sendMessage(message);
    }

    public Optional<OrderStatus> getById(String product){
        return messages.stream().filter(p -> p.getStatus().equals(product)).findFirst();
    }

    public void sendMessage(OrderStatus message){
        kafkaTemplate.send(topicStatusName, message);
    }

    public OrderStatus orderEventToOrderStatus(OrderEvent message){
        OrderStatus orderStatus = new OrderStatus();
        orderStatus.setStatus("PROCESS: product " + message.getProduct() + ", quantity " + message.getQuantity());
        orderStatus.setDate(Instant.now());
        return orderStatus;
    }
}
