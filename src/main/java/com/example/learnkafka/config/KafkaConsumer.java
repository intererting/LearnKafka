package com.example.learnkafka.config;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;

import java.util.Arrays;

/**
 * author        yiliyang
 * date          2023-03-20
 * time          下午4:31
 * version       1.0
 * since         1.0
 */
@Configuration
public class KafkaConsumer {

    @KafkaListener(topics = {"topic_demo"}, groupId = "topic_demo_group", containerFactory = "kafkaListenerContainerFactory")
    public void topicDemokafkaListener(ConsumerRecord<String, Object> message) {
        System.out.println("receive " + message);
    }

    @KafkaListener(topics = {"router"}, groupId = "router_group", containerFactory = "routeKafkaListenerContainerFactory")
    public void routerKafkaListener(ConsumerRecord<String, Object> message) {
        System.out.println(message.value());
    }

}
