package com.example.learnkafka.config;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;

/**
 * author        yiliyang
 * date          2023-03-20
 * time          下午4:31
 * version       1.0
 * since         1.0
 */
@Configuration
public class KafkaConsumer {

    @KafkaListener(topics = {"topic_demo"}, groupId = "test_consumer_group", containerFactory = "kafkaListenerContainerFactory")
    public void kafkaListener(ConsumerRecord<String, String> message) {
        System.out.println("receive " + message);
    }
}
