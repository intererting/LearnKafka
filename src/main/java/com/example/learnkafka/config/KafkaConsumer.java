package com.example.learnkafka.config;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;

import java.util.Arrays;
import java.util.List;

/**
 * author        yiliyang
 * date          2023-03-20
 * time          下午4:31
 * version       1.0
 * since         1.0
 */
@Configuration
public class KafkaConsumer {

    /**
     * concurrency==分区数,那么刚好一个线程消费一个,大于了浪费
     */
    @KafkaListener(topics = {"topic_demo"}, groupId = "topic_demo_group", concurrency = "2", containerFactory = "kafkaListenerContainerFactory")
    public void topicDemokafkaListener(ConsumerRecord<String, Object> message) {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        System.out.println("receive " + Thread.currentThread());
    }

    @KafkaListener(topics = {"router"}, groupId = "router_group", containerFactory = "routeKafkaListenerContainerFactory")
    public void routerKafkaListener(ConsumerRecord<String, Object> message) {
        System.out.println("router " + Arrays.toString((byte[]) message.value()));
    }

}
