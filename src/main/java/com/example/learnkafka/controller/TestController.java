package com.example.learnkafka.controller;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * author        yiliyang
 * date          2023-03-20
 * time          下午4:25
 * version       1.0
 * since         1.0
 */
@RestController
public class TestController {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @GetMapping("/test")
    public void test() {
        kafkaTemplate.setProducerListener(new ProducerListener<>() {
            @Override
            public void onSuccess(ProducerRecord<String, String> producerRecord, RecordMetadata recordMetadata) {
                System.out.println("send success");
                System.out.println(producerRecord.topic());
                System.out.println(recordMetadata.partition());
            }

            @Override
            public void onError(ProducerRecord<String, String> producerRecord, RecordMetadata recordMetadata, Exception exception) {
                System.out.println("send error");
                System.out.println(producerRecord.topic());
                System.out.println(recordMetadata.partition());
            }
        });
        kafkaTemplate.send("topic_demo", "hello kafka");
    }

}
