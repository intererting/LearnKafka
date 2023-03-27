package com.example.learnkafka.controller;

import com.example.learnkafka.config.JsonMessage;
import com.example.learnkafka.config.MessaegModel;
import jakarta.annotation.Resource;
import jakarta.annotation.Resources;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.RoutingKafkaTemplate;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.transaction.annotation.Transactional;
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

    @Resource(name = "kafkaTemplate")
    private KafkaTemplate<Object, Object> kafkaTemplate;

    @Autowired
    private RoutingKafkaTemplate routingTemplate;

    @GetMapping("/test")
    public void test() {
        kafkaTemplate.setProducerListener(new ProducerListener<>() {
            @Override
            public void onSuccess(ProducerRecord<Object, Object> producerRecord, RecordMetadata recordMetadata) {
                System.out.println("send success");
                System.out.println(recordMetadata.partition());
            }

            @Override
            public void onError(ProducerRecord<Object, Object> producerRecord, RecordMetadata recordMetadata, Exception exception) {
                System.out.println("send error");
                System.out.println(recordMetadata.partition());
            }
        });

        //        kafkaTemplate.executeInTransaction(operations -> {
        //            operations.send("topic_demo", "first");
        //            //            int a = 1 / 0;
        //            operations.send("topic_demo", "second");
        //            return true;
        //        });

        //        kafkaTemplate.send("topic_demo", "hello world");

        //                for (int i = 0; i < 20; i++) {
        //                    try {
        //                        Thread.sleep(200);
        //                    } catch (InterruptedException e) {
        //                        throw new RuntimeException(e);
        //                    }
        //                    kafkaTemplate.send("topic_demo", "hello kafka " + i);
        //                }

        kafkaTemplate.send("topic_demo", new JsonMessage("yuliyang", 11));

        //测试一个线程一个生产者
        //        new Thread(() -> {
        //            for (int i = 0; i < 20; i++) {
        //                kafkaTemplate.send("topic_demo", "hello kafka " + System.currentTimeMillis());
        //            }
        //        }).start();
        //                routingTemplate.send("topic_demo", "two");
        //        routingTemplate.send("router", new byte[]{1, 2, 3});
    }
}
