package com.example.learnkafka.config;

import jakarta.validation.Valid;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.event.ListenerContainerIdleEvent;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.transaction.annotation.Transactional;

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
    //    @KafkaListener(topics = {"topic_demo"}, groupId = "topic_demo_group", concurrency = "2",//
    //            properties = {"auto.offset.reset=latest"}, containerFactory = "kafkaListenerContainerFactory")
    //    public void topicDemokafkaListener(ConsumerRecord<String, Object> message) {
    //        try {
    //            Thread.sleep(1000);
    //        } catch (InterruptedException e) {
    //            throw new RuntimeException(e);
    //        }
    //        System.out.println("receive " + Thread.currentThread());
    //    }
    @KafkaListener(topics = {"topic_demo"}, groupId = "topic_demo_group", concurrency = "2",//
            id = "my_id", properties = {"auto.offset.reset=latest"}, containerFactory = "kafkaListenerContainerFactory", errorHandler = "validationErrorHandler")
//    @Transactional("ktm")
    public void topicDemokafkaListener(@Payload @Valid List<MessaegModel> messaegModel) {
        System.out.println("receive " + Thread.currentThread() + "  " + messaegModel.size() + " " + messaegModel);
    }

    @EventListener(condition = "event.listenerId.startsWith('my_')")
    public void eventHandler(ListenerContainerIdleEvent event) {
        System.out.println("=======ListenerContainerIdleEvent===========");
    }

    //    Starting with version 2.0, if you also annotate a @KafkaListener with a @SendTo annotation and the method invocation returns a result,
    //    the result is forwarded to the topic specified by the @SendTo.
    //You can annotate a @KafkaListener method with @SendTo even if no result is returned.
    // This is to allow the configuration of an errorHandler that can forward information about a failed message delivery to some topic.
    @KafkaListener(topics = {"router"}, groupId = "router_group", containerFactory = "routeKafkaListenerContainerFactory")
    public void routerKafkaListener(ConsumerRecord<String, Object> message) {
        System.out.println("router " + Arrays.toString((byte[]) message.value()));
    }

}
