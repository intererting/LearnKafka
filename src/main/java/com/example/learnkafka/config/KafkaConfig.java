package com.example.learnkafka.config;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.*;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.transaction.KafkaAwareTransactionManager;
import org.springframework.kafka.transaction.KafkaTransactionManager;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * author        yiliyang
 * date          2023-03-20
 * time          下午4:06
 * version       1.0
 * since         1.0
 */
@Configuration
public class KafkaConfig {
    /**
     * 创建topic
     */
    @Bean
    public KafkaAdmin.NewTopics newTopics() {
        return new KafkaAdmin.NewTopics(TopicBuilder.name("topic_demo").partitions(2)
                                                //                                                .replicas(1)
                                                .config(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, TimestampType.LOG_APPEND_TIME.name)
                                                .build());
    }

    @Bean
    public ProducerFactory<Object, Object> producerFactory(SomeBean someBean) {
        var factory = new DefaultKafkaProducerFactory<>(producerConfigs(someBean));
        //        factory.setTransactionIdPrefix("t_yu");
        factory.setProducerPerThread(true);
        factory.addListener(new ProducerFactory.Listener<>() {
            @Override
            public void producerAdded(String id, Producer<Object, Object> producer) {
                System.out.println("producerAdded " + id);
            }

            @Override
            public void producerRemoved(String id, Producer<Object, Object> producer) {
                ProducerFactory.Listener.super.producerRemoved(id, producer);
            }
        });
        return factory;
    }

    //    @Bean(name = "ktm")
    //    public KafkaTransactionManager<Object, Object> kafkaTransactionManager() {
    //        return new KafkaTransactionManager<>(producerFactory());
    //    }

    @Bean
    public ConsumerFactory<String, Object> consumerFactory(SomeBean someBean) {
        ConsumerFactory<String, Object> consumerFactory = new DefaultKafkaConsumerFactory<>(consumerConfigs(someBean));
        return consumerFactory;
    }

    @Bean
    public ConsumerFactory<String, Object> routeConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(routeConsumerConfigs());
    }

    @Bean(name = "kafkaListenerContainerFactory")
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, Object>> kafkaListenerContainerFactory(SomeBean someBean) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory(someBean));
        factory.setBatchListener(true);
        //设置手动提交ackMode
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.BATCH);
        return factory;
    }

    @Bean
    public KafkaListenerErrorHandler validationErrorHandler() {
        return (m, e) -> {
            System.out.println("catch error " + e);
            return "default";
        };
    }

    @Bean(name = "routeKafkaListenerContainerFactory")
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, Object>> routeKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(routeConsumerFactory());
        return factory;
    }

    @Bean
    public Map<String, Object> producerConfigs(SomeBean someBean) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.example.learnkafka.config.MyPartitioner");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "producer-yu");
        props.put("some.bean", someBean);
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, MyProducerInterceptor.class.getName());
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return props;
    }

    @Bean
    public Map<String, Object> consumerConfigs(SomeBean someBean) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer-yu");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);
        props.put("some.bean", someBean);
        props.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, MyConsumerInterceptor.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return props;
    }

    @Bean
    public Map<String, Object> routeConsumerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer-yu");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        return props;
    }

    @Bean
    public KafkaTemplate<Object, Object> kafkaTemplate(SomeBean someBean) {
        return new KafkaTemplate<>(producerFactory(someBean));
    }

    @Bean
    public RoutingKafkaTemplate routingTemplate(GenericApplicationContext context, ProducerFactory<Object, Object> pf) {
        Map<String, Object> configs = new HashMap<>(pf.getConfigurationProperties());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        DefaultKafkaProducerFactory<Object, Object> bytesPF = new DefaultKafkaProducerFactory<>(configs);
        context.registerBean(DefaultKafkaProducerFactory.class, "bytesPF", bytesPF);

        Map<Pattern, ProducerFactory<Object, Object>> map = new LinkedHashMap<>();
        map.put(Pattern.compile("router"), bytesPF);
        map.put(Pattern.compile(".+"), pf); // Default PF with StringSerializer
        return new RoutingKafkaTemplate(map);
    }
}
