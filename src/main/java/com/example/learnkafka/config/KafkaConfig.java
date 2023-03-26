package com.example.learnkafka.config;

import jakarta.annotation.Resource;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.*;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.transaction.KafkaAwareTransactionManager;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiConsumer;
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
        //当partitions大于1,才能多线程消费
        return new KafkaAdmin.NewTopics(TopicBuilder.name("topic_demo").partitions(2)
                                                //                                                .replicas(1)
                                                //消息时间由服务端生成
                                                .config(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, TimestampType.LOG_APPEND_TIME.name)
                                                .build());
    }

    @Bean
    public ProducerFactory<Object, Object> producerFactory(SomeBean someBean) {
        var factory = new DefaultKafkaProducerFactory<>(producerConfigs(someBean));
        //开启事务
        //        factory.setTransactionIdPrefix("t_yu");

        //默认情况系一个factory对应一个producer,如果这个设置打开,就会存在ThreadLocal中,那么一个线程就有一个producer
        factory.setProducerPerThread(true);
        factory.addListener(new ProducerFactory.Listener<>() {
            @Override
            public void producerAdded(String id, Producer<Object, Object> producer) {
                //监听producer生成
                System.out.println("producerAdded " + id);
            }

            @Override
            public void producerRemoved(String id, Producer<Object, Object> producer) {
                ProducerFactory.Listener.super.producerRemoved(id, producer);
            }
        });
        return factory;
    }

    @Bean(name = "kafkaTemplate")
    public KafkaTemplate<Object, Object> kafkaTemplate(ProducerFactory<Object, Object> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    //    @Bean(name = "ktm")
    //    public KafkaTransactionManager<Object, Object> kafkaTransactionManager() {
    //        return new KafkaTransactionManager<>(producerFactory());
    //    }

    @Bean
    public ConsumerFactory<String, Object> consumerFactory(SomeBean someBean) {
        ConsumerFactory<String, Object> consumerFactory = new DefaultKafkaConsumerFactory<>(consumerConfigs(someBean));
        consumerFactory.addListener(new ConsumerFactory.Listener<>() {

            @Override
            public void consumerAdded(String id, Consumer<String, Object> consumer) {
                //                ConsumerFactory.Listener.super.consumerAdded(id, consumer);
                System.out.println("consumer add " + id);
            }
        });
        return consumerFactory;
    }

    @Bean
    public ConsumerFactory<String, Object> routeConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(routeConsumerConfigs());
    }

    @Bean(name = "mKafkaListenerContainerFactory")
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, Object>> kafkaListenerContainerFactory(ConsumerFactory<String, Object> consumerFactory,
                                                                                                                           KafkaTemplate<Object, Object> kafkaTemplate) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        //这个为true,那么consumer的消息就不会一个一个的回调,可以用一个List来接收Message
        factory.setBatchListener(true);
//        factory.setCommonErrorHandler(new DefaultErrorHandler(new DeadLetterPublishingRecoverer(kafkaTemplate, (rec, ex) -> {
//            Header retries = rec.headers().lastHeader("retries");
//            if (retries == null) {
//                retries = new RecordHeader("retries", new byte[]{1});
//                rec.headers().add(retries);
//            } else {
//                retries.value()[0]++;
//            }
//            return retries.value()[0] > 2 ? new TopicPartition(rec.topic() + ".DLT", rec.partition()) : new TopicPartition(rec.topic(), rec.partition());
//        }), new FixedBackOff(1000L, 0)));

        factory.setCommonErrorHandler(new DefaultErrorHandler(new DeadLetterPublishingRecoverer(kafkaTemplate),//
                                                              new FixedBackOff(1000L, 2)));
        //设置提交ackMode
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.BATCH);
        return factory;
    }

    /**
     * Validation验证失败回调
     */
    @Bean
    public KafkaListenerErrorHandler validationErrorHandler() {
        return (m, e) -> {
            System.out.println("catch error " + e);
            return "default";
        };
    }

    //    @Bean
    //    public DefaultAfterRollbackProcessor errorHandler(BiConsumer<ConsumerRecord<?, ?>, Exception> recoverer) {
    //        DefaultAfterRollbackProcessor processor = new DefaultAfterRollbackProcessor(recoverer);
    //        return processor;
    //    }

    @Bean(name = "routeKafkaListenerContainerFactory")
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, Object>> routeKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(routeConsumerFactory());
        return factory;
    }

    @Bean
    public Map<String, Object> producerConfigs(SomeBean someBean) {
        Map<String, Object> props = new HashMap<>();
        //自定义Partition选择器
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.example.learnkafka.config.MyPartitioner");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "producer-yu");
        //注入bean,可以在拦截器使用
        props.put("some.bean", someBean);
        //拦截器
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, MyProducerInterceptor.class.getName());
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return props;
    }

    @Bean
    public Map<String, Object> consumerConfigs(SomeBean someBean) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer-yu");
        //每次拉取消息数
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        props.put("some.bean", someBean);
        props.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, MyConsumerInterceptor.class.getName());
        //关闭自动提交offset
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
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
