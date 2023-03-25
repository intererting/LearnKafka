package com.example.learnkafka.config;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * author        yiliyang
 * date          2023-03-24
 * time          下午5:45
 * version       1.0
 * since         1.0
 */
public class MyPartitioner implements Partitioner {

    @Override
    public void configure(Map<String, ?> configs) {

    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        int paritionCount = cluster.partitionCountForTopic(topic);
        return Math.abs(value.hashCode()) % paritionCount;
    }

    @Override
    public void close() {

    }

}
