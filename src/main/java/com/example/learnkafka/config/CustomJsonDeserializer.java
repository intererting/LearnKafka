package com.example.learnkafka.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.springframework.kafka.support.JacksonUtils;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.Map;

/**
 * author        yiliyang
 * date          2023-03-26
 * time          下午1:21
 * version       1.0
 * since         1.0
 */
public class CustomJsonDeserializer extends JsonDeserializer<Object> {

    public CustomJsonDeserializer() {
        super(customizedObjectMapper());
    }

    private static ObjectMapper customizedObjectMapper() {
        ObjectMapper mapper = JacksonUtils.enhancedObjectMapper();
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        return mapper;
    }

    @Override
    public synchronized void configure(Map<String, ?> configs, boolean isKey) {
        super.configure(configs, isKey);
    }
}