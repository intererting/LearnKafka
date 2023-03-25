package com.example.learnkafka.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListenerConfigurer;
import org.springframework.kafka.config.KafkaListenerEndpointRegistrar;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;

/**
 * author        yiliyang
 * date          2023-03-25
 * time          下午2:56
 * version       1.0
 * since         1.0
 */
@Configuration
public class KafkaListenerConfigure implements KafkaListenerConfigurer {

    @Autowired
    private LocalValidatorFactoryBean validator;

    @Override
    public void configureKafkaListeners(KafkaListenerEndpointRegistrar registrar) {
        //        registrar.setValidator(new MyValidator());
        registrar.setValidator(this.validator);
    }

}
