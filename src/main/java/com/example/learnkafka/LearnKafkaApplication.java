package com.example.learnkafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication
@EnableKafka
public class LearnKafkaApplication {
    public static void main(String[] args) {
        SpringApplication.run(LearnKafkaApplication.class, args);
    }

}
