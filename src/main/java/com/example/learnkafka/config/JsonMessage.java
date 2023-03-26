package com.example.learnkafka.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * author        yiliyang
 * date          2023-03-26
 * time          下午1:10
 * version       1.0
 * since         1.0
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class JsonMessage {

    private String name;

    private Integer age;


}
