package com.example.learnkafka.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.hibernate.validator.constraints.Length;

/**
 * author        yiliyang
 * date          2023-03-25
 * time          下午3:02
 * version       1.0
 * since         1.0
 */
@Data
@AllArgsConstructor
public class MessaegModel {
//    @Length(max = 5)
    private String message;

}
