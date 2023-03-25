package com.example.learnkafka.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * author        yiliyang
 * date          2023-03-25
 * time          下午8:42
 * version       1.0
 * since         1.0
 */
@Component
public class SomeBean {

    public SomeBean() {

    }

    public void someMethod(String what) {
        System.out.println(what + " in my foo bean");
    }

}
