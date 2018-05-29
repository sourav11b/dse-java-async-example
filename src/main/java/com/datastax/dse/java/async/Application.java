package com.datastax.dse.java.async;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@ConfigurationProperties
@ComponentScan(basePackages = {"com.datastax.dse.java.async"})
@EnableAsync

public class Application {

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
}
