package com.datastax.dse.java.async;

import org.aspectj.lang.annotation.Aspect;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@ConfigurationProperties
@ComponentScan(basePackages = {"com.datastax.dse.java.async"})
@EnableAsync
//@EnableAspectJAutoProxy



public class Application {

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
}
