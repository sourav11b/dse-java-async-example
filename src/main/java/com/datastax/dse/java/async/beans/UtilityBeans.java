package com.datastax.dse.java.async.beans;

import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.datastax.dse.java.aop.LoggingAspects;
@Component
public class UtilityBeans {
	
	@Bean
	public LoggingAspects loggingAspects() {
		return new LoggingAspects();
	}
	

}
