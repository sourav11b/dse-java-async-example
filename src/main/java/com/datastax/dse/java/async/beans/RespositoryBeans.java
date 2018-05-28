package com.datastax.dse.java.async.beans;

import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.datastax.dse.java.async.repositories.SimpleRepository;

@Component
public class RespositoryBeans {
	
	@Bean
	public SimpleRepository simpleRepository() {
		return new SimpleRepository();
	}
	



}
