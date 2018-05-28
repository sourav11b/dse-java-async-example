package com.datastax.dse.java.async.beans;

import org.springframework.context.annotation.Bean;

import com.datastax.dse.java.async.repositories.SimpleRepository;

public class RespositoryBeans {
	
	@Bean
	public SimpleRepository simpleRepository() {
		return new SimpleRepository();
	}

}
