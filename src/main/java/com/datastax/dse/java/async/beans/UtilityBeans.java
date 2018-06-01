package com.datastax.dse.java.async.beans;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.datastax.driver.dse.DseSession;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.dse.java.aop.LoggingAspects;
import com.datastax.dse.java.async.model.SimpleTable;
@Component
public class UtilityBeans {
	
	@Autowired
	DseSession session;
	
	@Bean
	public LoggingAspects loggingAspects() {
		return new LoggingAspects();
	}
	
	@Bean
	public Mapper<SimpleTable> mapper() {
		return new MappingManager(session).mapper(SimpleTable.class);
	}

	

}
