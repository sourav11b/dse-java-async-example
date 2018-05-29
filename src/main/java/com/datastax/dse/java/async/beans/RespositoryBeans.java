package com.datastax.dse.java.async.beans;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.DseSession;
import com.datastax.dse.java.async.repositories.SimpleRepository;
import com.google.common.util.concurrent.ListenableFuture;

@Component
public class RespositoryBeans {
	
	@Autowired
	DseCluster cluster;

	@Autowired
	DseSession session;
	
	@Bean
	public SimpleRepository simpleRepository() {
		return new SimpleRepository();
	}
	
	
	@Bean
	public PreparedStatement simpleInsertPS() {
		return session.prepare("insert into java_sample.simple_table(id,name, description) values (?,?, ?)");

	}

	@Bean
	public PreparedStatement simpleSelectByPKPS() {
		return session.prepare("select * from java_sample.simple_table where id=?");

	}



}
