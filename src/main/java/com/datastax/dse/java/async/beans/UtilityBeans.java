package com.datastax.dse.java.async.beans;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
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
	
	@Value("${application.thread.pool.core.size}")
	private int threadPoolCoreSize;
	@Value("${application.thread.pool.max.size}")
	private int threadPoolMaxSize;
	@Value("${application.thread.pool.queue.size}")
	private int queueSize;
	
	@Bean
	public LoggingAspects loggingAspects() {
		return new LoggingAspects();
	}
	
	@Bean
	public Mapper<SimpleTable> mapper() {
		return new MappingManager(session).mapper(SimpleTable.class);
	}
	
	@Bean
	public ExecutorService executorService() {
	//	return Executors.newFixedThreadPool(80);
		 return new ThreadPoolExecutor(threadPoolCoreSize, threadPoolMaxSize,
                 0L, TimeUnit.MILLISECONDS,
                 new LinkedBlockingQueue<Runnable>());
		 
	}

	

}
