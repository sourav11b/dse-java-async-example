package com.datastax.dse.java.async.beans;

import java.util.Iterator;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.QueryTrace;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.QueryTrace.Event;
import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.DseSession;
import com.datastax.dse.java.async.controller.AsyncController;
import com.datastax.dse.java.async.repositories.SimpleRepository;
import com.google.common.util.concurrent.ListenableFuture;

@Component
public class RespositoryBeans {
	
	static Logger logger = LoggerFactory.getLogger(RespositoryBeans.class);

	


	@Autowired
	DseSession session;
	
	@PostConstruct
	public void createKeyspaceAndTables() {
		
		
		
		logger.info("Start creating keyspace and tables");


		Statement createKS = new SimpleStatement(
				"CREATE KEYSPACE IF NOT EXISTS java_sample WITH replication = {'class': 'org.apache.cassandra.locator.NetworkTopologyStrategy', 'data-store': '3', 'processing': '3'}").setConsistencyLevel(ConsistencyLevel.ALL);
		createKS.enableTracing();


		ResultSet rs = session.execute(createKS);
		ExecutionInfo executionInfo = rs.getExecutionInfo();

		QueryTrace trace = executionInfo.getQueryTrace();

		Iterator<Event> it = trace.getEvents().iterator();

		while (it.hasNext()) {
			logger.info(it.next().toString());
		}

		Statement createTable = new SimpleStatement(
				"CREATE TABLE If NOT EXISTS java_sample.simple_table ( id uuid, name text, description text, PRIMARY KEY(id)) ;").setConsistencyLevel(ConsistencyLevel.ALL);

		// now we change the CL, and it should show up as part of this execution
		createTable.setConsistencyLevel(ConsistencyLevel.ALL);
		session.execute(createTable);
		
		logger.info("Done creating keyspace and tables");
	}

	
	
	
	
	
	@Bean
	public SimpleRepository simpleRepository() {
		logger.info("creating SimpleRepository");

		return new SimpleRepository();
	}
	
	
	@Bean
	public PreparedStatement simpleInsertPS() {	
		logger.info("creating simpleInsertPS");

		
		
		return session.prepare("insert into java_sample.simple_table(id,name, description) values (?,?, ?)").setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);

	}

	@Bean
	public PreparedStatement simpleSelectByPKPS() {
		logger.info("creating simpleSelectByPKPS");
		
		return session.prepare("select * from java_sample.simple_table where id=?").setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);

	}
	
	
	@Bean
	public ListenableFuture<PreparedStatement> simpleInsertPSAsync() {	
		logger.info("creating simpleInsertPSAsync");

		return session.prepareAsync("insert into java_sample.simple_table(id,name, description) values (?,?, ?)");

	}
	
	@Bean
	public ListenableFuture<PreparedStatement> simpleSelectByPKPSAsync() {
		logger.info("creating simpleSelectByPKPSAsync");

		return session.prepareAsync("select * from java_sample.simple_table where id=?");

	}
	
	@Bean
	public PreparedStatement simpleSelectBySQPS() {
		logger.info("creating simpleSelectBySQPS");

		
		return session.prepare("select * from java_sample.simple_table where solr_query=?").setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);

	}
	


}
