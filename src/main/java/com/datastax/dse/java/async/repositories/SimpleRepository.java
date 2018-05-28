package com.datastax.dse.java.async.repositories;

import java.util.Iterator;
import java.util.concurrent.Executors;

import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.context.request.async.DeferredResult;

import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.QueryTrace;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.BatchStatement.Type;
import com.datastax.driver.core.QueryTrace.Event;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.core.utils.UUIDs;
import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.DseSession;
import com.datastax.driver.dse.auth.DsePlainTextAuthProvider;
import com.datastax.dse.java.async.model.SimpleTable;
import com.datastax.dse.java.async.model.SimpleTables;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;


@Service
public class SimpleRepository {
	
	
	public void insertMany(DeferredResult<ResponseEntity<?>> deferredResult,SimpleTables rows) {
		DseCluster cluster = null;

		cluster = DseCluster.builder().addContactPoint("127.0.0.1").build();

		ListenableFuture<Session> session = cluster.connectAsync();

		Futures.transformAsync(session, new AsyncFunction<Session, ResultSet>() {
			public ListenableFuture<ResultSet> apply(Session session) throws Exception {
				System.out.println(Thread.currentThread().getName() + "preparing statement");

				ListenableFuture<PreparedStatement> prepared = session
						.prepareAsync("insert into java_sample.simple_table(id,name, description) values (?,?, ?)");
			
					
				Futures.transformAsync(prepared, new AsyncFunction<PreparedStatement, ResultSet>() {
					public ListenableFuture<ResultSet> apply(PreparedStatement statement) throws Exception {
						System.out.println(Thread.currentThread().getName() + "preparing statement"+statement.getPreparedId());
						BatchStatement batch = new BatchStatement(Type.LOGGED);
								
						for (SimpleTable simpleTable : rows.getRows()) {
							batch.add(statement.bind(UUIDs.random(),simpleTable.getName(),simpleTable.getDescription()));
						}
						
						ListenableFuture<ResultSet>  id = session.executeAsync(batch);
						
						Futures.addCallback(id, new FutureCallback<ResultSet>() {
						    @Override
						    public void onSuccess(ResultSet result) {
						        System.out.println("success");
						        deferredResult.setResult(ResponseEntity.ok("success"));
						  }

						    @Override
						    public void onFailure(Throwable t) {
						        System.out.println("1"+t);
						        deferredResult.setResult(ResponseEntity.ok(t.getMessage()));

						  }
						}, Executors.newCachedThreadPool());
						
						

						return null;
					}
				});
				
				Futures.addCallback(prepared, new FutureCallback<PreparedStatement>() {
				    @Override
				    public void onSuccess(PreparedStatement result) {
				       
				  }

				    @Override
				    public void onFailure(Throwable t) {
				        System.out.println("2"+t);
				        deferredResult.setResult(ResponseEntity.ok(t.getMessage()));

				  }
				}, Executors.newCachedThreadPool());

				return null;

			}
		});
		Futures.addCallback(session, new FutureCallback<Session>() {
		    @Override
		    public void onSuccess(Session result) {
		       
		  }

		    @Override
		    public void onFailure(Throwable t) {
		        System.out.println("3"+t);
		        deferredResult.setResult(ResponseEntity.ok(t.getMessage()));

		  }
		}, Executors.newCachedThreadPool());
	}
	
	
	private static void createKeyspaceAndTables(DseSession session) {
		// this is using a simple statement,  there are many other and better ways to execute against the cluster
		// my personal preferred method is using mappers, but since this is not about how to code my
		// examples are trying to use very simple methods
		Statement createKS = new SimpleStatement(
				"CREATE KEYSPACE IF NOT EXISTS java_sample WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
		createKS.enableTracing();
		
		// Note the consistency level, just uses the default for the cluster object if not set on the statement
		System.out.println("The Consistency Level is: " + createKS.getConsistencyLevel());
		ResultSet rs = session.execute(createKS);
		ExecutionInfo executionInfo = rs.getExecutionInfo();
		
		QueryTrace trace = executionInfo.getQueryTrace();
		
		Iterator<Event> it = trace.getEvents().iterator();
		
		while(it.hasNext()) {
			System.out.println(it.next());
		}
		
		Statement createTable = new SimpleStatement(
				"CREATE TABLE If NOT EXISTS java_sample.simple_table ( id uuid, name text, description text, PRIMARY KEY(id)) ;");
		
		// now we change the CL, and it should show up as part of this execution
		createTable.setConsistencyLevel(ConsistencyLevel.ALL);
		System.out.println("The Consistency Level is: " + createTable.getConsistencyLevel());
		session.execute(createTable);
	}
	
	private static DseSession createConnection() {
		DseCluster cluster = null;
		PoolingOptions poolingOptions = new PoolingOptions();
		
		AuthProvider authProvider = new DsePlainTextAuthProvider(
			    "sourav11b",
			    "password");

		//set pooling options
		// Really should have multiple contact points, i.e. 
		// cluster = DseCluster.builder().addContactPoints(new String[] {"127.0.0.1", "127.0.0.2", "127.0.0.3"}).build();
		cluster = DseCluster.builder().addContactPoint("127.0.0.1")
				.withLoadBalancingPolicy(new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().build()))
			//	.withPoolingOptions(poolingOptions)
				.withAuthProvider(authProvider)
			//	.withSSL()
				.build();

		// you can also create and then add other things like threadpools, load balance policys etc
		// cluster. = DseCluster.builder().withLoadBalancingPolicy(policy).withPoolingOptions(options)...
		
		// you can get lots of meta data, the below shows the keyspaces it can find out about
		// this is all part of the client gossip like query process
		System.out.println("The keyspaces known by Connection are: " + cluster.getMetadata().getKeyspaces().toString());
		
		// you don't have to specify a consistency level, there is always default 
		System.out.println("The Default Consistency Level is: "
				+ cluster.getConfiguration().getQueryOptions().getConsistencyLevel());
		
		// finally create a session to connect,  alternatively and what you normally will do is specify the keyspace
		// i.e. DseSession session = cluster.connect("keyspace_name");
		DseSession session = cluster.connect();
		return session;

	}

}
