package com.datastax.dse.java.async.controller;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.async.DeferredResult;

import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BatchStatement.Type;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.utils.UUIDs;
import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.auth.DsePlainTextAuthProvider;
import com.datastax.dse.java.async.model.SimpleTable;
import com.datastax.dse.java.async.model.SimpleTables;
import com.datastax.dse.java.async.repositories.SimpleRepository;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import com.fasterxml.jackson.databind.ObjectMapper;

@RestController
public class AsyncController {

	
	
	@Autowired
	SimpleRepository simpleRepository;

	private static final String template = "Hello, %s!";
	private final AtomicLong counter = new AtomicLong();

	@RequestMapping("/select")
	public DeferredResult<ResponseEntity<?>> select(@RequestParam("query") String query) {

		System.out.println("Query " + query);
		final List<String> results = new ArrayList<String>();

		final DeferredResult<ResponseEntity<?>> deferredResult = new DeferredResult<ResponseEntity<?>>(5000l);
		deferredResult.onTimeout(new Runnable() {

			public void run() { // Retry on timeout
				deferredResult.setErrorResult(
						ResponseEntity.status(HttpStatus.REQUEST_TIMEOUT).body("Request timeout occurred."));
			}
		});
		DseCluster cluster = null;
		// PoolingOptions poolingOptions = new PoolingOptions();

		// AuthProvider authProvider = new DsePlainTextAuthProvider("sourav11b",
		// "password");

		// set pooling options
		// Really should have multiple contact points, i.e.
		// cluster = DseCluster.builder().addContactPoints(new String[] {"127.0.0.1",
		// "127.0.0.2", "127.0.0.3"}).build();
		cluster = DseCluster.builder().addContactPoint("127.0.0.1")
				// .withLoadBalancingPolicy(new
				// TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().build()))
				// .withPoolingOptions(poolingOptions)
				// .withAuthProvider(authProvider)
				// .withSSL()
				.build();

		// you can also create and then add other things like threadpools, load balance
		// policys etc
		// cluster. =
		// DseCluster.builder().withLoadBalancingPolicy(policy).withPoolingOptions(options)...

		// you can get lots of meta data, the below shows the keyspaces it can find out
		// about
		// this is all part of the client gossip like query process
		// System.out.println("The keyspaces known by Connection are: " +
		// cluster.getMetadata().getKeyspaces().toString());

		// you don't have to specify a consistency level, there is always default
		// System.out.println("The Default Consistency Level is: "
		// + cluster.getConfiguration().getQueryOptions().getConsistencyLevel());

		// finally create a session to connect, alternatively and what you normally will
		// do is specify the keyspace
		// i.e. DseSession session = cluster.connect("keyspace_name");
		ListenableFuture<Session> session = cluster.connectAsync();

		ListenableFuture<ResultSet> resultSet = Futures.transformAsync(session,
				new AsyncFunction<Session, ResultSet>() {
					public ListenableFuture<ResultSet> apply(Session session) throws Exception {
						System.out.println(Thread.currentThread().getName() + "fetching");
						Statement statement = new SimpleStatement(query).setFetchSize(20);

						return session.executeAsync(statement);
					}
				});

		// Futures.addCallback(resultSet, new FutureCallback<ResultSet>() {
		// public void onSuccess(ResultSet resultSet) {
		// System.out.println(" start printing");
		//
		// resultSet.forEach(row
		// ->{System.out.println("print");System.out.println(row);} );
		// }
		//
		// public void onFailure(Throwable t) {
		// System.out.printf("Failed to retrieve the version: %s%n",
		// t.getMessage());
		// }
		// });

		Futures.transformAsync(resultSet, iterate(10, deferredResult, results), Executors.newCachedThreadPool());
		System.out.println(Thread.currentThread().getName() + "Done with main thread");

		return deferredResult;
	}

	@RequestMapping(value = "/insertOne", method = RequestMethod.POST)
	public DeferredResult<ResponseEntity<?>> insertOne(@RequestBody final SimpleTable row) {

		System.out.println("received request :" + row);
		final DeferredResult<ResponseEntity<?>> deferredResult = new DeferredResult<ResponseEntity<?>>(5000l);

		deferredResult.onTimeout(new Runnable() {

			public void run() { // Retry on timeout
				deferredResult.setErrorResult(
						ResponseEntity.status(HttpStatus.REQUEST_TIMEOUT).body("Request timeout occurred."));
			}
		});
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
						System.out.println(Thread.currentThread().getName() + "preparing statement");
						
						ListenableFuture<ResultSet>  id = session.executeAsync(statement.bind(UUIDs.random(),row.getName(),row.getDescription()));
						deferredResult.setResult(ResponseEntity.ok(id.get()));

						return null;
					}
				});
				
			

				return null;

			}
		});

		return deferredResult;
	}
	
	
	@RequestMapping(value = "/insertMany", method = RequestMethod.POST)
	public DeferredResult<ResponseEntity<?>> insertMany(@RequestBody final SimpleTables rows) {

		System.out.println("received request :" + rows);
		 DeferredResult<ResponseEntity<?>> deferredResult = new DeferredResult<ResponseEntity<?>>(5000l);

		deferredResult.onTimeout(new Runnable() {

			public void run() { // Retry on timeout
				deferredResult.setErrorResult(
						ResponseEntity.status(HttpStatus.REQUEST_TIMEOUT).body("Request timeout occurred."));
			}
		});
		simpleRepository.insertMany(deferredResult, rows);
		return deferredResult;
	}

	private static AsyncFunction<ResultSet, ResultSet> iterate(final int page,
			final DeferredResult<ResponseEntity<?>> deferredResult, final List<String> results) {
		return new AsyncFunction<ResultSet, ResultSet>() {
			@Override
			public ListenableFuture<ResultSet> apply(ResultSet rs) throws Exception {

				// How far we can go without triggering the blocking fetch:
				int remainingInPage = rs.getAvailableWithoutFetching();

				System.out.printf(Thread.currentThread().getName() + "Starting page %d (%d rows)%n", page,
						remainingInPage);

				for (Row row : rs) {
					results.add(row.toString());
					// System.out.printf(Thread.currentThread().getName() + "[page %d - %d] row =
					// %s%n", page,
					// remainingInPage, row);
					if (--remainingInPage == 0)
						break;
				}
				System.out.printf(Thread.currentThread().getName() + "Done page %d%n", page);

				boolean wasLastPage = rs.getExecutionInfo().getPagingState() == null;
				if (wasLastPage) {
					System.out.println(Thread.currentThread().getName() + "Done iterating");
					deferredResult.setResult(ResponseEntity.ok(results));

					return Futures.immediateFuture(rs);
				} else {
					ListenableFuture<ResultSet> future = rs.fetchMoreResults();
					return Futures.transformAsync(future, iterate(page + 1, deferredResult, results),
							Executors.newCachedThreadPool());
				}
			}
		};
	}
}
