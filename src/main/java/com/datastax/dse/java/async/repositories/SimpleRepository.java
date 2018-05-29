package com.datastax.dse.java.async.repositories;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.context.request.async.DeferredResult;

import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BatchStatement.Type;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.QueryTrace;
import com.datastax.driver.core.QueryTrace.Event;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
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

public class SimpleRepository {

	@Autowired
	DseCluster cluster;

	@Autowired
	DseSession session;

	@Autowired
	PreparedStatement simpleInsertPS;

	@Autowired
	PreparedStatement simpleSelectByPKPS;

	static Logger logger = LoggerFactory.getLogger(SimpleRepository.class);

	public void selectUsingQueryParam(DeferredResult<ResponseEntity<?>> deferredResult, String query) {

		logger.info("Starting select");

		final List<String> results = new ArrayList<String>();

		logger.info("Creating statement with fetch size");
		Statement statement = new SimpleStatement(query).setFetchSize(20);

		ResultSetFuture resultSet = session.executeAsync(statement);

		logger.info("Adding callback to record exception in response");

		Futures.addCallback(resultSet, new FutureCallback<ResultSet>() {
			public void onSuccess(ResultSet resultSet) {

			}

			public void onFailure(Throwable t) {
				logger.error("Error during processing resultSet", t);

				deferredResult.setResult(ResponseEntity.ok(t.getMessage()));

			}
		});

		Futures.transformAsync(resultSet, iterate(10, deferredResult, results), Executors.newCachedThreadPool());
		logger.info("Done with main thread");

	}

	@Async
	public void syncSelectUsingQueryParam(DeferredResult<ResponseEntity<?>> deferredResult, String query) {
		final List<String> results = new ArrayList<String>();

		DseSession session = cluster.connect();
		ResultSet resultSet = session.execute(new SimpleStatement(query).setFetchSize(20));
		for (Row row : resultSet) {
			results.add(row.toString());

		}

		deferredResult.setResult(ResponseEntity.ok(results));

		System.out.println(Thread.currentThread().getName() + "Done with syncSelectUsingQueryParam");

	}

	public void selectWithPK(DeferredResult<ResponseEntity<?>> deferredResult, String pk) {

		final List<String> results = new ArrayList<String>();

		ListenableFuture<ResultSet> resultSet = session.executeAsync(simpleSelectByPKPS.bind(UUID.fromString(pk)));
		Futures.transformAsync(resultSet, iterate(10, deferredResult, results), Executors.newCachedThreadPool());

		Futures.addCallback(resultSet, new FutureCallback<ResultSet>() {
			@Override
			public void onSuccess(ResultSet result) {
				System.out.println("success");
			}

			@Override
			public void onFailure(Throwable t) {
				System.out.println("1" + t);
				deferredResult.setResult(ResponseEntity.ok(t.getMessage()));

			}
		}, Executors.newCachedThreadPool());

	}

	public void insertOne(DeferredResult<ResponseEntity<?>> deferredResult, SimpleTable row) {

		ListenableFuture<ResultSet> id = session
				.executeAsync(simpleInsertPS.bind(UUIDs.random(), row.getName(), row.getDescription()));

		Futures.addCallback(id, new FutureCallback<ResultSet>() {
			@Override
			public void onSuccess(ResultSet result) {
				deferredResult.setResult(ResponseEntity.ok("success"));

			}

			@Override
			public void onFailure(Throwable t) {
				deferredResult.setResult(ResponseEntity.ok(t.getMessage()));

			}
		}, Executors.newCachedThreadPool());

	}

	/*
	 * public void insertMany(DeferredResult<ResponseEntity<?>> deferredResult,
	 * SimpleTables rows) {
	 * 
	 * ListenableFuture<Session> session = cluster.connectAsync();
	 * 
	 * Futures.transformAsync(session, new AsyncFunction<Session, ResultSet>() {
	 * public ListenableFuture<ResultSet> apply(Session session) throws Exception {
	 * System.out.println(Thread.currentThread().getName() + "preparing statement");
	 * 
	 * ListenableFuture<PreparedStatement> prepared = session
	 * .prepareAsync("insert into java_sample.simple_table(id,name, description) values (?,?, ?)"
	 * );
	 * 
	 * Futures.transformAsync(prepared, new AsyncFunction<PreparedStatement,
	 * ResultSet>() { public ListenableFuture<ResultSet> apply(PreparedStatement
	 * statement) throws Exception { System.out.println(
	 * Thread.currentThread().getName() + "preparing statement" +
	 * statement.getPreparedId()); BatchStatement batch = new
	 * BatchStatement(Type.LOGGED);
	 * 
	 * for (SimpleTable simpleTable : rows.getRows()) {
	 * batch.add(statement.bind(UUIDs.random(), simpleTable.getName(),
	 * simpleTable.getDescription())); }
	 * 
	 * ListenableFuture<ResultSet> id = session.executeAsync(batch);
	 * 
	 * Futures.addCallback(id, new FutureCallback<ResultSet>() {
	 * 
	 * @Override public void onSuccess(ResultSet result) {
	 * System.out.println("success");
	 * deferredResult.setResult(ResponseEntity.ok("success")); }
	 * 
	 * @Override public void onFailure(Throwable t) { System.out.println("1" + t);
	 * deferredResult.setResult(ResponseEntity.ok(t.getStackTrace()));
	 * 
	 * } }, Executors.newCachedThreadPool());
	 * 
	 * return null; } });
	 * 
	 * Futures.addCallback(prepared, new FutureCallback<PreparedStatement>() {
	 * 
	 * @Override public void onSuccess(PreparedStatement result) {
	 * 
	 * }
	 * 
	 * @Override public void onFailure(Throwable t) { System.out.println("2" + t);
	 * deferredResult.setResult(ResponseEntity.ok(t.getStackTrace()));
	 * 
	 * } }, Executors.newCachedThreadPool());
	 * 
	 * return null;
	 * 
	 * } }); Futures.addCallback(session, new FutureCallback<Session>() {
	 * 
	 * @Override public void onSuccess(Session result) {
	 * 
	 * }
	 * 
	 * @Override public void onFailure(Throwable t) { System.out.println("3" + t);
	 * deferredResult.setResult(ResponseEntity.ok(t.getStackTrace()));
	 * 
	 * } }, Executors.newCachedThreadPool()); }
	 */
	public void insertMany(DeferredResult<ResponseEntity<?>> deferredResult, SimpleTables rows) {

		BatchStatement batch = new BatchStatement(Type.LOGGED);

		for (SimpleTable simpleTable : rows.getRows()) {
			batch.add(simpleInsertPS.bind(UUIDs.random(), simpleTable.getName(), simpleTable.getDescription()));
		}

		ListenableFuture<ResultSet> id = session.executeAsync(batch);

		Futures.addCallback(id, new FutureCallback<ResultSet>() {
			@Override
			public void onSuccess(ResultSet result) {
				System.out.println("success");
				deferredResult.setResult(ResponseEntity.ok("success"));
			}

			@Override
			public void onFailure(Throwable t) {
				logger.error("error during insert :", t);
				deferredResult.setResult(ResponseEntity.ok(t.getStackTrace()));

			}
		});
	}

	private static void createKeyspaceAndTables(DseSession session) {
		// this is using a simple statement, there are many other and better ways to
		// execute against the cluster
		// my personal preferred method is using mappers, but since this is not about
		// how to code my
		// examples are trying to use very simple methods
		Statement createKS = new SimpleStatement(
				"CREATE KEYSPACE IF NOT EXISTS java_sample WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
		createKS.enableTracing();

		// Note the consistency level, just uses the default for the cluster object if
		// not set on the statement
		System.out.println("The Consistency Level is: " + createKS.getConsistencyLevel());
		ResultSet rs = session.execute(createKS);
		ExecutionInfo executionInfo = rs.getExecutionInfo();

		QueryTrace trace = executionInfo.getQueryTrace();

		Iterator<Event> it = trace.getEvents().iterator();

		while (it.hasNext()) {
			System.out.println(it.next());
		}

		Statement createTable = new SimpleStatement(
				"CREATE TABLE If NOT EXISTS java_sample.simple_table ( id uuid, name text, description text, PRIMARY KEY(id)) ;");

		// now we change the CL, and it should show up as part of this execution
		createTable.setConsistencyLevel(ConsistencyLevel.ALL);
		System.out.println("The Consistency Level is: " + createTable.getConsistencyLevel());
		session.execute(createTable);
	}

	/*
	 * private static DseSession createConnection() { DseCluster cluster = null;
	 * PoolingOptions poolingOptions = new PoolingOptions();
	 * 
	 * AuthProvider authProvider = new DsePlainTextAuthProvider("sourav11b",
	 * "password");
	 * 
	 * // set pooling options // Really should have multiple contact points, i.e. //
	 * cluster = DseCluster.builder().addContactPoints(new String[] {"127.0.0.1", //
	 * "127.0.0.2", "127.0.0.3"}).build(); cluster =
	 * DseCluster.builder().addContactPoint("127.0.0.1")
	 * .withLoadBalancingPolicy(new
	 * TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().build())) //
	 * .withPoolingOptions(poolingOptions) .withAuthProvider(authProvider) //
	 * .withSSL() .build();
	 * 
	 * // you can also create and then add other things like threadpools, load
	 * balance // policys etc // cluster. = //
	 * DseCluster.builder().withLoadBalancingPolicy(policy).withPoolingOptions(
	 * options)...
	 * 
	 * // you can get lots of meta data, the below shows the keyspaces it can find
	 * out // about // this is all part of the client gossip like query process
	 * System.out.println("The keyspaces known by Connection are: " +
	 * cluster.getMetadata().getKeyspaces().toString());
	 * 
	 * // you don't have to specify a consistency level, there is always default
	 * System.out.println("The Default Consistency Level is: " +
	 * cluster.getConfiguration().getQueryOptions().getConsistencyLevel());
	 * 
	 * // finally create a session to connect, alternatively and what you normally
	 * will // do is specify the keyspace // i.e. DseSession session =
	 * cluster.connect("keyspace_name"); DseSession session = cluster.connect();
	 * return session;
	 * 
	 * }
	 */

	private static AsyncFunction<ResultSet, ResultSet> iterate(final int page,
			final DeferredResult<ResponseEntity<?>> deferredResult, final List<String> results) {
		return new AsyncFunction<ResultSet, ResultSet>() {
			@Override
			public ListenableFuture<ResultSet> apply(ResultSet rs) throws Exception {

				// How far we can go without triggering the blocking fetch:
				int remainingInPage = rs.getAvailableWithoutFetching();

				logger.info("Starting page {} ({} rows)", page, remainingInPage);

				for (Row row : rs) {
					results.add(row.toString());
					// System.out.printf(Thread.currentThread().getName() + "[page %d - %d] row =
					// %s%n", page,
					// remainingInPage, row);
					if (--remainingInPage == 0)
						break;
				}

				logger.info("Done page {}", page);

				boolean wasLastPage = rs.getExecutionInfo().getPagingState() == null;
				if (wasLastPage) {
					logger.info("Done through all pages");
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
