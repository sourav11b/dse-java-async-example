package com.datastax.dse.java.async.beans;

import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.DseSession;
import com.datastax.driver.dse.auth.DsePlainTextAuthProvider;
import com.google.common.util.concurrent.ListenableFuture;

@Component
public class DSEBeans {

	@Bean
	public DseCluster cluster() {

		PoolingOptions poolingOptions = new PoolingOptions().setConnectionsPerHost(HostDistance.LOCAL, 1, 2)
				.setMaxRequestsPerConnection(HostDistance.LOCAL, 32768)
				.setCoreConnectionsPerHost(HostDistance.LOCAL, 2);

		// AuthProvider authProvider = new DsePlainTextAuthProvider("sourav11b",
		// "password");

		// set pooling options
		// Really should have multiple contact points, i.e.
		// cluster = DseCluster.builder().addContactPoints(new String[] {"127.0.0.1",
		// "127.0.0.2", "127.0.0.3"}).build();
		return DseCluster.builder().addContactPoint("127.0.0.1")
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

	}

	@Bean
	public DseSession session() {

		// you can also create and then add other things like threadpools, load balance
		// policys etc
		// cluster. =
		// DseCluster.builder().withLoadBalancingPolicy(policy).withPoolingOptions(options)...

		// you can get lots of meta data, the below shows the keyspaces it can find out
		// about
		// this is all part of the client gossip like query process

		System.out.println("The pooling options: "
				+ cluster().getConfiguration().getPoolingOptions().getMaxConnectionsPerHost(HostDistance.LOCAL));

		System.out
				.println("The keyspaces known by Connection are: " + cluster().getMetadata().getKeyspaces().toString());

		// you don't have to specify a consistency level, there is always default
		System.out.println("The Default Consistency Level is: "
				+ cluster().getConfiguration().getQueryOptions().getConsistencyLevel());

		// finally create a session to connect, alternatively and what you normally will
		// do is specify the keyspace
		// i.e. DseSession session = cluster.connect("keyspace_name");
		DseSession session = cluster().connect();
		return session;

	}

}
