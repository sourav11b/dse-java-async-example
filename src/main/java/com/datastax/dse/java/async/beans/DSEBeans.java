package com.datastax.dse.java.async.beans;

import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.datastax.driver.dse.DseCluster;
@Component
public class DSEBeans {
	

	@Bean
	public DseCluster cluster() {
		
		// PoolingOptions poolingOptions = new PoolingOptions();

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

}
