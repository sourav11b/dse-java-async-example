package com.datastax.dse.java.async.beans;

import java.util.Iterator;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.QueryTrace;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.QueryTrace.Event;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.DseSession;

@Component
public class DSEBeans {

	@Value("${dse.seeds}")
	private String[] dseSeeds;
	


	@Bean
	public DseCluster cluster() {

		PoolingOptions poolingOptions = new PoolingOptions()
				.setCoreConnectionsPerHost(HostDistance.LOCAL, 1)
				.setMaxConnectionsPerHost(HostDistance.LOCAL, 1)
				.setCoreConnectionsPerHost(HostDistance.REMOTE, 1)
				.setMaxConnectionsPerHost(HostDistance.REMOTE, 1)
				.setHeartbeatIntervalSeconds(60)
				.setMaxRequestsPerConnection(HostDistance.LOCAL, 20000)
				.setMaxRequestsPerConnection(HostDistance.REMOTE, 2000);
		;

		// refer to
		// https://docs.datastax.com/en/developer/java-driver-dse/1.6/manual/pooling/
		// for best practises and tuning

		SocketOptions so = new SocketOptions().setReadTimeoutMillis(3000).setConnectTimeoutMillis(3000);

			
		//set default consistency to LOCAL_ONE and explicitly setting metadata enabled for  token awareness to work
		
		QueryOptions qo = new QueryOptions().setConsistencyLevel(ConsistencyLevel.LOCAL_ONE).setMetadataEnabled(true);

		 return DseCluster.builder().addContactPoints(dseSeeds)
				.withLoadBalancingPolicy(
						new TokenAwarePolicy(
								DCAwareRoundRobinPolicy
								.builder()
								//.withLocalDc("myLocalDC")
		                        //.withUsedHostsPerRemoteDc(2)
		                        //.allowRemoteDCsForLocalConsistencyLevel()
		                        .build()
		                        )
						)
				.withPoolingOptions(poolingOptions)
				// .withAuthProvider(authProvider)
				// .withSSL()
				// .withSocketOptions(so)
				.withQueryOptions(qo)

				.build();

	}

	@Bean
	public DseSession session() {
        System.out.println("Creating session bean");
        DseSession session = cluster().connect();
		System.out.println("Done creating session bean  : "+session);
		return session;

	}
	

}
