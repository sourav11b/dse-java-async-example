package com.datastax.dse.java.async.beans;

import java.util.Iterator;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
	@Value("${dse.pooling.host.distance.local.core}")
	private int localCoreConnectionsPerHost;
	@Value("${dse.pooling.host.distance.local.max}")
	private int localMaxConnectionsPerHost;
	@Value("${dse.pooling.host.distance.remote.core}")
	private int remoteCoreConnectionsPerHost;
	@Value("${dse.pooling.host.distance.remote.max}")
	private int remoteMaxConnectionsPerHost;
	@Value("${dse.pooling.heartbeat.interval.seconds}")
	private int defaultHeartBeatIntervalSeconds;
	@Value("${dse.pooling.host.distance.local.max.per.connection}")
	private int localMaxHostsPerConnection;
	@Value("${dse.pooling.host.distance.remote.max.per.connection}")
	private int remotMaxHostsPerConnection;
	@Value("${dse.socket.read.timeout.ms}")
	private int readTimeout;
	@Value("${dse.socket.connect.timeout.ms}")
	private int connectTimeout;
	@Value("${dse.defaults.consistency.level}")
	private String consistencyLevel;
	@Value("${dse.defaults.metadata.enabled}")
	private boolean metadataEnabled;

	static Logger logger = LoggerFactory.getLogger(DSEBeans.class);

	@Bean
	public DseCluster cluster() {

		PoolingOptions poolingOptions = new PoolingOptions()
				.setCoreConnectionsPerHost(HostDistance.LOCAL, localCoreConnectionsPerHost)
				.setMaxConnectionsPerHost(HostDistance.LOCAL, localMaxConnectionsPerHost)
				.setCoreConnectionsPerHost(HostDistance.REMOTE, remoteCoreConnectionsPerHost)
				.setMaxConnectionsPerHost(HostDistance.REMOTE, remoteMaxConnectionsPerHost)
				.setHeartbeatIntervalSeconds(defaultHeartBeatIntervalSeconds)
				.setMaxRequestsPerConnection(HostDistance.LOCAL, localMaxHostsPerConnection)
				.setMaxRequestsPerConnection(HostDistance.REMOTE, localMaxHostsPerConnection);
		;

		// refer to
		// https://docs.datastax.com/en/developer/java-driver-dse/1.6/manual/pooling/
		// for best practises and tuning

		SocketOptions so = new SocketOptions().setReadTimeoutMillis(readTimeout).setConnectTimeoutMillis(connectTimeout);

			
		//set default consistency to LOCAL_ONE and explicitly setting metadata enabled for  token awareness to work
		
		QueryOptions qo = new QueryOptions().setConsistencyLevel(ConsistencyLevel.valueOf(consistencyLevel)).setMetadataEnabled(metadataEnabled);

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
        logger.info("Creating session bean");
        DseSession session = cluster().connect();
        logger.info("Done creating session bean  : "+session);
		return session;

	}
	

}
