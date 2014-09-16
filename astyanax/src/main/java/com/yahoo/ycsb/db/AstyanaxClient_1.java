package com.yahoo.ycsb.db;

import static com.netflix.astyanax.examples.ModelConstants.*;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.*;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.netflix.astyanax.query.IndexQuery;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.model.Rows;
import java.net.InetAddress;
import com.yahoo.ycsb.*;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.nio.ByteBuffer;
import java.util.Vector;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Properties;
import java.io.UnsupportedEncodingException;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import com.netflix.astyanax.connectionpool.impl.PendingRequestMap;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ForwardingListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.netflix.astyanax.connectionpool.exceptions.OperationException;
import com.netflix.astyanax.connectionpool.Host;
public class AstyanaxClient_1 extends DB {
	public static final int Ok = 0;
	public static final int Error = -1;

	private ColumnFamily<String, String> EMP_CF;
	private static final String EMP_CF_NAME = "data";

	public static final String READ_CONSISTENCY_LEVEL_PROPERTY = "cassandra.readconsistencylevel";
	public static final String READ_CONSISTENCY_LEVEL_PROPERTY_DEFAULT = "ONE";

	public static final String WRITE_CONSISTENCY_LEVEL_PROPERTY = "cassandra.writeconsistencylevel";
	public static final String WRITE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT = "ONE";

	public static final String NODE_DISCOVERY_PROPERTY = "discoveryType";
	public static final String NODE_DISCOVERY_PROPERTY_DEFAULT = "RING_DESCRIBE";

	public static final String CONNECTION_POOL_PROPERTY = "connectionPoolType";
	public static final String CONNECTION_POOL_PROPERTY_DEFAULT = "TOKEN_AWARE";

	public static final String SEED_PROPERTY = "seeds";
	public static final String SEED_PROPERTY_DEFAULT = "127.0.0.1:9160,127.0.0.2:9160,127.0.0.3:9160";

	public static final String MAXCONS_PROPERTY = "maxCons";
	public static final String MAXCONS_PROPERTY_DEFAULT = "100";

	public static final String PORT_PROPERTY = "port";
	public static final String PORT_PROPERTY_DEFAULT = "9160";
	
	public static final String HOST_SELECTOR_STRATEGY = "hostSelectorStrategy";
	public static final String HOST_SELECTOR_STRATEGY_DEFAULT = "ROUND_ROBIN";
	
	public static final String SCORE_STRATEGY = "scoreStrategy";
	public static final String SCORE_STRATEGY_DEFAULT = "continuous";
	
	public static final String MAP_SIZE = "map_size";
	public static final String MAP_SIZE_DEFAULT = "crash";

	private static AstyanaxContext<Keyspace> context;
	private static Keyspace keyspace;
	private static Object lock = new Object();
	private static boolean needTotSetInit = true;
	private String latencyScoreStrategy;
	static final ListeningExecutorService pool = MoreExecutors
			.listeningDecorator(Executors.newCachedThreadPool());

	public void init() throws DBException {
		String map_size_String = getProperties().getProperty(MAP_SIZE, MAP_SIZE_DEFAULT);
		assert (!map_size_String.equals("crash"));
		Double map_size = Double.valueOf(map_size_String);
		assert (map_size >= 0);
		PendingRequestMap.setMap_size(map_size);
		syncInit();

	}

	public void syncInit() {
		synchronized (lock) {

			needTotSetInit = false;
			ConnectionPoolConfigurationImpl connectionPoolConfig = new ConnectionPoolConfigurationImpl(getProperties()
					.getProperty(SCORE_STRATEGY, SCORE_STRATEGY_DEFAULT));

			latencyScoreStrategy = getProperties().getProperty(SCORE_STRATEGY, SCORE_STRATEGY_DEFAULT);
			if(latencyScoreStrategy.equals("continuous")) {
				connectionPoolConfig.setLatencyScoreStrategy(new EmaLatencyScoreContinuousStrategyImpl());
			} else {
				if(latencyScoreStrategy.equals("ema"))
					connectionPoolConfig.setLatencyScoreStrategy(new EmaLatencyScoreStrategyImpl(100));
				else
					connectionPoolConfig.setLatencyScoreStrategy(new SmaLatencyScoreStrategyImpl());
			}
			context = new AstyanaxContext.Builder()
					.forCluster("Test Cluster")
					.forKeyspace("usertable")
					.withAstyanaxConfiguration(
							new AstyanaxConfigurationImpl()
									.setDiscoveryType(
											NodeDiscoveryType
													.valueOf(getProperties()
															.getProperty(
																	NODE_DISCOVERY_PROPERTY,
																	NODE_DISCOVERY_PROPERTY_DEFAULT)))
									.setConnectionPoolType(
											ConnectionPoolType
													.valueOf(getProperties()
															.getProperty(
																	CONNECTION_POOL_PROPERTY,
																	CONNECTION_POOL_PROPERTY_DEFAULT)))
									.setDefaultReadConsistencyLevel(
											ConsistencyLevel
													.valueOf("CL_"
															+ getProperties()
																	.getProperty(
																			READ_CONSISTENCY_LEVEL_PROPERTY,
																			READ_CONSISTENCY_LEVEL_PROPERTY_DEFAULT)))
									.setDefaultWriteConsistencyLevel(
											ConsistencyLevel
													.valueOf("CL_"
															+ getProperties()
																	.getProperty(
																			WRITE_CONSISTENCY_LEVEL_PROPERTY,
																			WRITE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT)))
									.setCqlVersion("3.1.0").setTargetCassandraVersion("2.0"))
					.withConnectionPoolConfiguration(
							connectionPoolConfig
									.setPort(
											Integer.valueOf(getProperties()
													.getProperty(PORT_PROPERTY,
															PORT_PROPERTY_DEFAULT)))
									.setMaxConnsPerHost(
											Integer.valueOf(getProperties()
													.getProperty(
															MAXCONS_PROPERTY,
															MAXCONS_PROPERTY_DEFAULT)))
									.setSeeds(
											getProperties().getProperty(
													SEED_PROPERTY,
													SEED_PROPERTY_DEFAULT))
									.setHostSelectorStrategy(
											HostSelectorStrategy
													.valueOf(getProperties().getProperty(HOST_SELECTOR_STRATEGY, HOST_SELECTOR_STRATEGY_DEFAULT))))
					.withConnectionPoolMonitor(
							new CountingConnectionPoolMonitor())
					.buildKeyspace(ThriftFamilyFactory.getInstance());
			
			context.start();
			keyspace = context.getEntity();

			EMP_CF = ColumnFamily.newColumnFamily(EMP_CF_NAME,
					StringSerializer.get(), StringSerializer.get());

		}

	}

	/**
	 * Insert a record in the database. Any field/value pairs in the specified
	 * values HashMap will be written into the record with the specified record
	 * key.
	 * 
	 * @param table
	 *            The name of the table
	 * @param key
	 *            The record key of the record to insert.
	 * @param values
	 *            A HashMap of field/value pairs to insert in the record
	 * @return Zero on success, a non-zero error code on error
	 */
	public int insert(String table, String key,
			HashMap<String, ByteIterator> values) {
		MutationBatch m = keyspace.prepareMutationBatch();
		try {
			for (Entry<String, ByteIterator> entry : values.entrySet()) {
				m.withRow(EMP_CF, key)
						.putColumn(entry.getKey(), entry.getValue().toString(),
								null).setTimestamp(System.currentTimeMillis());

			}

			OperationResult<Void> result = m.execute();

		} catch (ConnectionException e) {
			System.out.println(e);
			return Error;
		}
		return Ok;
	}

	/**
	 * Update a record in the database. Any field/value pairs in the specified
	 * values HashMap will be written into the record with the specified record
	 * key, overwriting any existing values with the same field name.
	 * 
	 * @param table
	 *            The name of the table
	 * @param key
	 *            The record key of the record to write.
	 * @param values
	 *            A HashMap of field/value pairs to update in the record
	 * @return Zero on success, a non-zero error code on error
	 */
	public int update(String table, String key,
			HashMap<String, ByteIterator> values) {
		return insert(table, key, values);
	}

	/**
	 * Read a record from the database. Each field/value pair from the result
	 * will be stored in a HashMap.
	 * 
	 * @param table
	 *            The name of the table
	 * @param key
	 *            The record key of the record to read.
	 * @param fields
	 *            The list of fields to read, or null for all of them
	 * @param result
	 *            A HashMap of field/value pairs for the result
	 * @return Zero on success, a non-zero error code on error
	 */
	public int read(String table, String key, Set<String> fields,
			HashMap<String, ByteIterator> result) {
		try {
			if (fields == null) {
				
				final OperationResult<ColumnList<String>> opresult = keyspace
						.prepareQuery(EMP_CF).getKey(key).execute();
				
				ColumnList<String> columns  = opresult.getResult();
				for (String s : columns.getColumnNames()) {
					result.put(s, new StringByteIterator(
						columns.getColumnByName(s)
						.getStringValue()));
				} 
				if(latencyScoreStrategy.equals("continuous")) {
					String ip = opresult.getHost().getIpAddress();
					double mu = 0;
					int qsz = 0;
					double latency = (double) opresult.getLatency();
					 if (columns.getColumnByName("MU") != null) {
						 mu = Double.valueOf(columns.getColumnByName("MU").getStringValue());
	                 }
	                 if (columns.getColumnByName("QSZ") != null) {
	                	 qsz = Integer.valueOf(columns.getColumnByName("QSZ").getStringValue());
	                 }
	                 PendingRequestMap.addSamples(ip, mu, qsz, latency);
				}
				
                 
			} else {
				
				final OperationResult<ColumnList<String>> opresult = keyspace
						.prepareQuery(EMP_CF).getKey(key)
						.withColumnSlice(fields).execute();
				
				ColumnList<String> columns  = opresult.getResult();
				for (String s : columns.getColumnNames()) {
					result.put(s, new StringByteIterator(
						columns.getColumnByName(s)
						.getStringValue()));
				}
				if(latencyScoreStrategy.equals("continuous")) {
					String ip = opresult.getHost().getIpAddress();
					double mu = 0;
					int qsz = 0;
					double latency = (double) opresult.getLatency();
					 if (columns.getColumnByName("MU") != null) {
						 mu = Double.valueOf(columns.getColumnByName("MU").getStringValue());
	                 }
	                 if (columns.getColumnByName("QSZ") != null) {
	                	 qsz = Integer.valueOf(columns.getColumnByName("QSZ").getStringValue());
	                 }
	                 PendingRequestMap.addSamples(ip, mu, qsz, latency);
				}
				
			}
			return Ok;

		} catch (Throwable e) {
			e.printStackTrace();
			return Error;
		}

	}

	/**
	 * Delete a record from the database.
	 * 
	 * @param table
	 *            The name of the table
	 * @param key
	 *            The record key of the record to delete.
	 * @return Zero on success, a non-zero error code on error
	 */
	public int delete(String table, String key) {
		try {
			MutationBatch m = keyspace.prepareMutationBatch();
			m.withRow(EMP_CF, key).setTimestamp(System.currentTimeMillis())
					.delete();
			m.execute();
			return Ok;
		} catch (Exception e) {
			System.out.println(e);
			return Error;

		}

	}

	/**
	 * Perform a range scan for a set of records in the database. Each
	 * field/value pair from the result will be stored in a HashMap.
	 * 
	 * @param table
	 *            The name of the table
	 * @param startkey
	 *            The record key of the first record to read.
	 * @param recordcount
	 *            The number of records to read
	 * @param fields
	 *            The list of fields to read, or null for all of them
	 * @param result
	 *            A Vector of HashMaps, where each HashMap is a set field/value
	 *            pairs for one record
	 * @return Zero on success, a non-zero error code on error. See this class's
	 *         description for a discussion of error codes.
	 */
	public int scan(String table, String startkey, int recordcount,
			Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
		throw new UnsupportedOperationException(
				"Scan method is not implemented.");

	}

}
