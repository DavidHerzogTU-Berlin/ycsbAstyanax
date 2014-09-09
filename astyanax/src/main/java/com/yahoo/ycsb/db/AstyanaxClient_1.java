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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ForwardingListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.netflix.astyanax.connectionpool.exceptions.OperationException;

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
	public static final String NODE_DISCOVERY_PROPERTY_DEFAULT = "TOKEN_AWARE";

	public static final String CONNECTION_POOL_PROPERTY = "connectionPoolType";
	public static final String CONNECTION_POOL_PROPERTY_DEFAULT = "TOKEN_AWARE";

	public static final String SEED_PROPERTY = "seeds";
	public static final String SEED_PROPERTY_DEFAULT = "127.0.0.1:9160,127.0.0.2:9160,127.0.0.3:9160";

	public static final String MAXCONS_PROPERTY = "maxCons";
	public static final String MAXCONS_PROPERTY_DEFAULT = "100";

	public static final String PORT_PROPERTY = "port";
	public static final String PORT_PROPERTY_DEFAULT = "9160";
	private static AstyanaxContext<Keyspace> context;
	private static Keyspace keyspace;
	private static Object lock = new Object();
	private static boolean needTotSetInit = true;
	static final ListeningExecutorService pool = MoreExecutors
			.listeningDecorator(Executors.newCachedThreadPool());

	public void init() throws DBException {
		syncInit();

	}

	public void syncInit() {
		synchronized (lock) {

			needTotSetInit = false;
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
																			WRITE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT))))
					.withConnectionPoolConfiguration(
							new ConnectionPoolConfigurationImpl(
									"MyConnectionPool")
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
									.setLatencyScoreStrategy(
											new EmaLatencyScoreContinuousStrategyImpl())
									.setHostSelectorStrategy(
											HostSelectorStrategy
													.valueOf("LEAST_OUTSTANDING")))
					.withAstyanaxConfiguration(
							new AstyanaxConfigurationImpl().setCqlVersion(
									"3.1.0").setTargetCassandraVersion("2.0"))
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
			final HashMap<String, ByteIterator> result2 = new HashMap<String, ByteIterator>();
			final String key1 = key;
			final Set<String> fields1 = fields;
			if (fields == null) {

				final ListenableFuture<OperationResult<ColumnList<String>>> opresult = keyspace
						.prepareQuery(EMP_CF).getKey(key).executeAsync();
				Futures.addCallback(
						opresult,
						new FutureCallback<OperationResult<ColumnList<String>>>() {

							@Override
							public void onFailure(Throwable e) {
								System.out.println("onFailure fields null");
							}

							@Override
							public void onSuccess(
									OperationResult<ColumnList<String>> oResult) {
								System.out
										.println("SUCCESS yeah baby#########");
								ColumnList<String> columns = oResult
										.getResult();
								for (String s : columns.getColumnNames()) {
									result2.put(s, new StringByteIterator(
											columns.getColumnByName(s)
													.getStringValue()));
								}

							}

						});
				opresult.get();
				result = result2;

			} else {
				final ListenableFuture<OperationResult<ColumnList<String>>> opresult = keyspace
						.prepareQuery(EMP_CF).getKey(key1)
						.withColumnSlice(fields1).executeAsync();
				System.out.println("before future thread id "
						+ Thread.currentThread().getId());
				Futures.addCallback(
						opresult,
						new FutureCallback<OperationResult<ColumnList<String>>>() {

							@Override
							public void onFailure(Throwable e) {
								System.out.println("onFailure fields null");
							}

							@Override
							public void onSuccess(
									OperationResult<ColumnList<String>> oResult) {
								System.out
										.println("SUCCESS yeah baby#########");
								ColumnList<String> columns = oResult
										.getResult();
								for (String s : columns.getColumnNames()) {
									result2.put(s, new StringByteIterator(
											columns.getColumnByName(s)
													.getStringValue()));
								}

							}

						});
				opresult.get();
				result = result2;
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
