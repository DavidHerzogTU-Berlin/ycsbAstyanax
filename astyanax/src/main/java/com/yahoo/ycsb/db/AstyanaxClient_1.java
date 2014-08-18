package com.yahoo.ycsb.db;

import static com.netflix.astyanax.examples.ModelConstants.*;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
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

public class AstyanaxClient_1 extends DB{
	public static final int Ok = 0;
  	public static final int Error = -1;
	private AstyanaxContext<Keyspace> context;
	private Keyspace keyspace;
	private ColumnFamily<String, String> EMP_CF;
	private static final String EMP_CF_NAME = "data";

	public static final String READ_CONSISTENCY_LEVEL_PROPERTY = "cassandra.readconsistencylevel";
	public static final String READ_CONSISTENCY_LEVEL_PROPERTY_DEFAULT = "ONE";

	public static final String WRITE_CONSISTENCY_LEVEL_PROPERTY = "cassandra.writeconsistencylevel";
	public static final String WRITE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT = "ONE";

	ConsistencyLevel readConsistencyLevel;
  	ConsistencyLevel writeConsistencyLevel;

	private static final String INSERT_STATEMENT =
		String.format("INSERT INTO %s (%s, %s, %s, %s) VALUES (?, ?, ?, ?)",
		EMP_CF_NAME, COL_NAME_EMPID, COL_NAME_DEPTID, COL_NAME_FIRST_NAME, COL_NAME_LAST_NAME);

	private static final String CREATE_STATEMENT =
		String.format("CREATE TABLE %s (%s int, %s int, %s varchar, %s varchar, PRIMARY KEY (%s, %s))",
		EMP_CF_NAME, COL_NAME_EMPID, COL_NAME_DEPTID, COL_NAME_FIRST_NAME, COL_NAME_LAST_NAME,
		COL_NAME_EMPID, COL_NAME_DEPTID);

	public void init() throws DBException {
		String hosts = getProperties().getProperty("hosts");
		/**if (hosts == null) {
			throw new DBException("Required property \"hosts\" missing for CassandraClient");
		}**/
		System.out.println("coniste new");
		System.out.println(getProperties().getProperty(READ_CONSISTENCY_LEVEL_PROPERTY, READ_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));
		readConsistencyLevel = ConsistencyLevel
			.valueOf("CL_"+getProperties()
				.getProperty(READ_CONSISTENCY_LEVEL_PROPERTY, READ_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));
		writeConsistencyLevel = ConsistencyLevel
			.valueOf("CL_"+getProperties()
				.getProperty(WRITE_CONSISTENCY_LEVEL_PROPERTY, WRITE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));
			
	    context = new AstyanaxContext.Builder()
	    .forCluster("Test Cluster")
	    .forKeyspace("usertable")
	    .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()      
	        .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
	    )
	    .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("MyConnectionPool")
	        .setPort(9160)
	        .setMaxConnsPerHost(1)
	        .setSeeds("127.0.0.1:9160")
	    )
	    .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()      
	        .setCqlVersion("3.1.0")
	        .setTargetCassandraVersion("2.0"))
	    .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
	    .buildKeyspace(ThriftFamilyFactory.getInstance());
	     
	    context.start();
	    keyspace = context.getEntity();
	   
	    EMP_CF = ColumnFamily.newColumnFamily(
	        EMP_CF_NAME, 
	        StringSerializer.get(), 
	        StringSerializer.get());
	  }

	/**
   * Insert a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to insert.
   * @param values
   *          A HashMap of field/value pairs to insert in the record
   * @return Zero on success, a non-zero error code on error
   */
	public int insert(String table, String key, HashMap<String, ByteIterator> values) {
		MutationBatch m = keyspace.prepareMutationBatch();
		try {
			for (Entry<String, ByteIterator> entry : values.entrySet()) {
				m.withRow(EMP_CF,key)
				.putColumn(entry.getKey(), entry.getValue().toString(), null)
				.setTimestamp(System.currentTimeMillis());
				
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
   *          The name of the table
   * @param key
   *          The record key of the record to write.
   * @param values
   *          A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error
   */
	public int update(String table, String key, HashMap<String, ByteIterator> values) {
		return insert(table, key, values);
	}

	/**
	* Read a record from the database. Each field/value pair from the result will
	* be stored in a HashMap.
	*
	* @param table
	*          The name of the table
	* @param key
	*          The record key of the record to read.
	* @param fields
	*          The list of fields to read, or null for all of them
	* @param result
	*          A HashMap of field/value pairs for the result
	* @return Zero on success, a non-zero error code on error
	*/
	public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
		try{
			if(fields == null) {
				OperationResult<ColumnList<String>> oResult =
				keyspace.prepareQuery(EMP_CF)
					.getKey(key)
					.execute();
				ColumnList<String> columns = oResult.getResult();
				for(String s : columns.getColumnNames()) {
					result.put(s,new StringByteIterator(columns.getColumnByName(s).getStringValue()));
				}
			} else {
					OperationResult<ColumnList<String>> opResult = keyspace.prepareQuery(EMP_CF)
				    .getKey(key)
				    .withColumnSlice(fields)
				    .execute();	
				    ColumnList<String> columns = opResult.getResult();
			
				for(String name : columns.getColumnNames()) {
					result.put(name,new StringByteIterator(columns.getColumnByName(name).getStringValue()));
				}
			}
			return Ok;
		}catch (ConnectionException e) {
			System.out.println(e);
			return Error;
			
		}
		
	}

	/**
   * Delete a record from the database.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error
   */
	public int delete(String table, String key) {
		try{
			MutationBatch m = keyspace.prepareMutationBatch();
			//timestamp is not set, because i will not delete properly
			m.withRow(EMP_CF, key)
				.setTimestamp(System.currentTimeMillis())
				.delete(); 
			m.execute();
			return Ok;
		} catch (Exception e) {
			System.out.println(e);
			return Error;
			
		}
		
	}

	/**
	 * Perform a range scan for a set of records in the database. Each field/value pair from the result will be stored in a HashMap.
	 *
	 * @param table The name of the table
	 * @param startkey The record key of the first record to read.
	 * @param recordcount The number of records to read
	 * @param fields The list of fields to read, or null for all of them
	 * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
	 * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
	 */
	public int scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String,ByteIterator>> result) {
		throw new UnsupportedOperationException("Scan method is not implemented.");
		/**try {

			if (fields == null) {
				OperationResult<Rows<String,String>>opResult;
				opResult = keyspace.prepareQuery(EMP_CF)
				    .getRowRange(startkey, "", "", "", recordcount)
				    .withColumnSlice(fields)
				    .execute();

				for (Row<String, String> row : opResult.getResult()) {
				HashMap<String,ByteIterator> resultMap = new HashMap<String,ByteIterator> ();
				ColumnList<String> columns = row.getColumns();		
				for(String name : columns.getColumnNames()) {
					resultMap.put(name, new StringByteIterator(columns.getColumnByName(name).getStringValue()));
				}
				result.add(resultMap);
			}
			} else {
				OperationResult<Rows<String,String>>opResult;
			opResult = keyspace.prepareQuery(EMP_CF)
			    .searchWithIndex()
			    .setStartKey(startkey)
			    .setRowLimit(recordcount)
			    .withColumnSlice(fields)
			    .execute();
			    System.out.println("Operation result ext.");
			for (Row<String, String> row : opResult.getResult()) {
				HashMap<String,ByteIterator> resultMap = new HashMap<String,ByteIterator> ();
				ColumnList<String> columns = row.getColumns();		
				for(String name : columns.getColumnNames()) {
					resultMap.put(name, new StringByteIterator(columns.getColumnByName(name).getStringValue()));
				}
				result.add(resultMap);
			}
			}
			
			return Ok;
		} catch (ConnectionException e) {
			System.out.println(e);
			return Error;
		}**/
		
	}
	/**
	* Perform a range scan for a set of records in the database. Each field/value
	* pair from the result will be stored in a HashMap.
	*
	* @param table
	*          The name of the table
	* @param startkey
	*          The record key of the first record to read.
	* @param recordcount
	*          The number of records to read
	* @param fields
	*          The list of fields to read, or null for all of them
	* @param result
	*          A Vector of HashMaps, where each HashMap is a set field/value
	*          pairs for one record
	* @return Zero on success, a non-zero error code on error
	*/
	/**public int scan(String table, String startkey, int recordcount, Set<String> fields,
	Vector<HashMap<String, ByteIterator>> result) {
		return Ok;
	}


	public void insert2(int key, String firstname, String lastname) {
		MutationBatch m = keyspace.prepareMutationBatch();

		m.withRow(EMP_CF, key)
			.putColumn("firstname", firstname, null)
			.putColumn("lastname", lastname, null);

		try {
			OperationResult<Void> result = m.execute();
		} catch (ConnectionException e) {
			System.out.println(e);
		}
	}

	public void read2(int key) {
		try{
			OperationResult<ColumnList<String>> result =
				keyspace.prepareQuery(EMP_CF)
					.getKey(key)
					.execute();

			ColumnList<String> columns = result.getResult();
			if(columns != null) {
				System.out.println("firstname: "+columns.getColumnByName("firstname").getStringValue());
				System.out.println("lastname: "+columns.getColumnByName("lastname").getStringValue());
		
			} else {
				System.out.println("Columns are null");
			}
			
		}catch (ConnectionException e) {
			System.out.println(e);
			throw new RuntimeException("failed to read from C*", e);
		}
		
	}**/


    

}
