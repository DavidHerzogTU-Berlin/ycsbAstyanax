package com.yahoo.ycsb.db;

import static org.junit.Assert.*;
//import javax.inject.Inject;

import org.junit.Rule;
import org.junit.Test;
import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;
import org.junit.Before;
import com.yahoo.ycsb.*;
public class Client_1Test {

	private AstyanaxClient_1 ac1;

	@Before
	public void setUp() throws Exception {
		ac1 = new AstyanaxClient_1();
		//ac1.init();
	}

	/**@Test
	public void initTest() {
	  	System.out.println("##################################################################");
		System.out.println("------initTest----");
		System.out.println("##################################################################");
	  	try {
			ac1.init();
			assertTrue(true);
			} catch (Exception e) {
	
			}
	}**/
  	/**@Test
	public void insert1Test() {

		System.out.println("------------before values-------------");
		HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
		StringByteIterator sbi = new StringByteIterator("HelloWorld");
		values.put("IsThisRealLive", sbi);
		System.out.println("------------before insert-------------");
		assertEquals(1, ac1.insert("usertable","1234", values));
	}**/
}
