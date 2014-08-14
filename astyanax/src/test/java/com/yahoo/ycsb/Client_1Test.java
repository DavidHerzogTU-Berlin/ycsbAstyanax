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

	

	
  	@Test
	public void insert1Test() {

		try{
			AstyanaxClient_1 ac1 = new AstyanaxClient_1();
			ac1.init();
			HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
			values.put("age", new StringByteIterator("57"));
	    	values.put("middlename", new StringByteIterator("bradley"));
	    	values.put("favoritecolor", new StringByteIterator("blue"));
			assertEquals(0, ac1.insert("usertable","HansBradley", values));

			HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
			System.out.println("-----------MyTest------------------");
			assertEquals(0, ac1.read("usertable", "HansBradley", null, result));
			for (String s : result.keySet()) {
		      System.out.println("[" + s + "]=[" + result.get(s) + "]");
		      assertEquals(s, result.get(s));

		    }

		}catch (Exception e) {
			System.out.println(e);
			throw new RuntimeException("failed to read from C*", e);
		}
		
	}
}
