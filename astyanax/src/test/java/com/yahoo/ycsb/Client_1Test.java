package com.yahoo.ycsb.db;

import static org.junit.Assert.*;
import java.util.Map.Entry;
import java.util.Map;
import java.util.Set;
import org.junit.Rule;
import org.junit.Test;
import java.util.HashMap;
import java.util.HashSet;
import org.junit.Before;
import java.util.Iterator;
import com.yahoo.ycsb.*;
import java.util.Vector;
public class Client_1Test {

  	@Test
	public void insertReadDeleteTest() {
		
		/**try{
			AstyanaxClient_1 ac1 = new AstyanaxClient_1();
			ac1.init();
			HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
			values.put("age", new StringByteIterator("57"));
	    	values.put("middlename", new StringByteIterator("bradley"));
	    	values.put("favoritecolor", new StringByteIterator("blue"));

			assertEquals(0, ac1.insert("data","HansBradley", values));
			values.put("car", new StringByteIterator("VW"));
			
			assertEquals(0, ac1.insert("data","HansBradley1", values));
			
			values.put("wife", new StringByteIterator("ugly"));
			assertEquals(0, ac1.insert("data","HansBradley2", values));

			HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
			Set<String> readSet = new HashSet<String>();
			readSet.add("middlename");
			readSet.add("age");
			System.out.println("----3 reads follow: ");
			assertEquals(0, ac1.read("data", "HansBradley", null, result));
			System.out.println("Results for HansBradley coming: ");
			for (String column : result.keySet()) {
				System.out.println("Column: " + column + " Value: " + result.get(column) );
			}
			
			System.out.println("Results for HansBradley1 coming: ");
			assertEquals(0, ac1.read("data", "HansBradley1", null, result));
			for (String column : result.keySet()) {
				System.out.println("Column: " + column + " Value: " + result.get(column) );
			}
			
			System.out.println("Results for HansBradley2 coming: ");
			assertEquals(0, ac1.read("data", "HansBradley2", null, result));
			for (String column : result.keySet()) {
				System.out.println("Column: " + column + " Value: " + result.get(column) );
			}

			System.out.println("----3 reads follow fields not null: ");
			assertEquals(0, ac1.read("data", "HansBradley", readSet, result));
			assertEquals(0, ac1.read("data", "HansBradley", readSet, result));
			assertEquals(0, ac1.read("data", "HansBradley", readSet, result));
			System.out.println("Read results: ");
			for (String column : result.keySet()) {
				System.out.println("Column: " + column + " Value: " + result.get(column) );
			}
//			for(int i = 0; i < 100; i++) {
//				//ac1.read("data", "HansBradley", null, result);
//			}
//			/**Vector<HashMap<String,ByteIterator>> scanResult = new Vector<HashMap<String,ByteIterator>>();
//			assertEquals(0, ac1.scan("data", "HansBradley", 2, null, scanResult));
//			System.out.println("Scan results: ");
//			for (HashMap<String, ByteIterator> hashMap : scanResult) {
//				for (String column : hashMap.keySet()) {
//					System.out.println("Column: " + column + " Value: " + result.get(column) );
//				}
//			}
			assertEquals(0, ac1.delete("data", "HansBradley"));
			assertEquals(0, ac1.delete("data", "HansBradley1"));
			assertEquals(0, ac1.delete("data", "HansBradley2"));
			HashMap<String, ByteIterator> result2 = new HashMap<String, ByteIterator>();
			assertEquals(0, ac1.read("data", "HansBradley", null, result2));

			System.out.println("Read results: ");
			for (String column : result2.keySet()) {
				System.out.println("Column: " + column + " Value: " + result.get(column) );
			}
		}catch (Exception e) {
			System.out.println(e);
		
		}**/

	}

}
