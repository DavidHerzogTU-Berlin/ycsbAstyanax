package com.netflix.astyanax.connectionpool.impl;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.HashMap;
import com.netflix.astyanax.connectionpool.Host;
public class ConnectionMap {

	private static Map<String, AtomicInteger > connectionMap = new HashMap<String, AtomicInteger>();

	public static void putIP(String ip) {
		AtomicInteger c = new AtomicInteger(0);
		connectionMap.put(ip, c);
	}
	
	public static void incrementConnection(String ip) {
		if(connectionMap.get(ip) == null)
			putIP(ip);
		AtomicInteger count = new AtomicInteger(connectionMap.get(ip).addAndGet(1));
		connectionMap.put(ip, count);
	}

	public static void decrementConnection(String ip) {
		if(connectionMap.get(ip) == null)
			putIP(ip);
		AtomicInteger count = new AtomicInteger(connectionMap.get(ip).addAndGet(-1));
		connectionMap.put(ip, count);
	}
	
	public static int getCountOfHost(String ip) {
		return connectionMap.get(ip).get();
	}

}