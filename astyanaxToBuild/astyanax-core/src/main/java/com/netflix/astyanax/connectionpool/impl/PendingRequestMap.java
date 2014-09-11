package com.netflix.astyanax.connectionpool.impl;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.HashMap;
import com.netflix.astyanax.connectionpool.Host;
import java.util.concurrent.ConcurrentHashMap;
public class PendingRequestMap {

	private static ConcurrentHashMap<String, AtomicInteger > pendingRequestMap = new ConcurrentHashMap<String, AtomicInteger>();

	
	public static void incrementPendingRequest(String ip) {
		AtomicInteger count = pendingRequestMap.get(ip);
		if (count == null) {
			pendingRequestMap.putIfAbsent(ip, new AtomicInteger(0));
			count = pendingRequestMap.get(ip);
		}
		count.incrementAndGet();

	}

	public static void decrementPendingRequest(String ip) {
		AtomicInteger count = pendingRequestMap.get(ip);
		if (count == null) {
			pendingRequestMap.putIfAbsent(ip, new AtomicInteger(0));
			count = pendingRequestMap.get(ip);
		}
		count.decrementAndGet();
	}
	
	public static int getPendingRequests(String ip) {
		return pendingRequestMap.get(ip).get();
	}

}