package com.netflix.astyanax.connectionpool.impl;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.HashMap;

import com.google.common.util.concurrent.AtomicDouble;
import com.netflix.astyanax.connectionpool.Host;
import akka.actor.ActorSystem;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.Config;
import java.util.concurrent.ConcurrentHashMap;

public class PendingRequestMap {

	private static ConcurrentHashMap<String, AtomicInteger> pendingRequestMap = new ConcurrentHashMap<String, AtomicInteger>();
	private static final Config config = ConfigFactory
			.parseString("my-dispatcher {\n" + "  type = Dispatcher\n"
					+ "  executor = \"fork-join-executor\"\n"
					+ "  fork-join-executor {\n" + "    parallelism-min = 2\n"
					+ "    parallelism-factor = 2.0\n"
					+ "    parallelism-max = 20\n" + "  }\n"
					+ "  throughput = 10\n" + "}\n");
	private static final ActorSystem actorSystem = ActorSystem.create(
			"Machete", config);

	private static ConcurrentHashMap<String, AtomicDouble> qszEmaMap = new ConcurrentHashMap<String, AtomicDouble>();
	private static ConcurrentHashMap<String, AtomicDouble> muEmaMap = new ConcurrentHashMap<String, AtomicDouble>();
	private static ConcurrentHashMap<String, AtomicDouble> rtMap = new ConcurrentHashMap<String, AtomicDouble>();
	private static ConcurrentHashMap<String, AtomicDouble> nwMap = new ConcurrentHashMap<String, AtomicDouble>();

	private static double c = 1;
	private final static double k = .9; // value for calculation
	private final static double one_minus_k = .1; // value for calculation

	public static void addMUSample(String ip, double sample) {
		AtomicDouble cached = muEmaMap.get(ip);
		if (cached == null) {
			muEmaMap.putIfAbsent(ip, new AtomicDouble(0));
			cached = muEmaMap.get(ip);
		}
		cached.set(sample * k + cached.get() * one_minus_k);
	}

	public static void addQSZsample(String ip, double sample) {
		AtomicDouble cached = qszEmaMap.get(ip);
		if (cached == null) {
			qszEmaMap.putIfAbsent(ip, new AtomicDouble(0));
			cached = qszEmaMap.get(ip);
		}
		cached.set(sample * k + cached.get() * one_minus_k);
	}

	

	public static void addRTsample(String ip, double sample) {
		AtomicDouble cached = rtMap.get(ip);
		if (cached == null) {
			rtMap.putIfAbsent(ip, new AtomicDouble(0));
			cached = rtMap.get(ip);
		}
		cached.set(sample * k + cached.get() * one_minus_k);

	}

	public static double getnw(String ip) {
		if (rtMap.get(ip) == null || muEmaMap.get(ip) == null)
			return 0;
		else
			return rtMap.get(ip).get() - muEmaMap.get(ip).get();
	}

	public static double getScoreForHost(String ip) {
		if ( pendingRequestMap.get(ip) == null || qszEmaMap.get(ip) == null
				|| muEmaMap.get(ip) == null)
			return 0;
		else
			return getnw(ip)
					+ Math.pow(
							(1 + pendingRequestMap.get(ip).get() * c + qszEmaMap
									.get(ip).get()), 3)
					* muEmaMap.get(ip).get();
	}

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