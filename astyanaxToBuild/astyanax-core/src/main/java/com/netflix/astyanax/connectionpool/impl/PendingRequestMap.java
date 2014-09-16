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
import akka.actor.ActorRef;
import akka.actor.Props;
import java.net.InetAddress;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import java.net.UnknownHostException;

public class PendingRequestMap {

	private static final ConcurrentHashMap<InetAddress, ActorRef> actorMap = new ConcurrentHashMap<InetAddress, ActorRef>();
    private static final ConcurrentHashMap<InetAddress, CopyOnWriteArrayList<String>> rgInvertedInex = new ConcurrentHashMap<InetAddress, CopyOnWriteArrayList<String>>();

	private static ConcurrentHashMap<InetAddress, SendReceiveRateContainer> scoreMap = new ConcurrentHashMap<InetAddress, SendReceiveRateContainer>();
	private static ConcurrentHashMap<InetAddress, AtomicInteger > pendingRequestMap = new ConcurrentHashMap<InetAddress, AtomicInteger >();
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
	private static Object lock = new Object();

	 public static ActorRef getReplicaGroupActor(List<InetAddress> endpoints) {
        final InetAddress rgOwner = endpoints.get(0);
        ActorRef rgActor = actorMap.get(rgOwner);
        if (rgActor == null) {
            synchronized (lock) {
                if (!actorMap.containsKey(rgOwner)) {
                    rgActor = actorSystem.actorOf(Props.create(ReplicaGroupActor.class).withDispatcher("my-dispatcher"), rgOwner.getHostName());
                    final ActorRef result = actorMap.putIfAbsent(rgOwner, rgActor);
                    if (result == null) {
                        /* For every endpoint, we add the replica group actor's name to the list
                         */
                        for (InetAddress e : endpoints) {
                            rgInvertedInex.putIfAbsent(e, new CopyOnWriteArrayList<String>());
                            rgInvertedInex.get(e).add(rgOwner.getHostName());
                        }
                    }
           
                }
            }
            return actorMap.get(rgOwner);
        }
        return rgActor;
    }

	public static Config getConfig() {
		return config;
	}

	public static ActorSystem getActorSystem() {
		return actorSystem;
	}

	public static void addSamples(String ip, double mu, int qsz, double responseTime) {
		try {
			InetAddress endpoint = InetAddress.getByName(ip);
			SendReceiveRateContainer cached = scoreMap.get(endpoint);
			if (cached == null) {
				scoreMap.putIfAbsent(endpoint, new SendReceiveRateContainer(endpoint));
				cached = scoreMap.get(endpoint);
			}
			cached.updateNodeScore(qsz, mu, responseTime);

		} catch (UnknownHostException e) {
			System.out.println(e);
		}
		
	}

	/**public static void addMUSample(String ip, double sample) {
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
**/
	public static double getScoreForHost(String ip) {
		try {
			InetAddress endpoint = InetAddress.getByName(ip);
			if(scoreMap.get(endpoint) == null) {
				return 0;
			} else {
				return scoreMap.get(endpoint).getScore();
			}
			
		} catch(UnknownHostException e) {

		}
		return -1;
	}

	public static void incrementPendingRequest(String ip) {
		try {
			AtomicInteger count = pendingRequestMap.get(InetAddress.getByName(ip));
			if (count == null) {
				pendingRequestMap.putIfAbsent(InetAddress.getByName(ip), new AtomicInteger(0));
				count = pendingRequestMap.get(InetAddress.getByName(ip));
			}
			count.incrementAndGet();
		} catch(UnknownHostException e) {

		}
		

	}

	public static void decrementPendingRequest(String ip) {
		try {
			AtomicInteger count = pendingRequestMap.get(InetAddress.getByName(ip));
			if (count == null) {
				pendingRequestMap.putIfAbsent(InetAddress.getByName(ip), new AtomicInteger(0));
				count = pendingRequestMap.get(InetAddress.getByName(ip));
			}
			count.decrementAndGet();
		} catch(UnknownHostException e) {

		}
	}

	public static int getPendingRequests(String ip) {
		try {
			return pendingRequestMap.get(InetAddress.getByName(ip)).get();
		} catch(UnknownHostException e) {

		}
		return -1;
	}

	public static AtomicInteger getPendingRequestsAtomic(String ip) {
		try {
			return pendingRequestMap.get(InetAddress.getByName(ip));
		} catch(UnknownHostException e) {

		}
		return new AtomicInteger(-1);
	}

	public static int getPendingRequestsMapSize() {
		return pendingRequestMap.size();
	}
}