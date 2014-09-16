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
	private static double map_size;

	private static Object lock = new Object();

	public static double getMap_size() {
		return map_size;
	}

	public static void setMap_size(double map_sizE) {
		map_size = map_sizE;
	}

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
			cached.receiveRateTrackerTick();
			cached.updateCubicSendingRate();
			cached.updateNodeScore(qsz, mu, responseTime);

		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		
	}
	public static double getRateLimit(String ip) {
		try {
			InetAddress endpoint = InetAddress.getByName(ip);
			SendReceiveRateContainer cached = scoreMap.get(endpoint);
			if (cached == null) {
				scoreMap.putIfAbsent(endpoint, new SendReceiveRateContainer(endpoint));
				cached = scoreMap.get(endpoint);
			}
			
			return cached.tryAcquire();

		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		return 0;
	}
	
	public static double getScoreForHost(String ip) {
		try {
			InetAddress endpoint = InetAddress.getByName(ip);
			if(scoreMap.get(endpoint) == null) {
				return 0;
			} else {
				return scoreMap.get(endpoint).getScore();
			}
			
		} catch(UnknownHostException e) {
			e.printStackTrace();
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
			e.printStackTrace();
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
			e.printStackTrace();
		}
	}

	public static int getPendingRequests(String ip) {
		try {
			return pendingRequestMap.get(InetAddress.getByName(ip)).get();
		} catch(UnknownHostException e) {
			e.printStackTrace();
		}
		return -1;
	}

	public static AtomicInteger getPendingRequestsAtomic(String ip) {
		try {
			return pendingRequestMap.get(InetAddress.getByName(ip));
		} catch(UnknownHostException e) {
			e.printStackTrace();
		}
		return new AtomicInteger(-1);
	}

	public static int getPendingRequestsMapSize() {
		return pendingRequestMap.size();
	}
}