package com.netflix.astyanax.connectionpool.impl;

import java.util.concurrent.ConcurrentHashMap;
import akka.actor.ActorRef;
import java.util.concurrent.CopyOnWriteArrayList;
import java.net.InetAddress;
import java.util.List;
import akka.actor.ActorSystem;
import akka.actor.Props;
public class MyActor {

    private static final ConcurrentHashMap<InetAddress, ActorRef> actorMap = new ConcurrentHashMap<InetAddress, ActorRef>();
    private static final ConcurrentHashMap<InetAddress, CopyOnWriteArrayList<String>> rgInvertedInex = new ConcurrentHashMap<InetAddress, CopyOnWriteArrayList<String>>();

 /* Machete methods */
    public ActorRef getReplicaGroupActor(List<InetAddress> endpoints) {
        final InetAddress rgOwner = endpoints.get(0);
        ActorRef rgActor = actorMap.get(rgOwner);
        if (rgActor == null) {
            synchronized (this) {
                if (!actorMap.containsKey(rgOwner)) {
                    rgActor = PendingRequestMap.getActorSystem().actorOf(Props.create(ReplicaGroupActor.class).withDispatcher("my-dispatcher"), rgOwner.getHostName());
                    final ActorRef result = actorMap.putIfAbsent(rgOwner, rgActor);
                    if (result == null) {
                        /* For every endpoint, we add the replica group actor's name to the list
                         */
                        for (InetAddress e : endpoints) {
                            rgInvertedInex.putIfAbsent(e, new CopyOnWriteArrayList<String>());
                            rgInvertedInex.get(e).add(rgOwner.getHostName());
                        }
                    }
                   System.out.println("getReplicaGroupActor");
                }
            }
            return actorMap.get(rgOwner);
        }
        return rgActor;
    }
}





