/**
 * Created by lalith on 29.08.14.
 * and edited by david 16.09.14
 */
package com.netflix.astyanax.connectionpool.impl;

import java.util.List;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import akka.actor.UntypedActor;
import akka.actor.UntypedActorWithStash;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.Procedure;
import scala.concurrent.duration.Duration;
import com.netflix.astyanax.connectionpool.HostConnectionPool;
import java.util.concurrent.TimeUnit;


public class ReplicaGroupActor extends UntypedActorWithStash {



    private final Procedure<Object> WAITING_STATE = new Procedure<Object>() {
        @Override
        public void apply(Object msg) throws Exception {
             if (msg instanceof ReplicaGroupActorCommand) {
                switch ((ReplicaGroupActorCommand) msg) {
                    case UNBLOCK:
                        getContext().unbecome();
                        unstashAll();
                        break;
                    default:
                        throw new AssertionError("Received invalid command "
                                + (ReplicaGroupActorCommand) msg);
                }
            }
            else {
                stash();
            }
            
        }
    };

    @Override
    public void onReceive(Object msg) {
        if (msg instanceof SimpleActorMessage) {
            long durationToWait = 0;
            SimpleActorMessage simpleActorMessage = (SimpleActorMessage) msg;
            List<HostConnectionPool<?>> hostList = simpleActorMessage.getPools();
            Collections.sort(hostList, scoreComparator);
            int dataEndpointIndex = 0;
            double minimumDurationToWait = Double.MAX_VALUE;
            boolean shouldWait = true;
            for(int i = 0; i < hostList.size(); i++) {
                final String ep = hostList.get(i).getHost().getIpAddress();
                double timeToNextRefill = 0L;
                timeToNextRefill = PendingRequestMap.getRateLimit(ep);
                
                if (timeToNextRefill == 0L) {
                    dataEndpointIndex = i;
                    shouldWait = false;

                    // Other parts of the code require this endpoint-mapping
                    // to be ordered such that the data-endpoint is the first
                    // one. Let's do that now itself.
                    Collections.<HostConnectionPool<?>>swap(hostList, dataEndpointIndex, 0);
                    break;
                }

                minimumDurationToWait = Math.min(minimumDurationToWait, timeToNextRefill);
            }

            assert ((long) minimumDurationToWait >= 0);
            if (shouldWait && minimumDurationToWait > 0) {
                stash();
                switchToWaiting((long)minimumDurationToWait);
            } else {
                getSender().tell(msg, getSelf());
            }
        }
    }

    private Comparator<HostConnectionPool<?>> scoreComparator = new Comparator<HostConnectionPool<?>>() {
        @Override
        public int compare(HostConnectionPool<?> p1, HostConnectionPool<?> p2) {
            double score1 = PendingRequestMap.getScoreForHost(p1.getHost().getIpAddress());
            double score2 = PendingRequestMap.getScoreForHost(p2.getHost().getIpAddress());
            if (score1 < score2) {
                return -1;
            }
            else if (score1 > score2) {
                return 1;
            }
            return 0;
        }
    };

    private void switchToWaiting(final long durationToWait) {
        System.out.println("Switching to waiting " + durationToWait );
        getContext().become(WAITING_STATE, false);
        getContext().system().scheduler().scheduleOnce(
                Duration.create(durationToWait, TimeUnit.NANOSECONDS),
                getSelf(),
                ReplicaGroupActorCommand.UNBLOCK,
                getContext().system().dispatcher(),
                null);
    }

}