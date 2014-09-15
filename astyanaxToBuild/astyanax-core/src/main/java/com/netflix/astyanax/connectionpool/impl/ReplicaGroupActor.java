/**
 * Created by lalith on 29.08.14.
 */
package com.netflix.astyanax.connectionpool.impl;

import akka.actor.UntypedActor;
import akka.actor.UntypedActorWithStash;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.Procedure;
import scala.concurrent.duration.Duration;

import java.util.concurrent.TimeUnit;


public class ReplicaGroupActor extends UntypedActorWithStash {



    private final Procedure<Object> WAITING_STATE = new Procedure<Object>() {
        @Override
        public void apply(Object msg) throws Exception {
            if (msg instanceof  String) {
               
            } else {
                
            }
            
        }
    };

    @Override
    public void onReceive(Object msg) {
        if (msg instanceof String) {
            //long durationToWait = (long) ((ConnectionPoolConfigurationImpl) msg);
            //System.out.println("on Receive: instance of String");
            long durationToWait = 0;
            getSender().tell(msg, getSelf());
            assert (durationToWait >= 0);
            if (durationToWait > 0) {
                stash();
                switchToWaiting(durationToWait);
            }
        }
    }

    private void switchToWaiting(final long durationToWait) {
        System.out.println("Switching to waiting " + durationToWait);
        getContext().become(WAITING_STATE, false);
       /** getContext().system().scheduler().scheduleOnce(
                Duration.create(durationToWait, TimeUnit.NANOSECONDS),
                getSelf(),
                ReplicaGroupActorCommand.UNBLOCK,
                getContext().system().dispatcher(),
                null);**/
    }

}