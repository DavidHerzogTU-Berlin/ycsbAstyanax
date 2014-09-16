package com.netflix.astyanax.connectionpool.impl;

import java.util.List;

import com.netflix.astyanax.connectionpool.Connection;
import com.netflix.astyanax.connectionpool.ConnectionPoolConfiguration;
import com.netflix.astyanax.connectionpool.ConnectionPoolMonitor;
import com.netflix.astyanax.connectionpool.HostConnectionPool;
import com.netflix.astyanax.connectionpool.Operation;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.NoAvailableHostsException;
import java.util.LinkedList;
import java.net.InetAddress;
import akka.actor.ActorRef;
import java.net.UnknownHostException;
import scala.concurrent.Future;
import scala.concurrent.Await;
import scala.concurrent.*;
import static akka.pattern.Patterns.ask;
import akka.dispatch.*;
import scala.concurrent.duration.Duration;
import akka.util.Timeout;
/**
 * Class that extends {@link AbstractExecuteWithFailoverImpl} to provide
 * functionality for borrowing a {@link Connection} from a list of
 * {@link HostConnectionPool}(s) in a round robin fashion. <br/>
 * <br/>
 * 
 * It maintains state of the current and next pool to be used by a revolving
 * index over the list of pools, hence round robin. It also maintains state of
 * how many retries have been done for this instance and consults the
 * {@link ConnectionPoolConfiguration#getMaxFailoverCount()} threshold.
 * 
 * @author elandau
 * 
 * @param <CL>
 * @param <R>
 * 
 * @see {@link AbstractExecuteWithFailoverImpl} for details on how failover is
 *      repeatedly called for ensuring that an {@link Operation} can be executed
 *      with resiliency.
 * @see {@link RoundRobinConnectionPoolImpl} for the impl that references this
 *      class.
 * @see {@link AbstractHostPartitionConnectionPool} for more context on how
 *      failover functionality is used within the context of an operation
 *      execution
 * 
 */
public class RoundRobinExecuteWithFailover<CL, R> extends
		AbstractExecuteWithFailoverImpl<CL, R> {
	private int index;
	protected HostConnectionPool<CL> pool;
	private int retryCountdown;
	protected final List<HostConnectionPool<CL>> pools;
	protected final int size;
	protected int waitDelta;
	protected int waitMultiplier = 1;

	public RoundRobinExecuteWithFailover(ConnectionPoolConfiguration config,
			ConnectionPoolMonitor monitor, List<HostConnectionPool<CL>> pools,
			int index) throws ConnectionException {
		super(config, monitor);

		this.index = index;
		this.pools = pools;

		if (pools == null || pools.isEmpty()) {
			throw new NoAvailableHostsException("No hosts to borrow from");
		}

		size = pools.size();
		retryCountdown = Math.min(config.getMaxFailoverCount(), size);
		if (retryCountdown < 0)
			retryCountdown = size;
		else if (retryCountdown == 0)
			retryCountdown = 1;

		waitDelta = config.getMaxTimeoutWhenExhausted() / retryCountdown;
	}

	public int getNextHostIndex() {
		// No RR anymore: Get the node with the best score.
		if (this.config.getName().equals("continuous")) {
			return 0;
		} else {
			try {
				return index % size;
			} finally {
				index++;
				if (index < 0)
					index = 0;
			}
		}
	}

	public boolean canRetry() {
		return --retryCountdown > 0;
	}

	@Override
	public HostConnectionPool<CL> getCurrentHostConnectionPool() {
		return pool;
	}

	@Override
	public Connection<CL> borrowConnection(Operation<CL, R> operation)
			throws ConnectionException {

	
        if (this.config.getName().equals("continuous") && operation.getClass().getName().contains("ThriftColumnFamilyQueryImpl")) {
            List<InetAddress> ipAddressList = new LinkedList<InetAddress>();
          
            try {
                for (HostConnectionPool<CL> hostConpool: pools) {
                    ipAddressList.add(InetAddress.getByName(  hostConpool.getHost().getIpAddress() ));
                }

                Timeout timeout = new Timeout(Duration.create(1, "seconds"));
                ActorRef actor = PendingRequestMap.getReplicaGroupActor(ipAddressList);
               
                if( pools != null ) {
                    SimpleActorMessage<CL> simpleActorMessage = new SimpleActorMessage<CL>(pools);
                    Future<Object> future = ask(actor, simpleActorMessage, timeout);
                    SimpleActorMessage<CL> result =  (SimpleActorMessage<CL> ) Await.result(future, timeout.duration());
                    if( result != null){
                        return result.getPools().get(0).borrowConnection(waitDelta * waitMultiplier);
                    }
                }
                
            } catch (UnknownHostException e) {
                e.printStackTrace();
            } catch (Exception e ) {
                System.out.println(e);
            }
            
        }        
		pool = pools.get(getNextHostIndex());
		return pool.borrowConnection(waitDelta * waitMultiplier);
	}

}
