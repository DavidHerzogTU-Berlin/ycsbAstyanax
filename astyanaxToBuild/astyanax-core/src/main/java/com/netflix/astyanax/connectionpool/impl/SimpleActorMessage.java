package com.netflix.astyanax.connectionpool.impl;
import java.util.List;
import com.netflix.astyanax.connectionpool.HostConnectionPool;
public class SimpleActorMessage<CL> {

	protected final List<HostConnectionPool<CL>> pools;

	public SimpleActorMessage(List<HostConnectionPool<CL>> pools) {
		this.pools = pools;
	}

	public List<HostConnectionPool<CL>> getPools() {
		return pools;
	}
}