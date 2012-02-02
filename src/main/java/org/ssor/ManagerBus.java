package org.ssor;

import org.ssor.protocol.election.ElectionManager;
import org.ssor.protocol.replication.ReplicationManager;
import org.ssor.protocol.tolerance.FTManager;

public interface ManagerBus {

	
	public ElectionManager getElectionManager();

	public ReplicationManager getReplicationManager();

	public FTManager getFaultyManager();
}
