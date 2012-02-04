package org.ssor.protocol;

import org.ssor.util.Group;

public interface ProtocolManager {

	public boolean handleReceive(Message message, Object address, int addressUUID);
	
	public void initializeProtoclManager (Group group);
}
