package org.ssor;

import org.ssor.protocol.ProtocolManager;
public interface ManagerBus {

	public ProtocolManager getManager (Class<?> clazz);
	
}
