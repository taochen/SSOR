package org.ssor.protocol;

public interface ProtocolManager {

	public boolean handleReceive(Message message, Object address, int addressUUID);
}
