package org.ssor.gcm;

import org.ssor.protocol.Message;

public interface CommunicationAdaptor {

	/**
	 * This normally implemented in GCM aware manner
	 * 
	 * @param message
	 */
	public void multicast(Message message);

	/**
	 * This normally implemented in GCM aware manner
	 * 
	 * @param message
	 */
	public void unicast(Message message, Object address);

	public void receive(Message message, Object address);

	/**
	 * This normally implemented in GCM aware manner
	 * 
	 * @param message
	 */
	public void blockCall(Object object);

	/**
	 * This normally implemented in GCM aware manner
	 * 
	 * @param message
	 */
	public void blockCall(Object object, Object address);
}
