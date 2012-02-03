package org.ssor.listener.helper;

import org.ssor.util.Group;

public interface ProtocolHelper<E> {

	public Object createParameters (Group group);
	
	public void assignParameters (E protocol, Group group, Object params);
}
