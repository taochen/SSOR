package org.ssor.listener.helper;

import org.ssor.listener.CommunicationListener;
import org.ssor.util.Group;

public class CommunicationListenerHelper implements ProtocolHelper<CommunicationListener> {

	@Override
	public void assignParameters(CommunicationListener protocol, Group group, Object params) {
		protocol.setCommunicationAdaptor(group);
	}

	@Override
	public Object createParameters(Group group) {
		return group;
	}

}
