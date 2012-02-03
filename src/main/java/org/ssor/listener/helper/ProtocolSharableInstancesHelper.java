package org.ssor.listener.helper;

import java.util.Map;

import org.ssor.CollectionFacade;
import org.ssor.protocol.Message;
import org.ssor.protocol.ProtocolSharableInstances;
import org.ssor.util.Group;


public class ProtocolSharableInstancesHelper implements ProtocolHelper<ProtocolSharableInstances> {

	@SuppressWarnings("unchecked")
	@Override
	public void assignParameters(ProtocolSharableInstances protocol, Group group, Object params) {
		protocol.setCachedSentMessages((Map<String, Message>)params);
	}

	@Override
	public Object createParameters(Group group) {
		return CollectionFacade.getConcurrentHashMap();
	}

}
