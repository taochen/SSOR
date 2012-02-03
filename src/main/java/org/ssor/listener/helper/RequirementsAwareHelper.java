package org.ssor.listener.helper;

import org.ssor.listener.RequirementsAware;
import org.ssor.util.Group;

public class RequirementsAwareHelper implements ProtocolHelper<RequirementsAware> {

	@Override
	public void assignParameters(RequirementsAware protocol, Group group, Object params) {
		protocol.setRequirementsAwareAdaptor(group);
	}

	@Override
	public Object createParameters(Group group) {
		return group;
	}

}
