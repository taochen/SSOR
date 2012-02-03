package org.ssor.listener.helper;

import org.ssor.invocation.InvocationAdaptor;
import org.ssor.invocation.NativeInvocationHandler;
import org.ssor.listener.InvocationAware;
import org.ssor.util.Group;

public class InvocationAwareHelper implements ProtocolHelper <InvocationAware> {

	@Override
	public void assignParameters(InvocationAware protocol, Group group, Object params) {
		protocol.setInvocationAdaptor((InvocationAdaptor)params);
		protocol.setCallback(group.getCallback());
	}

	@Override
	public Object createParameters(Group group) {
		return new NativeInvocationHandler(group, group, group.getProxyFactory());
	}

}
