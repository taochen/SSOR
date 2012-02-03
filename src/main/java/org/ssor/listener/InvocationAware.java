package org.ssor.listener;

import org.ssor.annotation.ProtocolListenerHelper;
import org.ssor.invocation.InvocationAdaptor;
import org.ssor.invocation.InvocationCallback;

@ProtocolListenerHelper(helperClass=org.ssor.listener.helper.InvocationAwareHelper.class)
public interface InvocationAware {

	
	public void setInvocationAdaptor(InvocationAdaptor adaptor);
	
	public void setCallback(InvocationCallback callback);
}
