package org.ssor.listener;

import org.ssor.invocation.InvocationAdaptor;
import org.ssor.invocation.InvocationCallback;
public interface InvocationAware {

	
	public void setInvocationAdaptor(InvocationAdaptor adaptor);
	
	public void setCallback(InvocationCallback callback);
}
