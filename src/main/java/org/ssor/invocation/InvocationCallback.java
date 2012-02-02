package org.ssor.invocation;

import java.lang.reflect.Method;

public interface InvocationCallback {

	public Object onException(Throwable t, Object instance, Method method, Object[] arguments) throws Throwable;
	
	public void finallyBlock( Object instance, Method method, Object[] arguments);
}
