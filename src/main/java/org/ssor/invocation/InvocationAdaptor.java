package org.ssor.invocation;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.ssor.AtomicService;

public interface InvocationAdaptor {

	
	public void invoke(AtomicService service, Object[] arguments);
	
	public Object invoke(Method method, String service, Object instance, Object[] arguments)throws IllegalArgumentException, 
	IllegalAccessException, InvocationTargetException;
	
	public void setCallback(InvocationCallback callback);
}
