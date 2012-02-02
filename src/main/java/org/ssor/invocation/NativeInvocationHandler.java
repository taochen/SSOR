package org.ssor.invocation;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import org.ssor.AtomicService;
import org.ssor.ManagerBus;
import org.ssor.RequirementsAwareAdaptor;

public class NativeInvocationHandler extends GenericInvocation implements
		InvocationHandler {

	
	private Object realInstance;
	
	private ProxyFactory proxyFactory;

	
	public NativeInvocationHandler(ManagerBus bus, RequirementsAwareAdaptor awareness, ProxyFactory proxyFactory) {
		super(bus, awareness);
		this.proxyFactory = proxyFactory;
	}

	public NativeInvocationHandler(ManagerBus bus, RequirementsAwareAdaptor awareness, ProxyFactory proxyFactory, Object realInstance) {
		super(bus, awareness);
		this.realInstance = realInstance;
		this.proxyFactory = proxyFactory;
	}

	@Override
	public Object invoke(Object instance, Method method, Object[] arguments)
			throws Throwable {
		
		return wrapInvoke(method, realInstance, arguments);
	}

	/**
	 * This is the invocation on replica site, therefore no return needed
	 */
	@Override
	public void invoke(AtomicService service, Object[] arguments) {
		
		Object proxy = proxyFactory.get(service.getProxyAlias());
		invokeOnReplica(service, proxy, arguments);
		
	}
	
	public Object getProxy() {

		return Proxy.newProxyInstance(
				realInstance.getClass().getClassLoader(), realInstance
						.getClass().getInterfaces(), this);
	}

}
