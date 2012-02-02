package org.ssor.invocation;

import java.util.Map;

import org.ssor.CollectionFacade;
import org.ssor.util.Group;


/**
 * 
 * Factory class for maintain instance of service, as well as invocation on replica site
 * This is a native implementation, can be replaced by Spring's IoC
 * 
 * 
 * @author Tao Chen
 *
 */
 
@SuppressWarnings("unchecked")
public class ProxyFactory {

	private  Map<String, Object> registry;

	private Group group;
	
	public ProxyFactory(Group group) {
		super();
		this.group = group;
	}


	@SuppressWarnings("unchecked")
	public  Object get(Class<?> clazz){
		
		
		// Should not concurrent
		if(registry == null)
			registry = CollectionFacade.getConcurrentHashMap(50);
		
		if(registry.containsKey(clazz.getName()))
				return registry.get(clazz.getName());
		
		Object proxy = null;
		try {
			proxy = new NativeInvocationHandler(group, group, group.getProxyFactory(), clazz.newInstance()).getProxy();
		} catch (Exception e) {
			e.printStackTrace();
		} 
		registry.put(clazz.getName(), proxy);
		
		return proxy;
	}
	

	@SuppressWarnings("unchecked")
	public  Object getNew(Class<?> clazz){
		
		
		// Should not concurrent
		if(registry == null)
			registry = CollectionFacade.getConcurrentHashMap(50);
		
		Object proxy = null;
		try {
			proxy = new NativeInvocationHandler(group, group, group.getProxyFactory(), clazz.newInstance()).getProxy();
		} catch (Exception e) {
			e.printStackTrace();
		} 
		registry.put(clazz.getName(), proxy);
		
		return proxy;
	}
	
	@SuppressWarnings("unchecked")
	public  Object getNew(Class<?> clazz, InvocationCallback callback){

		
		// Should not concurrent
		if(registry == null)
			registry = CollectionFacade.getConcurrentHashMap(50);
		Object proxy = null;
		try {
			NativeInvocationHandler handler = new NativeInvocationHandler(group, group, group.getProxyFactory(), clazz.newInstance());
			handler.setCallback(callback);
			proxy = handler.getProxy();
		} catch (Exception e) {
			e.printStackTrace();
		} 
		registry.put(clazz.getName(), proxy);
		
		return proxy;
	}
	
	@SuppressWarnings("unchecked")
	public  Object get(Class<?> clazz, InvocationCallback callback){
		
		// Should not concurrent
		if(registry == null)
			registry = CollectionFacade.getConcurrentHashMap(50);
		
		if(registry.containsKey(clazz.getName()))
				return registry.get(clazz.getName());
		
		
		Object proxy = null;
		try {
			NativeInvocationHandler handler = new NativeInvocationHandler(group, group, group.getProxyFactory(), clazz.newInstance());
			handler.setCallback(callback);
			proxy = handler.getProxy();
		} catch (Exception e) {
			e.printStackTrace();
		} 
		registry.put(clazz.getName(), proxy);
		
		return proxy;
	}
	/**
	 * This should always called if not using IoC framework like Spring
	 * @param alias
	 * @return
	 */
	public  Object get(String alias){
		return registry.get(alias);
	}
	
	
	
	
}
