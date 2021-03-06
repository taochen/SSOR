package org.ssor.util;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.ssor.CollectionFacade;

/**
 * Cache environmental variables
 * @author Tao Chen
 *
 */
@SuppressWarnings("unchecked")
public class Environment {

	
	// Indicate if it is allow to use session level FIFO communication (used by GCM)
	// to determine on different sessions.
	public static boolean ENABLE_CHANGE_FIFO = false;

	private static final Map<String, Group> globalContext = CollectionFacade.getConcurrentHashMap();
	
	public static final  ExecutorService pool = Executors.newCachedThreadPool();
	
	public static final ThreadLocal<Boolean> isNew = new ThreadLocal<Boolean> ();
	// This is used to determine if the current service delivery has been transfered
	// to another thread, (used in releaseMsgOnView) as GCM can not tolerance suspend
	// of a delivery process.
	public static final ThreadLocal<Boolean> isDeliverySuspended = new ThreadLocal<Boolean> ();
	
	public static Group getGroup(String group){
		return globalContext.get(group);
	}

	public static void setGroup(Group group){
		globalContext.put(group.getName(), group);
	}
	
	/**
	 * Singleton mode
	 * @param groupName
	 * @param clazz
	 * @return
	 */
	public static Object getProxy(String groupName, Class<?> clazz){
		return globalContext.get(groupName).getProxyFactory().get(clazz);
	}
	
	/**
	 * Prototype mode
	 * @param groupName
	 * @param clazz
	 * @return
	 */
	public static Object getNewProxy(String groupName, Class<?> clazz){
		return globalContext.get(groupName).getProxyFactory().getNew(clazz);
	}
	
}
