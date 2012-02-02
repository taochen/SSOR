package org.ssor.invocation;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ssor.AtomicService;
import org.ssor.ManagerBus;
import org.ssor.RequirementsAwareAdaptor;
import org.ssor.ServiceManager;
import org.ssor.ServiceStackContext;
import org.ssor.CompositeService;
import org.ssor.protocol.Message;
import org.ssor.protocol.replication.ReplicationManager;
import org.ssor.protocol.replication.RequestHeader;
import org.ssor.protocol.replication.ResponsePacket;

/**
 * 
 * 
 * 
 * @author Tao Chen
 * 
 */
public abstract class GenericInvocation implements InvocationAdaptor {

	private static final Logger logger = LoggerFactory
	.getLogger(GenericInvocation.class);
	
	protected ServiceManager serviceManager;
	protected ReplicationManager replicationManager;

	protected InvocationCallback callback;
	
	public GenericInvocation(){
		
	}

	public GenericInvocation(ManagerBus bus, RequirementsAwareAdaptor awareness) {
		serviceManager = awareness.getServiceManager();
		replicationManager = bus.getReplicationManager();
	}

	protected Object preService(AtomicService service, Object[] arguments) {

		if (!ServiceStackContext.isReplica()) 
			return replicationManager.request(new InvocationUnit(service,
					arguments));
		

		return null;
	}

	protected Object postService(AtomicService service, Object returnValue) {
		if (!ServiceStackContext.isReplica())
			return replicationManager.finish(new Object[] { service,
					returnValue });

		return null;
	}
	
	/**
	 * This is used on replica
	 * @param service
	 * @param proxy
	 * @param arguments
	 */
	protected void invokeOnReplica(AtomicService service, Object proxy, Object[] arguments) {
		
		try{
		    Object result = service.invoke(proxy, arguments);
		   // System.out.print("Result: " + result+ "\n");
		    if(logger.isDebugEnabled()){
		    	logger.debug("Result: " + result);
		    }
		} catch (Throwable t){
			t.printStackTrace();
			try {
				// If unexcepted exceptions, just throw it
				if(callback != null)
				  callback.onException(t, proxy,  service.getInterfaceMethod(), arguments);
			} catch (Throwable e) {
				throw new RuntimeException(e);
			}
			
		} finally {
			
			if(callback != null)
			  callback.finallyBlock(proxy,  service.getInterfaceMethod(), arguments);
		}
		
	}

	/**
	 * This access point of subclasses for invocation
	 * @param method
	 * @param serviceName
	 * @param instance
	 * @param arguments
	 * @return
	 * @throws Throwable
	 */
	protected Object wrapInvoke(Method method,  Object instance,
			Object[] arguments)
			throws Throwable {
		
		Object result = null;
		String key = "";
		if (arguments != null) {
			for (Type type : method.getGenericParameterTypes())
				key += "," + type.toString();
		}

		if (logger.isDebugEnabled())
			logger.debug("Full service name is: " + instance.getClass().getName() + "." + method.getName() + key);

		try {
			result = invoke(method, instance.getClass().getName() + "." + method.getName() + key, instance,
					arguments);
		} catch (Throwable t) {
			t.printStackTrace();
			if (callback != null)
				return callback.onException(t, instance, method, arguments);

		} finally {
			if (callback != null)
				callback.finallyBlock(instance, method, arguments);
		}
		// System.out.print("method "+ method.getName() + "*******\n");
		return result;
	}

	public Object invoke(Method method, String serviceName, Object instance,
			Object[] arguments) throws IllegalArgumentException,
			IllegalAccessException, InvocationTargetException {

		AtomicService service = serviceManager.get(serviceName);
		
		if (service == null)
			return method.invoke(instance, arguments);
		
		Object object = preService(service, arguments);
		
		
		Object result = null;
		Message message = null;
		// If null means this is the replica, which should not be replicated
		// again
		// or the service should not be replicated.
		if (object == null) {

			// If the is the idempotent service, then reply the result of
			// requester
			// in order to maintain consistency, this only occur on replica
			// sites
			if (ServiceStackContext.isNeedReturnValues(service))
				return ServiceStackContext.getRedundantReturnValue();
			if (ServiceStackContext.isReplica()
					|| ServiceStackContext.isCanReturn(service))
				result = method.invoke(instance, arguments);
			// Even on the requester site, it would need to order composed
			// service
			else {

				Message temp = ServiceStackContext
						.getNestedOrderedMessage(service);
				if (temp == null)
					result = method.invoke(instance, arguments);
				else {
					InvocationHeader header = new InvocationHeader(method,
							instance, arguments);
					// The sequence is RequestHeader, ResponseHeader
					temp.getHeader().getOuter().setOuter(header);
					((RequestHeader) temp.getHeader()).setService(service
							.getName());
					result = replicationManager.executeInAdvance(temp);
				}

			}
		} else {

			message = (Message) object;
			// Non-ordered message would not be blocked
			if (message.isRequirePreBroadcasting()) {

				// if true means service does not need order or nested service
				// that does not need delay, this would be false
				// This is not for sync but for trigger
				synchronized (message) {

					while (!message.isExecutable()) {

						try {

							// This would be unblock after the order timestamp
							// received
							message.wait();

						} catch (InterruptedException e) {
							e.printStackTrace();
						}

					}
				}

			}
			// This will add thread local if this is blocking trigger service
			ServiceStackContext.setNestedServiceStack(service, message);
			InvocationHeader header = new InvocationHeader(method, instance,
					arguments);
			// The sequence is RequestHeader, ResponseHeader
			message.getHeader().getOuter().setOuter(header);
			// Actually the final B has been received here
			result = replicationManager.executeInAdvance(message);

		}

		Object packet = null;
		if ((packet = postService(service, result)) != null) {

			if (!(service instanceof CompositeService)) {
				((ResponsePacket) packet).setNestedArguments(arguments);
			}
			// Broadcast the complete return data or arguments to nodes
			message.setBody(packet);
			replicationManager.doDelayBroadcast(message);
		}
		return result;
	}

	public void setCallback(InvocationCallback callback) {
		this.callback = callback;
	}

}
