package org.ssor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ssor.exception.ConfigurationException;
import org.ssor.protocol.Header;
import org.ssor.protocol.Message;
import org.ssor.protocol.replication.ResponseHeader;
import org.ssor.protocol.replication.ResponsePacket;

/**
 * 
 * This is thread safe
 * 
 * @author Tao Chen
 * 
 */
public class ServiceStackContext {

	private static final Logger logger = LoggerFactory
			.getLogger(ServiceStackContext.class);
	private static ThreadLocal<AtomicService> rootService = new ThreadLocal<AtomicService>();
	// For replica site, avoid redundant invocation
	private static ThreadLocal<Boolean> isReplica = new ThreadLocal<Boolean>();
	// For requester site, cache deterministic values and nested services'
	// arguments
	private static ThreadLocal<ArgumentWrapper> delayArguments = new ThreadLocal<ArgumentWrapper>();
	// For replica site, to maintain deterministic value
	private static ThreadLocal<ValueWrapper> redundantServiceReturnValues = new ThreadLocal<ValueWrapper>();
	// For requester site
	private static ThreadLocal<ValueWrapper> nestedArguments = new ThreadLocal<ValueWrapper>();

	public static boolean pushService(AtomicService service, Object[] arguments) {

		// Only for single thread
		if (rootService.get() != null) {

			/*
			 * if (logger.isWarnEnabled() &&
			 * rootService.get().compareConsistencyLevel(service) < 0) { logger
			 * .warn("The nested service: " + service.getName() + " has stronger
			 * consistency requirement then the outer service: " +
			 * rootService.get().getName() + ", therefore the outer one may
			 * volatile the consistency, please check to avoid inconsistent
			 * state"); }
			 */

			if (delayArguments.get() == null) {

				if (logger.isDebugEnabled()) {
					logger.debug("Service: " + service.getName()
							+ " does not need delay agreement");
				}

				return false;
			}

			ArgumentWrapper wrapper = delayArguments.get();
			AtomicService root = rootService.get();

			if (!(root instanceof CompositeService)
					&& !(service instanceof RedundantService)) {
				throw new ConfigurationException(
						"Atomic service can not contain other non-redundant service, if atomic service is deserable, then please do not trigger the nested service through proxy");
			}

			// Record arguments for nested service
			if (root instanceof CompositeService
					&& ((CompositeService) root).isNestedReplicationRequired(
							service, wrapper.getNested())) {

				if (logger.isDebugEnabled()) {
					logger
							.debug("Service: "
									+ service.getName()
									+ " needs delay agreement due to nested services, index:"
									+ wrapper.getNested());
				}
				wrapper.addNested(arguments);
			}

			return false;
		}

		rootService.set(service);

		int size = 0;
		if (service instanceof CompositeService)
			size = ((CompositeService) service).getNestedServiceLength();
		if (size != 0 || service.getRedundantServiceCount() != 0)
			delayArguments.set(new ArgumentWrapper(size, service
					.getRedundantServiceCount()));

		if (logger.isDebugEnabled()) {
			logger.debug("Root service: " + service.getName()
					+ "; number of nested service: " + size
					+ "; number of deterministic service: "
					+ service.getRedundantServiceCount());

		}

		return true;
	}

	public static Object popService(AtomicService service, Object returnValue) {

		if (rootService.get() == service) {
			nestedArguments.remove();
			rootService.remove();
			ResponsePacket packet = null;
			// Set the group name to transient field of packet
			if (delayArguments.get() != null) {
				packet = delayArguments.get().getPacket();
				// packet.setGroupName(service.groupName);
				if (service.isRequireBlocking()
						&& delayArguments.get().getNested() == 0
						&& delayArguments.get().getIdempotent() == 0)
					throw new ConfigurationException(
							"Composed service : "
									+ service.getName()
									+ " supposes to have blocking nested services, but there is not detection of them, this may due to invocation of nested services is not issued on the proxy");
			}
			delayArguments.remove();

			if (logger.isDebugEnabled()) {
				logger.debug("Root service: "
						+ service.getName()
						+ " has been executed completely, "
						+ (packet == null ? " has no delay arugments"
								: "has delay arugments"));
				if (packet != null)
					logger.debug(packet.toString());
			}

			return packet;
		}

		AtomicService root = rootService.get();
		ArgumentWrapper wrapper = delayArguments.get();
		if (root.isRedundantServiceRequired(service, wrapper.getIdempotent())) {

			if (logger.isDebugEnabled()) {
				logger.debug("Service: " + service.getName()
						+ " need deterministic value " + returnValue);
			}

			wrapper.addIdempotent(returnValue);
		}

		return null;
	}

	public static void setRedundantReturnValues(AtomicService root,
			Object[] values) {

		if (logger.isDebugEnabled()) {
			logger.debug("Root service: " + root.getName()
					+ " numbers of deterministic value: "
					+ (values == null ? 0 : values.length));
		}

		redundantServiceReturnValues.set(new ValueWrapper(root, values));

	}

	public static Object getRedundantReturnValue() {
		Object result = redundantServiceReturnValues.get().get();
		if (logger.isDebugEnabled()) {
			logger.debug("Use deterministic value: " + result);
		}
		return result;
	}

	public static boolean isNeedReturnValues(AtomicService service) {
		return redundantServiceReturnValues.get() == null ? false
				: redundantServiceReturnValues.get().isIncluded(service);
	}

	public static void setNestedServiceStack(AtomicService service,
			Message message) {
		if (service instanceof CompositeService && service.isRequireBlocking()) {

			// Clone the header, but use new message
			Message newMessage = new Message((Header) message.getHeader()
					.clone(), null, false);

			nestedArguments.set(new ValueWrapper(service, newMessage));
		}

	}

	public static boolean isCanReturn(AtomicService service) {
		if (nestedArguments.get() == null
				|| service instanceof RedundantService)
			return true;
		else
			return !((CompositeService) rootService.get())
					.isRequireBlocking(nestedArguments.get().getCheckIndex());

	}

	public static Message getNestedOrderedMessage(AtomicService service) {
		return nestedArguments.get().isNestedService(service) ? nestedArguments
				.get().getMessage() : null;
	}

	public static void markReplica() {
		isReplica.set(true);
	}

	public static boolean isReplica() {
		Boolean result = isReplica.get();
		return (result == null) ? false : true;
	}

	public static void releaseReplicaMarker() {
		isReplica.remove();
	}

	public static void releaseReturnValues() {
		redundantServiceReturnValues.remove();
	}

	/**
	 * This should be used by single thread only since it should be implemented
	 * in thread safe manner
	 * 
	 * Used to cache nested arguments and redundant services' returns for
	 * replica
	 * 
	 * @author Tao Chen
	 * 
	 */
	public static class ArgumentWrapper {

		// This include arguments of nested service and return result
		// of redundant service, then all sorted as execution order
		private ResponsePacket packet;

		private int nIndex = 0;
		private int iIndex = 0;

		public ArgumentWrapper(int nSize, int rSize) {
			super();
			packet = new ResponsePacket(nSize, rSize);
		}

		public ResponsePacket getPacket() {
			return packet;
		}

		public int getNested() {
			return nIndex;
		}

		public int getIdempotent() {
			return iIndex;
		}

		public void addNested(Object value) {

			Object[] objectrs = packet.getNestedArguments();
			objectrs[nIndex] = value;
			nIndex++;
		}

		public void addIdempotent(Object value) {

			Object[] objects = packet.getReturns();
			objects[iIndex] = value;
			iIndex++;
		}

	}

	/**
	 * This should be used by single thread only since it should be implemented
	 * in thread safe manner
	 * 
	 * Used to cache redundant services' returns for replica sites
	 * 
	 * it is also used for ordering of nested service on requester site
	 * 
	 * @author Tao Chen
	 * 
	 */
	public static class ValueWrapper {

		// normally sequence instance or deterministic value
		private Object[] values;
		private int index = 0;
		private int checkIndex = 0;
		private AtomicService root;
		private Message message;

		public ValueWrapper(AtomicService root, Object[] values) {
			super();
			this.values = values;
			this.root = root;
		}

		public ValueWrapper(AtomicService root, Message message) {
			super();

			this.root = root;
			this.message = message;
			ResponseHeader header = (ResponseHeader) message.getHeader()
					.getOuter();
			this.values = (Object[]) header.getTimestamp();
		}

		/**
		 * Get either value of sequence instance
		 * 
		 * @return
		 */
		public Object get() {
			Object object = values[index];
			index++;
			return object;
		}

		public boolean isIncluded(AtomicService service) {
			return root.isRedundantServiceRequired(service, index);
		}

		public boolean isNestedService(AtomicService service) {
			return ((CompositeService) rootService.get())
					.isNestedOrderedService(service, index);
		}

		public Message getMessage() {
			((ResponseHeader) message.getHeader().getOuter())
					.setTimestamp(get());
			return message;
		}

		public int getCheckIndex() {
			int i = checkIndex;
			checkIndex++;
			return i;
		}

	}

}
