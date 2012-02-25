package org.ssor.util;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ssor.CollectionFacade;
import org.ssor.ManagerBus;
import org.ssor.RequirementsAwareAdaptor;
import org.ssor.Sequence;
import org.ssor.ServiceManager;
import org.ssor.RegionDistributionManager;
import org.ssor.RegionDistributionSynchronyManager;
import org.ssor.State;
import org.ssor.conf.ClassConfigurator;
import org.ssor.exception.ConfigurationException;
import org.ssor.gcm.CommunicationAdaptor;
import org.ssor.gcm.GCMAdaptor;
import org.ssor.invocation.InvocationCallback;
import org.ssor.invocation.ProxyFactory;
import org.ssor.protocol.Message;
import org.ssor.protocol.Protocol;
import org.ssor.protocol.ProtocolManager;
import org.ssor.protocol.ProtocolStack;

/**
 * 
 * This class used to represent different group that the current node interested
 * in, each group maintains its own instances of requirements aware service,
 * invocation service, protocol stack and adaptor
 * 
 * @author Tao Chen
 * 
 */
public class Group implements ManagerBus, RequirementsAwareAdaptor,
		CommunicationAdaptor {

	private static final Logger logger = LoggerFactory.getLogger(Group.class);

	private boolean init = true;

	private Object CONSENSUS_LOCK = new Byte[0];

	private volatile boolean isConsensus = true;

	private GCMAdaptor adaptor;

	private ProtocolStack protocolStack;

	private ServiceManager serviceManager;

	private RegionDistributionManager regionDistributionManager;

	private String name;

	private Map<Class<?>, ProtocolManager> protocolsToManagers;
	
	private Map<Class<?>, ProtocolManager> headersToManagers;

	private RegionDistributionSynchronyManager regionDistributionSynchronyManager;

	private ProxyFactory proxyFactory;

	private InvocationCallback callback;

	// UUID of this node within this group
	private int UUID_ADDR;

	private Util util;

	private Map<Integer, State> states;

	private Map<String, State> sessionalStates;

	private int suspendedMsgNo = 0;

	@SuppressWarnings("unchecked")
	public Group(String name, GCMAdaptor adaptor) {
		super();
		// This is a fake configuration
		/*
		 * String[] classes = new String[]{ "org.ssor.protocol.election.REP",
		 * "org.ssor.protocol.tolerance.FT", "org.ssor.protocol.replication.AR",
		 * "org.ssor.protocol.replication.abcast.MSP", };
		 */
		this.adaptor = adaptor;

		proxyFactory = new ProxyFactory(this);

		serviceManager = new ServiceManager();
		regionDistributionManager = new RegionDistributionManager(this);
		regionDistributionSynchronyManager = new RegionDistributionSynchronyManager(
				this);
		protocolStack = new ProtocolStack(ClassConfigurator
				.getProtocolClasses());
		protocolsToManagers = CollectionFacade.getConcurrentHashMap();
		headersToManagers = CollectionFacade.getConcurrentHashMap();
		
		ProtocolManager manager = null;
		Class<?>[] headers = null;
		try {
			for (Protocol protocol : protocolStack.getProtocols()) {
				
				// The protocol may not need a corresponding protocol manager
				if (protocol.getClass().getAnnotation(org.ssor.annotation.ProtocolBinder.class) == null){
					if (logger.isDebugEnabled()) {
						logger.debug("Protocol " + protocol.getClass() + " does not have a dedicated manager");
					}
					continue;
				}
				
				manager = (ProtocolManager) protocol.getClass().getAnnotation(org.ssor.annotation.ProtocolBinder.class).managerClass().newInstance();
				manager.initializeProtoclManager(this);
				protocolsToManagers.put(protocol.getClass(), manager);
				
				if (logger.isDebugEnabled()) {
					logger.debug("Aassociating protocol " + protocol.getClass() + " with protocol manager " + manager.getClass());
				}
				
				// Initialize headers and protocol managers
				headers = protocol.getClass().getAnnotation(org.ssor.annotation.ProtocolBinder.class).headers();
				for (Class<?> header : headers) {
					
					if (headersToManagers.containsKey(header)) {
						throw new ConfigurationException("Header "  + header + " can not be associated with two protocols");
					}
					
					headersToManagers.put(header, manager);
					if (logger.isDebugEnabled()) {
						logger.debug("Aassociating header " + header + " with protocol manager " + manager.getClass());
					}
				}				
			}
		} catch (InstantiationException e) {
			logger.error("Binding protocol and its manager failed", e);
		} catch (IllegalAccessException e) {
			logger.error("Binding protocol and its manager failed", e);
		}
		states = CollectionFacade.getConcurrentHashMap();
		sessionalStates = CollectionFacade.getConcurrentHashMap();

		this.name = name;
	}

	public boolean isInit() {
		return init;
	}

	public void setInit(boolean init) {
		this.init = init;
	}

	public Object getCONSENSUS_LOCK() {
		return CONSENSUS_LOCK;
	}

	public boolean isConsensus() {
		return isConsensus;
	}

	public synchronized void finishConsensus() {

		// Notify the other suspended thread to carry on
		synchronized (CONSENSUS_LOCK) {
			protocolStack.finishConsensus();
			isConsensus = false;
			CONSENSUS_LOCK.notifyAll();
		}

		if (suspendedMsgNo == 0) {
			setState();
			suspendedMsgNo--;
		}

		if (logger.isInfoEnabled()) {
			logger.info("Consensus finished!");
		}
	}

	public void setGroup(GCMAdaptor adaptor) {
		this.adaptor = adaptor;
	}

	@Override
	public void blockCall(Object object) {
		// TODO Auto-generated method stub

	}

	@Override
	public void blockCall(Object object, Object address) {
		// TODO Auto-generated method stub

	}

	@Override
	public void multicast(Message message) {
		adaptor.multicast(message);

	}

	@Override
	public void unicast(Message message, Object address) {
		adaptor.unicast(message, address);
	}

	@Override
	public void receive(Message message, Object address) {
		// TODO Auto-generated method stub

	}

	public ProtocolStack getProtocolStack() {
		return protocolStack;
	}

	public ServiceManager getServiceManager() {
		return serviceManager;
	}

	public RegionDistributionManager getRegionDistributionManager() {
		return regionDistributionManager;
	}

	public String getName() {
		return name;
	}

	public ProtocolManager getManager (Class<?> clazz){
		return protocolsToManagers.get(clazz);
	}

	/**
	 * Return a read-only set of protocol managers, this is not thread-safe
	 * @return the set of protocol managers
	 */
	public Set<ProtocolManager> getManagers () {
		Iterator<Map.Entry<Class<?>, ProtocolManager>> itr = protocolsToManagers.entrySet().iterator();		
		Set<ProtocolManager> set = new HashSet<ProtocolManager>();
		while (itr.hasNext()) {
			set.add(itr.next().getValue());
		}
		
		return set;
	}
	
	/**
	 * Return a read-only set of all registered header classes and managers mapping, this is thread-safe
	 * @return the set of header classes and managers mapping
	 */
	public Map<Class<?>, ProtocolManager> getHeadersToManagers() {
        return headersToManagers;
	}
	
	public String toString() {
		return "(Group: " + name + ")";
	}

	public RegionDistributionSynchronyManager getRegionDistributionSynchronyManager() {
		return regionDistributionSynchronyManager;
	}

	public ProxyFactory getProxyFactory() {
		return proxyFactory;
	}

	public int getUUID_ADDR() {
		return UUID_ADDR;
	}

	public void setUUID_ADDR(int UUID_ADDR) {
		this.UUID_ADDR = UUID_ADDR;
		protocolStack.init(this);
	}

	public Util getUtil() {
		return util;
	}

	public void setUtil(Util util) {
		this.util = util;
	}

	public InvocationCallback getCallback() {
		return callback;
	}

	public void setCallback(InvocationCallback callback) {
		this.callback = callback;
	}

	public void setStates(State[] array) {

		for (State state : array) {
			System.out.print("region" + state.getRegion() + "transfer: "
					+ state.getSequence() + "\n");
			state.fromStateTransfer();
			if (state.getSessionId() == null)
				states.put(state.getRegion(), state);
			else
				sessionalStates.put(state.getSessionId(), state);
		}
	}

	/**
	 * If suspending requests exist, then use it even there is smaller value
	 * obtained through
	 * state transfer, since the sequence after the given state may never
	 * arrive due to state is transfered before any
	 * VS constraints.
	 * This is because the entire delivery process is not managed by GCM
	 * therefore it is possible that msg deliveried but enter the cacheMsgOnView
	 * after the view. It is a pure implementation issue.
	 * A potential solution for this is thread priority.
	 * 
	 * Since state has been guaranteed to be consistent with view delivery thus
	 * this is no longer needed
	 * @param serviceName
	 * @param sessionId
	 * @param sequence
	 */
	@Deprecated
	public void setState(String serviceName, String sessionId, Sequence sequence) {

		State state = null;
		if (sessionId == null)
			state = states.get(serviceManager.get(serviceName)
					.getRegionNumber());
		else
			state = sessionalStates.get(sessionId);
		System.out.print("State:" + state + "msg region: " + sequence
				+ "region: "
				+ serviceManager.get(serviceName).getRegionNumber() + "\n");
		if (state == null)
			return;
		
		if (state.isFromStateTransfer())
			state.setSequence(sequence);
		else {
			switch (sequence.compareTo(state.getSequence())) {

			case -1: {
				state.setSequence(sequence);
				break;
			}
			}
		}

	}

	public synchronized void increaseSuspendedNo() {
		suspendedMsgNo++;
	}

	public synchronized void isFinishSuspendMsgsProcess() {

		suspendedMsgNo--;
		if (suspendedMsgNo == 0) {
			setState();
			suspendedMsgNo--;
		}

	}

	private void setState() {
		Set<Map.Entry<Integer, State>> set = states.entrySet();
		for (Map.Entry<Integer, State> entry : set) {
			System.out.print("region: " + entry.getKey() + "final set: "
					+ entry.getValue().getSequence() + "\n");
			serviceManager.getRegion(entry.getKey()).setState(
					entry.getValue().getSequence());
			// Trigger is needed in case the expecting one is suspended before
			// the expected values are set
			serviceManager.getRegion(entry.getKey()).trigger(null);
		}

		Set<Map.Entry<String, State>> sessionalSet = sessionalStates.entrySet();
		for (Map.Entry<String, State> entry : sessionalSet) {
			serviceManager.getSessionRegion().setState(entry.getKey(),
					entry.getValue().getSequence());
			serviceManager.getSessionRegion().trigger(entry.getKey());
		}

		states.clear();
		sessionalStates.clear();
	}

	@Override
	public Group getGroup() {
		return this;
	}

}
