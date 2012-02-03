package org.ssor.util;

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
import org.ssor.gcm.CommunicationAdaptor;
import org.ssor.gcm.GCMAdaptor;
import org.ssor.invocation.InvocationCallback;
import org.ssor.invocation.ProxyFactory;
import org.ssor.protocol.Message;
import org.ssor.protocol.ProtocolStack;
import org.ssor.protocol.election.ElectionManager;
import org.ssor.protocol.replication.ReplicationManager;
import org.ssor.protocol.tolerance.FTManager;

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

	private ElectionManager electionManager;

	private ReplicationManager replicationManager;

	private FTManager faultyManager;

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
		replicationManager = new ReplicationManager(this);
		electionManager = new ElectionManager(this);
		faultyManager = new FTManager(this);
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

	public ElectionManager getElectionManager() {
		return electionManager;
	}

	public ReplicationManager getReplicationManager() {
		return replicationManager;
	}

	public FTManager getFaultyManager() {
		return faultyManager;
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

		// If suspending requests exist, then use it even there is smaller value
		// obtained through
		// state transfer, since the sequence after the given state may never
		// arrive due to state is transfered before any
		// VS constraints.
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

}
