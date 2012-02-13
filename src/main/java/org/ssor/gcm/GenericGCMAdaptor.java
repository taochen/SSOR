package org.ssor.gcm;

import java.util.Collection;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ssor.CollectionFacade;
import org.ssor.Node;
import org.ssor.Region;
import org.ssor.ServiceManager;
import org.ssor.RegionDistributionManager;
import org.ssor.RegionDistributionSynchronyManager;
import org.ssor.invocation.InvocationCallback;
import org.ssor.protocol.Message;
import org.ssor.protocol.ProtocolManager;
import org.ssor.protocol.election.ElectionManager;
import org.ssor.protocol.replication.RequestHeader;
import org.ssor.protocol.tolerance.FTManager;
import org.ssor.util.Callback;
import org.ssor.util.Environment;
import org.ssor.util.Group;
import org.ssor.util.Util;

/**
 * Implement some passive functions
 * 
 * @author Tao Chen
 * 
 */
public abstract class GenericGCMAdaptor implements GCMAdaptor {

	private static final Logger logger = LoggerFactory
			.getLogger(GenericGCMAdaptor.class);
	protected ElectionManager electionManager;

	protected FTManager faultyManager;

	protected RegionDistributionManager regionDistributionManager;

	protected Group group;

	protected String path;
	
	protected Util util;
	// If false then use default tcp configuration
	protected boolean isUDP = true;
	
	protected RegionDistributionSynchronyManager regionDistributionSynchronyManager;
	
	protected Map<Class<?>, ProtocolManager> headersToManagers;
	
	protected Set<Class<?>> headersSet;

	public GenericGCMAdaptor(String groupName) {
		this.group = new Group(groupName, this);
		headersToManagers = group.getHeadersToManagers();
		headersSet = headersToManagers.keySet();
		faultyManager = (FTManager) group.getManager(org.ssor.protocol.tolerance.FT.class);
		electionManager = (ElectionManager) group.getManager(org.ssor.protocol.election.REP.class);
		regionDistributionManager = group.getRegionDistributionManager();
		regionDistributionSynchronyManager = group.getRegionDistributionSynchronyManager();
	}

	@Override
	public void receive(Message message, Object address) {

	
		int addressUUID = util.getUUIDFromAddress(address);
		message.setSrc(address);
		//System.out.print(address.hashCode() + " : " + Environment.UUID_ADDR + "************\n");
		try {
			
			for (Class<?> key : headersSet) {
				// A certain header can be handled by one manager only
				// if multiple protocol needs to involved in the same header
				// then it would be better to consider within the protocol (with the right command)
				// rather than the protocol manager.
				// 
				// This ensure a header would never be handled by two different protocol incorrectly
				if (key.isInstance(message.getHeader())) {
					headersToManagers.get(key).handleReceive(message, address, addressUUID);
					return;
				}
			}
			/*
			if(replicationManager.handleReceive(message, address, addressUUID)) return;
			else if(electionManager.handleReceive(message, address, addressUUID)) return;
			else if(faultyManager.handleReceive(message, address, addressUUID)) return;
			/*
			 * For first U, always has RequestHeader and ordered
			 * for 2nd U, always has ResponseHeader
			 * for final B, always has both RequestHeader and ResponseHeader
			 * for non-ordered B, always has RequestHeader and non-ordered
			 *
			if (header instanceof RequestHeader) {

				final RequestHeader requestheader = (RequestHeader) header;
				// Set requester into header so the isRequester function could
				// work
				if (requestheader.getRequester() == null)
					requestheader
							.setRequester(util.getUUIDFromAddress(address));
				// The receive of first U/B
				if ((!requestheader.isNonOrdered()) && requestheader.getOuter() == null)
					replicationManager.coordinate(message, address);
				// The receive of second B or
				// Non-ordered message
				else
					replicationManager.execute(message);

			} else if (header instanceof ResponseHeader) {

				((ResponseHeader)message.getHeader()).setSequencer(util.getUUIDFromAddress(address));
				// The receive of U
				replicationManager.acquireSequence(message);

			} else if (header instanceof InterestHeader
					&& util.getUUIDFromAddress(address) != group.getUUID_ADDR()) {
				
				
				// subsequence join node does not need to reply
				electionManager.assign(message, address);

			} else if (header instanceof DecisionHeader && group.isConsensus()) {

				electionManager.record(message, address);

			} else if (header instanceof org.ssor.protocol.election.AgreementHeader) {

				// If this is the subsequence join node, discard this
				if(group.isConsensus()){
					//viewSynchronizationManager.releaseOnAgreement(util.getUUIDFromAddress(address));
					return;
				}
				
				electionManager.doAgreement(message, address);

			} else if (header instanceof org.ssor.protocol.tolerance.AgreementHeader) {
				
				// If this is the subsequence join node, discard this
				if(group.isConsensus()){
					//viewSynchronizationManager.releaseOnRetransmission(util.getUUIDFromAddress(address), null);
					return;
				}
				
				faultyManager.doAgreement(message);
			} else if (header instanceof ConsensusHeader) {
				
				faultyManager.collect(message);
				
			}*/
		} catch (Throwable t) {
			logger.error("Handle receive message failed", t);
		}
	}

	public boolean isNeedNewThread(Message message) {
		// Use new thread on service associated with non-conflict region
		//return message.getHeader() instanceof RequestHeader && message.getHeader() instanceof ResponseHeader; 
		// Only for concurrent
		return message.getHeader() instanceof RequestHeader && message.getHeader().getOuter() != null
		&&((RequestHeader)message.getHeader()).isNonOrdered(); 
		//return false;
	}

	public void blockRelease(Message message, Object address) {
		
		/*
		if (message.getHeader() instanceof RequestHeader){
		System.out.print((message.getHeader() instanceof RequestHeader) + "\n");
		System.out.print((message.getHeader().getOuter() != null) + "\n");
		System.out.print((!((RequestHeader) message.getHeader()).isNonOrdered()) + "\n");
		}*/
		if (message.getHeader() instanceof RequestHeader
				&& message.getHeader().getOuter() != null
				&& !((RequestHeader) message.getHeader()).isNonOrdered()) {
			final RequestHeader requestheader = (RequestHeader) message
					.getHeader();
		
			// Set requester into header so the isRequester function could work
			requestheader.setRequester(util.getUUIDFromAddress(address));
			faultyManager.releaseCachedSequences(message);
		
		}
	}
	

	@Override
	public void memberLeave(int uuid, Collection<?> cache) {
		faultyManager.onCrashFailure(uuid, convertToUUIDQueueIncludingCurrentNode(cache));

	}

	@Override
	public void currentNodeJoin(Collection<?> cache) {
		electionManager.join(convertToUUIDQueue(cache));

	}
	public void initView(Collection<?> addresses, boolean isFirst, Callback callback) {

		

		if (group.isInit()) {

			synchronized (group.getCONSENSUS_LOCK()) {

				if (logger.isDebugEnabled()) {
					logger.debug("First view received, initialization starts");
				}
				// If this is the first node in the group
				if (isFirst) {

					if (logger.isDebugEnabled()) {
						logger
								.debug("This is the first node in group, thus create without election");
					}

					ServiceManager manager = group.getServiceManager();

					Node node = new Node(group.getUUID_ADDR(), addresses
							.iterator().next());

					// Set all regions are responsible by itself
					Set<Map.Entry<Integer, Region>> set = manager
							.getAllRegions();

					for (Map.Entry<Integer, Region> entry : set) {
						//System.out.print(entry.getValue() + "**********\n");
						entry.getValue().setSequencer(group.getUUID_ADDR());
						node.addRegion(entry.getKey());
					}

					regionDistributionManager.register(node, group.getUUID_ADDR());
					group.setInit(false);

					group.finishConsensus();

					regionDistributionSynchronyManager.releaseOnRetransmission(group.getUUID_ADDR(), null);
					
					return;
				}

				regionDistributionManager.clear();

				if (logger.isDebugEnabled()) {
					logger.debug("Copy info of current nodes from view");
				}

				int uuid = 0;
				for (Object address : addresses) {
					uuid = util.getUUIDFromAddress(address);
					// Do not add itself, since it would be added by the last B
					// of election
					if (group.getUUID_ADDR() != uuid)
						regionDistributionManager.register(new Node(uuid, address), group.getUUID_ADDR());
				}
				group.setInit(false);
				// Broadcast notification after the view has been recorded
				callback.run();
				
			}

		}

	}


	
	/**
	 * Used when this node join
	 * @param cache
	 * @return
	 */
	@SuppressWarnings("unchecked")
	protected Queue<Integer> convertToUUIDQueue(Collection<?> cache){
		
		
		final Queue<Integer> queue = CollectionFacade.getConcurrentQueue();
		for (Object address : cache){
			   if(group.getUUID_ADDR() != util.getUUIDFromAddress(address))
			      queue.add(util.getUUIDFromAddress(address));
		}
		return queue;
			
	}
	
	/**
	 * Used when nodes leaving
	 * @param cache
	 * @return
	 */
	@SuppressWarnings("unchecked")
	protected Queue<Integer> convertToUUIDQueueIncludingCurrentNode(Collection<?> cache){
		
		
		final Queue<Integer> queue = CollectionFacade.getConcurrentQueue();
		for (Object address : cache){
			   queue.add(util.getUUIDFromAddress(address));
		}
		return queue;
			
	}
	/**
	 * Only used this for unicast
	 * @param message
	 */
	protected void processPriorUnicast(final Message message){
		message.setViewSeqno(regionDistributionSynchronyManager
				.getViewSeqno());
	}
	
	
	
	public void processPriorReceive(final Message message, final Object src){
		

		if (isNeedNewThread(message)) {
			Environment.pool.execute(new Runnable() {

				@Override
				public void run() {
					Environment.isNew.set(true);
					receive(message, src);
					Environment.isNew.remove();
				}

			});
		} else {//if(!regionDistributionSynchronyManager.isMsgDeliverable()){
			processReceive(message, src);
			}
			/*
		} else {
			pool.execute(new Runnable() {

				@Override
				public void run() {
					regionDistributionSynchronyManager.suspendMsg(message);
					processReceive(message, src);
				}

			});
		}*/
	}
	
	private void processReceive(final Message message, final Object src){
		if(regionDistributionSynchronyManager.cacheMsgOnView(util.getUUIDFromAddress(src), message, message.getViewSeqno())){
			
			//if(message.getHeader() instanceof RequestHeader && message.getHeader().getOuter() instanceof ResponseHeader)
				//System.out.print("finish reqid: " + message.getReqId()+ "process: " +  ((ResponseHeader)message.getHeader().getOuter()).getTimestamp() + "\n");
			
			// For releasing the cached sequence on sequencer site
			blockRelease(message, src);
			receive(message, src);
		
			regionDistributionSynchronyManager.releaseMsgOnView(message);
		}
	}
	
	
	public void setUtil(Util util) {
		this.util = util;
		group.setUtil(util);
	}
	



	public void setPath(String path) {
		this.path = path;
	}

	public Group getGroup() {
		return group;
	}

	public void setUDP(boolean isUDP) {
		this.isUDP = isUDP;
	}

	public void setCallback(InvocationCallback callback) {
		group.setCallback(callback);
	}
	

}
