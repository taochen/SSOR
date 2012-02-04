package org.ssor.protocol.tolerance;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ssor.CollectionFacade;
import org.ssor.Node;
import org.ssor.Region;
import org.ssor.Sequence;
import org.ssor.AtomicService;
import org.ssor.SessionRegion;
import org.ssor.CompositeService;
import org.ssor.annotation.ProtocolManager;
import org.ssor.gcm.CommunicationAdaptor;
import org.ssor.listener.CommunicationListener;
import org.ssor.protocol.Command;
import org.ssor.protocol.Message;
import org.ssor.protocol.ProtocolSharableInstances;
import org.ssor.protocol.RequirementsAwareProtocol;
import org.ssor.protocol.Token;
import org.ssor.protocol.replication.BatchRequestHeader;
import org.ssor.protocol.replication.RequestHeader;
import org.ssor.protocol.replication.ResponseHeader;
import org.ssor.protocol.replication.abcast.DeliveryPacket;
import org.ssor.protocol.replication.abcast.SequenceVector;
import org.ssor.util.SequenceLinkedList;
import org.ssor.util.Triple;
import org.ssor.util.Tuple;
import org.ssor.util.Util;

@ProtocolManager(managerClass=org.ssor.protocol.tolerance.FTManager.class)
public class FT extends RequirementsAwareProtocol implements
		CommunicationListener, ProtocolSharableInstances {

	private static final Logger logger = LoggerFactory.getLogger(FT.class);

	/*
	 * Cache the sequences that has been assigned (means for triggering the
	 * agreement phase), for fault tolerance, if no faulty, then it should be
	 * removed after receive of the last broadcast, <node UUID, list of
	 * sequence>, this is only done on sequencer site
	 * 
	 */
	private Map<Integer, SequenceLinkedList> assignedSequences;
	/*
	 * Cache the sequences that has been executed (means for triggering the
	 * execute phase), for fault tolerance, if no faulty, then it should be
	 * removed after receive of the sequence assignment, <region number, list of
	 * sequence>, this is only done on requester/local site
	 * 
	 * Due to this is normally immrotable, thus it can be initilized when create
	 * the instance
	 */
	private Map<Integer, SequenceLinkedList> executedSequences;

	/*
	 * The cache of sequences until all have been collected <region number,
	 * Tuple<view id, region instance>>
	 */
	private Map<Integer, Tuple<Integer, Region>> collectedSequences;

	// The same instance as the one in MSP
	private Map<String, Message> sentMessages;

	private CommunicationAdaptor adaptor;

	@SuppressWarnings("unchecked")
	public FT() {

		assignedSequences = CollectionFacade.getConcurrentHashMap(100);
		executedSequences = CollectionFacade.getConcurrentHashMap(100);
		collectedSequences = CollectionFacade.getConcurrentHashMap(100);

	}

	@Override
	public Token down(short command, Token value) {

		switch (command) {

		case Command.FT_RELEASE_SEQUENCE: {

			if (value == null)
				return doDown(command, value);

			if (logger.isTraceEnabled()) {
				trace(logger, "FT_RELEASE_SEQUENCE",
						"Release assigned sequences on sequencer");
			}
			/*
			 * Filter has been done outside, in the fault tolerance manager
			 */

			final Message message = (Message) value.getData();

			final RequestHeader header = (RequestHeader) message.getHeader();
			final AtomicService service = serviceManager.get(header
					.getService());

			if (service instanceof CompositeService) {

				final CompositeService triggerService = (CompositeService) service;
				// Remove as a sequencer or all requests it sent
				releaseAssignedSequenceCache(header.getRequester(),
						triggerService.getServices(),
						(Sequence[]) ((ResponseHeader) header.getOuter())
								.getTimestamp());
			} else {

				// Remove as a sequencer
				releaseAssignedSequenceCache(header.getRequester(), service,
						(Sequence) ((ResponseHeader) header.getOuter())
								.getTimestamp());

			}
			return doDown(command, value);
			// Trigger by the node that just assign region to a new joined node,
			// during agreement phase of election protocol
		}
		case Command.FT_RELEASE_WHEN_JOIN: {

			if (logger.isTraceEnabled()) {
				trace(logger, "FT_RELEASE_WHEN_JOIN",
						"Release assigned sequences as the old sequencer");
			}

			// Clean up when there is a new sequencer show up
			for (SequenceLinkedList list : assignedSequences.values())
				list.removeByRegionNumber((Integer) value.getData());

			return null;

		}
		case Command.FT_FINAL_BROADCAST: {

			if (value == null)
				return doDown(command, value);

			FaultySequence[] faulty = null;
			AgreementHeader header = null;
			Message message = null;
			// On old sequencer, which does not crash
			if (value.getData() instanceof Integer) {

				if (logger.isTraceEnabled()) {
					trace(logger, "FT_FINAL_BROADCAST",
							"Send the assigned sequence of the faulty node, as the seqeucner");
				}

				SequenceLinkedList queue = assignedSequences
						.remove((Integer) value.getData());

				// No need for agreement if there is no cache
				if (queue == null || queue.size() == 0) {

					if (logger.isTraceEnabled()) {
						trace(logger, "FT_FINAL_BROADCAST",
								"No assigned sequence found on this sequencer, for crash node: "
										+ value);
					}

					return doDown(command, null);
				}
				faulty = new FaultySequence[queue.size()];
				for (int i = 0; i < faulty.length; i++)
					faulty[i] = new FaultySequence(queue.poll());

				header = new AgreementHeader(faulty, false);

				// On new sequencer, will require consensus process
			} else {

				if (logger.isTraceEnabled()) {
					trace(
							logger,
							"FT_FINAL_BROADCAST",
							"Send the assigned sequence of faulty nodes after consensus, as the new seqeucner");
				}
				@SuppressWarnings("unchecked")
				final Tuple<Integer, List<Sequence>> tuple = (Tuple<Integer, List<Sequence>>) value
						.getData();

				List<Sequence> sequences = tuple.getVal2();

				faulty = new FaultySequence[sequences.size()];

				for (int i = 0; i < faulty.length; i++) {
					System.out.print("skipped target: " + sequences.get(i)
							+ "\n");
					faulty[i] = new FaultySequence(sequences.get(i));
				}
				header = new AgreementHeader(faulty, true, tuple.getVal1());
			}
			adaptor.multicast(message = new Message(header, null, false));

			value.setDataForNextProtocol(message);

			return doDown(command, value);
		}
		case Command.FT_UNICAST: {

			if (value == null)
				return doDown(command, value);

			// Should reply even there is no cache

			final Message original = (Message) value.getData();

			Message message = null;
			final Integer[] regions = (Integer[]) original.getBody();
			final ConsensusHeader header = new ConsensusHeader(regions);

			if (logger.isTraceEnabled()) {
				trace(logger, "FT_UNICAST",
						"Receive consensus request from the new sequencer, which responsible for: "
								+ regions.length + " regions");
			}

			/*
			 * Does not remove, since they would be removed when receive the
			 * last broadcast which triggered by this node itself (but the last
			 * broadcast should be suspended during this consensus)
			 */
			SequenceLinkedList list = null;
			for (Integer number : regions) {
				list = executedSequences.get(number);
				if (logger.isDebugEnabled()) {
					logger
							.debug("Receive region: "
									+ number
									+ ", now extract the agreed sequences sent by this node");
				}
				// synchronized (list) {
				// The change would be sync with the change state
				// list.setConsensus(true);
				header.addTripleValue(serviceManager.getRegion(number), list
						.getAllForConsensus());
				// }
			}
			adaptor.unicast(message = new Message(header, null, false),
					original.getSrc());

			value.setDataForNextProtocol(message);
			return doDown(command, value);
		}

		case Command.FT_RETRANSMIT: {

			if (value == null)
				return doDown(command, value);

			@SuppressWarnings("unchecked")
			final Triple<Set<Region>, Object, Integer> triple = (Triple<Set<Region>, Object, Integer>) value
					.getData();

			RequestHeader header = null;
			Message message = null;
			Integer[] retransmitedIndexList = null;
			final Set<Map.Entry<String, Message>> entries = sentMessages
					.entrySet();

			final Set<Message> retransmitedSet = new HashSet<Message>();

			for (Map.Entry<String, Message> entry : entries) {
				message = entry.getValue();
				header = (RequestHeader) message.getHeader();

				// If composed service
				if (message.getSequenceCache() != null
						&& message.getSequenceCache().isNeedRetransmission()) {

					if (logger.isTraceEnabled()) {
						trace(logger, "FT_RETRANSMIT",
								"Retransmission needed for composed service: "
										+ header.getService());
					}

					retransmitedIndexList = header.getNestedBelongingRegion(
							triple.getVal1(), message.getSequenceCache()
									.getWaitingIndices(), serviceManager
									.get(header.getService()));
					Message cached = new Message(new BatchRequestHeader(header,
							retransmitedIndexList), null, false);
					cached.setReqId(message.getReqId());
					retransmitedSet.add(cached);
				} else if (message.isNeedRetransmited()
						&& !serviceManager.get(header.getService()).getRegion()
								.isAllowRequest()
						&& triple.getVal1().contains(
								serviceManager.get(header.getService())
										.getRegion())) {

					if (logger.isTraceEnabled()) {
						trace(logger, "FT_RETRANSMIT",
								"Retransmission needed for service: "
										+ header.getService());
					}
					header.setOuter(null);
					// Get rid of the arguments of service
					Message cached = new Message(header, null, false);
					cached.setReqId(message.getReqId());
					retransmitedSet.add(cached);

				}

			}

			for (Message msg : retransmitedSet) {
				System.out.print("111ret: " + msg.getReqId() + "\n");
				adaptor.unicast(msg, triple.getVal2());
			}
			/*
			 * We then unblock the suspend request and set the flag up
			 */

			System.out.print(triple.getVal1().size() + "***size\n");
			System.out.print(retransmitedSet.size() + "re size\n");
			// SequenceLinkedList sequenceList = null;
			// Notify null sessional region
			for (Region reg : triple.getVal1()) {

				// sequenceList = executedMessages.get(reg.getRegion());
				// Unblock so the incoming timestamp can be accepted

				if (!reg.isAllowRequest()) {

					if (logger.isTraceEnabled()) {
						trace(logger, "FT_RETRANSMIT",
								"Allow requests to be handled for region: "
										+ reg);
					}
					System.out.print("region: " + reg.getRegion() + "\n");
					reg.allowRequest();

					if (reg instanceof SessionRegion)
						((SessionRegion) reg).triggerAll();
					else
						reg.trigger(null);

				}

			}

			// This would release join view changing
			if (triple.getVal3() == null)
				regionDistributionSynchronyManager.releaseOnRetransmission(util
						.getUUIDFromAddress(triple.getVal2()), null);
			else
				regionDistributionSynchronyManager.releaseOnRetransmission(
						triple.getVal3(), util.getUUIDFromAddress(triple
								.getVal2()));

			value.setDataForNextProtocol(retransmitedSet);
			return doDown(command, value);
		}

			/*
			 * 
			 * if(value == null) doDown(command, value);
			 * 
			 * Integer regionNumber = (Integer) value; Collection<SequenceLinkedList>
			 * values = agreedMessages.values(); for(SequenceLinkedList list :
			 * values) list.removeByRegionID(regionNumber);
			 * 
			 * return null; }
			 */

		}

		return doDown(command, value);
	}

	@SuppressWarnings("unchecked")
	@Override
	public Token up(final short command, final Token value) {

		switch (command) {

		case Command.ABCAST_COORDINATE: {

			if (value == null)
				return doUp(command, value);

			if (logger.isTraceEnabled()) {
				trace(logger, "ABCAST_COORDINATE",
						"Cache the assigned sequence on sequencer");
			}

			if (Token.REPLICATION_COORDINATE_NOT_SEQUENCER == value
					.getNextAction())
				return doUp(command, value);

			@SuppressWarnings("unchecked")
			final Tuple<Integer, Object> tulpe = (Tuple<Integer, Object>) value
					.getDataForNextProtocol();

			value.setDataForNextProtocol(createAssignedSequenceCache(tulpe
					.getVal1(), tulpe.getVal2()));
			return doUp(command, value);

		}
		case Command.ABCAST_ACQUIRE_SEQUENCE: {

			if (value == null)
				return doUp(command, value);

			if (logger.isTraceEnabled()) {
				trace(logger, "ABCAST_EXECUTE",
						"Cache the sent sequence on requester");
			}

			@SuppressWarnings("unchecked")
			final Tuple<Tuple<String, Message>, Tuple<Integer, Object>> tuple = (Tuple<Tuple<String, Message>, Tuple<Integer, Object>>) value
					.getDataForNextProtocol();

			final Tuple<String, Message> nestedTuple = tuple.getVal1();

			value.setDataForNextProtocol(createExecutedSequenceCache(
					nestedTuple.getVal1(), tuple.getVal2(), nestedTuple
							.getVal2()));

			// The result of the next action was decided by replicating layer
			return doUp(command, value);

		}
		case Command.FT_AGREEMENT: {

			if (value == null)
				return doUp(command, value);

			if (logger.isTraceEnabled()) {
				trace(logger, "FT_AGREEMENT",
						"Receive agreed sequences that needs to be skipped");
			}

			final AgreementHeader header = (AgreementHeader) ((Message) value
					.getData()).getHeader();
			final FaultySequence[] sequences = header.getFaulty();

			// Service should be all meta service, here we directly use the
			// region
			// since there is no need to access it though service
			Region region = null;
			Sequence sequence = null;
			Set<Region> set = new HashSet<Region>();

			for (FaultySequence faulty : sequences) {
				sequence = faulty.getSequence();
				region = serviceManager.getRegion(sequence.getRegionNumber());

				if (logger.isDebugEnabled()) {
					logger.debug("Receive agreed sequences: " + sequence
							+ " for region: " + sequence.getRegionNumber());
				}

				// This is region based, thus only mark once
				if (!set.contains(region))
					executedSequences.get(sequence.getRegionNumber())
							.markSequence();

				set.add(region);
				// System.out.print("skip: " + sequence + "\n");
				region.addSkippedSequence(sequence);

			}

			/*
			 * In case for a region there is no sequence needs to be agreed on,
			 * but there is need to release the corresponding suspending
			 * messages
			 * 
			 * this can be optimized since it included unnecessary regions, suhc
			 * as those do not crash region would suffer one extra notify
			 * function
			 */
			Integer[] regionNumbers = regionDistributionManager.get(
					util.getUUIDFromAddress(((Message) value.getData())
							.getSrc())).getResponsibleRegions();
			for (Integer regionNumber : regionNumbers) {
				// This would auto-get ride of duplicate elements
				set.add(serviceManager.getRegion(regionNumber));
			}

			// If this is tolerance of non-sequencer, then release the lock
			if (!header.isTriggerRetransmission()) {

				for (Region releasedRegion : set) {

					if (releasedRegion instanceof SessionRegion)
						((SessionRegion) releasedRegion).triggerAll();
					else
						releasedRegion.trigger(null);

				}
			}

			if (header.isTriggerRetransmission())
				value.setNextAction(Token.FT_AGREEMENT_RETRANSMISSION);

			value
					.setDataForNextProtocol(new Triple<Set<Region>, Object, Integer>(
							set, null, header.getViewId()));

			// The Object can be either boolean or address
			return doUp(command, value);
		}
		case Command.FT_COLLECT: {

			if (value == null)
				return doUp(command, value);

			if (logger.isTraceEnabled()) {
				trace(logger, "FT_COLLECT",
						"Receive voted sequences that sent by each node");
			}

			final Message message = (Message) value.getData();
			final ConsensusHeader header = (ConsensusHeader) message
					.getHeader();

			Tuple<Integer, List<Sequence>> tuple = collectSequence(header
					.getTripleArray(), message.getSrc());
			if (header.getSessionalTripleArray() != null) {

				if (tuple == null)
					tuple = collectSessionalSequence(header
							.getSessionalTripleArray(), message.getSrc());
				else
					tuple.getVal2().addAll(
							collectSessionalSequence(
									header.getSessionalTripleArray(),
									message.getSrc()).getVal2());
			}

			if (tuple != null)
				down(Command.FT_FINAL_BROADCAST, new Token(tuple));
			return doUp(command, value);

		}
		case Command.FT_AFTER_ABCAST_AGREEMENT: {

			if (value == null)
				return doUp(command, value);

			if (logger.isTraceEnabled()) {
				trace(logger, "FT_AFTER_ABCAST_AGREEMENT",
						"Release the sent sequence on requester, after execution complete");
			}

			/*
			 * 
			 * Filter has been done outside, in the fault tolerance manager
			 */

			final DeliveryPacket packet = (DeliveryPacket) value
					.getDataForNextProtocol();

			if (packet.getService() instanceof CompositeService) {

				final CompositeService triggerService = (CompositeService) packet
						.getService();
				// Remove all requests it sent
				releaseExecutedSequenceCache(triggerService.getServices(),
						(Sequence[]) packet.getSequence());
			} else {
				// Remove all requests it sent
				releaseExecutedSequenceCache(packet.getService(),
						(Sequence) packet.getSequence());
			}
			return doUp(command, value);

		}
		case Command.INSTALL_VIEW_ON_LEAVING: {

			if (value == null)
				return doUp(command, value);

			@SuppressWarnings("unchecked")
			final Tuple<Integer, Queue<Integer>> tuple = (Tuple<Integer, Queue<Integer>>) value
					.getData();

			final Node node = regionDistributionManager.remove(tuple.getVal1());

			// If things perform correctly, this should be always non-null,
			// however, in case a node startup and down immediately (e.g
			// join-crash)
			// even before it is recorded, then this may be null.
			if (node == null)
				return null;
			// This would move the region to responsible regions, so the
			// sequencer can be re-elected
			// and also release the view if it is due to suspend

			node
					.releaseViewsWhenThisNodeCrash(regionDistributionSynchronyManager);
			// If the faulty node is the new joining node, and this node just
			// gave some regions to it
			Integer[] regionNumbers = node.getUncompletedRegion();
			Region region = null;
			if (regionNumbers.length != 0) {

				if (logger.isTraceEnabled()) {
					trace(
							logger,
							"INSTALL_VIEW_ON_LEAVING",
							"The crash node: "
									+ tuple.getVal1()
									+ " having been assigned regions, which was originally responsible by this node, therefore it adjust it back without agreement");
				}

				for (Integer regionNumber : regionNumbers) {

					region = serviceManager.getRegion(regionNumber);
					// Set it back
					// synchronized(region){
					region.setSequencer(UUID_ADDR);
					// }
				}

				// Broadcast to other nodes in order to release this view
				// group.multicast(new Message(null, tuple.getVal1(), false));

				// No need to run consensus
				return null;

			}

			// Release the change view for current node locally, in case its
			// consensus is not finished yet
			boolean isAddToQueue = regionDistributionSynchronyManager
					.releaseOnCrashDuringConsensus(tuple.getVal1());

			// If true means it is not join-crash node
			if (isAddToQueue) {

				if (logger.isTraceEnabled()) {
					trace(logger, "INSTALL_VIEW_ON_LEAVING",
							"Notify crash of node: " + tuple.getVal1()
									+ ", then execute the normal FT protocol");
				}

				// Avoid dead lock on collect phase of consensus
				crashDuringConsensus(tuple.getVal1());

				// Notify the other protocols who interests in the protocol
				// stack
				// / Avoid dead lock on election
				doUp(command, value);

				regionNumbers = node.getResponsibleRegions();
				// SequenceLinkedList list = null;
				final List<Integer> interests = new LinkedList<Integer>();
				// <Node ID, list of assigned regions>
				final Map<Integer, List<Integer>> allocation = new HashMap<Integer, List<Integer>>();
				List<Integer> list = null;
				if (null != regionNumbers && regionNumbers.length != 0) {
					regionDistributionSynchronyManager.recordLeave(tuple
							.getVal1());

					// Node ID
					int uuid = -1;

					for (int regionNumber : regionNumbers) {
						region = serviceManager.getRegion(regionNumber);

						/*
						 * list = executedMessages.get(regionNumber);
						 * synchronized (region){ region.setAllowRequest(false); //
						 * Sync with the adding of receiving sequence, from old
						 * sequencer //synchronized (list){
						 * region.setSequencer(-1); //} }
						 */

						uuid = regionDistributionManager
								.getNextSequencer(regionNumber);

						// If this node willing to become the new sequencer
						if (UUID_ADDR == uuid) {
							interests.add(regionNumber);
							region.setConsensusView(tuple.getVal2());

							collectedSequences.put(regionNumber, new Tuple(node
									.getUUID_ADDR(), region));
							// Release early collection msg
							synchronized (region){
								region.notifyAll();
							}

							if (logger.isTraceEnabled()) {
								trace(logger, "INSTALL_VIEW_ON_LEAVING",
										"This node willing to become the next sequencer for region: "
												+ regionNumber);
							}

						} else {
							regionDistributionManager
									.get(uuid)
									.addNewResposibleRegion(
											region.getSequencer(), regionNumber);

							if (logger.isTraceEnabled()) {
								trace(
										logger,
										"INSTALL_VIEW_ON_LEAVING",
										"The node: "
												+ uuid
												+ " willing to become the next sequencer for region: "
												+ regionNumber
												+ ", the old sequencer:  "
												+ region.getSequencer());
							}
						}

						if ((list = allocation.get(uuid)) == null)
							allocation.put(uuid,
									list = new LinkedList<Integer>());

						list.add(regionNumber);
						regionDistributionSynchronyManager
								.addDecidedSequencerOnViewChange(node
										.getUUID_ADDR(), uuid);
					}

				}

				Token token = new Token(new Tuple<Node, List<Integer>>(node,
						interests));
				token.setDataForNextProtocol(allocation);
				// Ensure the message can be sent, outside current view,
				// protocol synchrony is maintained as well
				new LeavingProtocolThread(Command.SUSPECT_NOTIFY, token)
						.start();
			} else if (logger.isTraceEnabled()) {
				trace(
						logger,
						"SUSPECT_NOTIFY",
						"Notify crash of node: "
								+ tuple.getVal1()
								+ ", which is join-crash node, therefore does not need consensus");
			}

			return doUp(command, value);
		}
			// Sync with RDS
		case Command.SUSPECT_NOTIFY: {

			if (value == null)
				return doUp(command, value);

			@SuppressWarnings("unchecked")
			final Tuple<Node, List<Integer>> tuple = (Tuple<Node, List<Integer>>) value
					.getData();

			final Node node = tuple.getVal1();

			if (logger.isTraceEnabled()) {
				trace(logger, "SUSPECT_NOTIFY", "Node: " + node
						+ " crash and has been exclude from current view");
			}

			Integer[] regionNumbers = node.getResponsibleRegions();
			// If the crash node does not responsible for any regions or not
			// under consensus for any region
			// then execute the protocol immediately
			if (null == regionNumbers || regionNumbers.length == 0) {

				if (logger.isTraceEnabled()) {
					trace(
							logger,
							"SUSPECT_NOTIFY",
							"Node: "
									+ node
									+ " has no resiponsible regions, therefore exclude it immediately");
				}

				// ViewSynchronizationManager.releaseOnCrashWhenNormalNode(tuple.getVal1().getUUID_ADDR());
				return down(Command.FT_FINAL_BROADCAST, new Token(node
						.getUUID_ADDR()));
			}
			// Maintain view order, extends to protocol scope
			regionDistributionSynchronyManager.suspend(node.getUUID_ADDR());

			// Run the protocol as a sequencer
			down(Command.FT_FINAL_BROADCAST, new Token(node.getUUID_ADDR()));

			// value.setDataForNextProtocol(tuple.getVal2().size() == 0 ? null :
			// tuple
			// .getVal2().toArray(new Integer[tuple.getVal2().size()]));
			// This would trigger the 2nd broadcast of REP if the list of
			// regions is not empty
			return doUp(command, value);

		}

		}
		return doUp(command, value);
	}

	@Override
	public void setUtil(Util util) {
		this.util = util;

	}

	/**
	 * The cached sequences should be released only after the execution of
	 * service!
	 * 
	 * When FT for retransmission, setIsNeedRetransmited = true means a meta
	 * service needs retransmission while composed service would be decided by
	 * waitingIndic list in the TriggerOrder instance.
	 * 
	 * @param serviceName
	 * @param sequence
	 * @return
	 */
	private Object createExecutedSequenceCache(String serviceName,
			Tuple<Integer, Object> tuple, Message message) {

		Object sequence = tuple.getVal2();

		if (sequence == null)
			return null;

		SequenceLinkedList list = null;
		Region region = null;
		AtomicService service = serviceManager.get(serviceName);

		int id = tuple.getVal1();

		if (sequence instanceof Sequence) {

			// Used the instance of region as lock, since consensus aims to
			// block everything before finish
			region = service.getRegion();
			// This should not sync concurrency but sync between this and the
			// adding of new message

			list = executedSequences.get(service.getRegionNumber());
			// Set region number on the cached sequence
			((Sequence) sequence).setRegionNumber(service.getRegionNumber());
			// synchronized (list) {

			if (!region.isSequencer(id)) {

				if (logger.isDebugEnabled()) {
					logger.debug("Receive sequence from sequencer for region: "
							+ region + ", sequence: " + sequence
							+ ", but it is discard due to the sequencer crash");
				}

				return null;
			}

			list.add(sequence);
			message.setIsNeedRetransmited(false);
			// sentMessages.remove(message.getReqId());
			// }

		} else {

			final SequenceVector[] vectors = (SequenceVector[]) sequence;

			final CompositeService trigger = (CompositeService) service;
			Integer number = null;
			/*
			 * If this is composed service, then decompose it and cache it for
			 * each single nested service
			 */
			AtomicService nestedService = null;

			for (SequenceVector vector : vectors) {

				nestedService = trigger.getServices()[vector.getIndex()];
				// Used the instance of region as lock, since consensus aims to
				// block everything before finish
				region = nestedService.getRegion();
				// This should not sync concurrency but sync between this and
				// the adding of new message

				number = nestedService.getRegionNumber();
				list = executedSequences.get(number);

				// synchronized (list) {
				// If the sequencer has been changed
				if (!region.isSequencer(id)) {
					// Remove the one and put it into un-receive list, so it
					// can be retransmitted
					message.getSequenceCache().remove(vector);
					if (logger.isDebugEnabled()) {
						logger
								.debug("Receive sequence from sequencer for region: "
										+ region
										+ ", sequence: "
										+ sequence
										+ ", but it is discard due to the sequencer crash");
					}

				} else {
					// Set region number on the cached sequence
					vector.getSequence().setRegionNumber(number);
					list.add(vector.getSequence());
				}
				// }

			}

		}

		if (logger.isDebugEnabled()) {
			logger.debug("Caches sequence got from sequencer for service: "
					+ service + ", sequence: " + sequence);
		}

		return sequence;
	}

	private void releaseExecutedSequenceCache(AtomicService service,
			Sequence sequence) {

		// Concurrent sequence would be removed by the subsequent sequence
		if (sequence.getConcurrentno() != null
				&& sequence.getConcurrentno() == -1)
			return;

		SequenceLinkedList list = executedSequences.get(service
				.getRegionNumber());
		/*
		 * We use iteration here, can be optimized
		 */
		if (sequence.getConcurrentno() == null)
			list.remove(sequence);
		else
			list.removeWithConcurrentSequences(sequence);

		if (logger.isDebugEnabled()) {
			logger.debug("Remove from executed sequence cache, sequence: "
					+ sequence);
		}
	}

	private void releaseExecutedSequenceCache(AtomicService[] nestedService,
			Sequence[] sequences) {

		for (int i = 0; i < nestedService.length; i++) {
			releaseExecutedSequenceCache(nestedService[i], sequences[i]);
		}
		if (logger.isDebugEnabled()) {
			logger
					.debug("Remove from executed sequence cache on a composed service");
		}
	}

	private Object createAssignedSequenceCache(int uuid, Object sequence) {

		if (sequence == null)
			return null;

		SequenceLinkedList list = null;
		if (assignedSequences.containsKey(uuid))
			list = assignedSequences.get(uuid);
		else {
			list = new SequenceLinkedList();
			assignedSequences.put(uuid, list);
		}

		list.add(sequence);

		if (logger.isDebugEnabled()) {
			logger.debug("Sequencer cached sequence for node: " + uuid
					+ ", sequence: " + sequence);
		}

		return sequence;
	}

	private void releaseAssignedSequenceCache(int uuid, AtomicService service,
			Sequence sequence) {

		if (!service.isSequencer(UUID_ADDR))
			return;

		SequenceLinkedList list = assignedSequences.get(uuid);

		/*
		 * We use iteration here, can be optimized this may be null if this is
		 * assigned by original sequencer but arrive right after the region was
		 * moved to here
		 */
		if (list != null)
			list.remove(sequence);

		if (logger.isDebugEnabled()) {
			logger.debug("Sequencer removes assigned sequence cache for node: "
					+ uuid + ", sequence: " + sequence);
		}
	}

	private void releaseAssignedSequenceCache(int uuid,
			AtomicService[] nestedService, Sequence[] sequences) {

		for (int i = 0; i < nestedService.length; i++) {
			releaseAssignedSequenceCache(uuid, nestedService[i], sequences[i]);
		}
		if (logger.isDebugEnabled()) {
			logger.debug("Sequencer removes assigned seqeunce cache for node: "
					+ uuid + ", on a composed service");
		}
	}

	public void init() {

		final Set<Map.Entry<Integer, Region>> set = serviceManager
				.getAllRegions();
		for (Map.Entry<Integer, Region> entry : set)
			executedSequences.put(entry.getKey(), new SequenceLinkedList());

	}

	private Object crashDuringConsensus(int id) {

		final Set<Map.Entry<Integer, Tuple<Integer, Region>>> set = collectedSequences
				.entrySet();

		List<Sequence> list = null;
		Integer viewId = null;
		Region region = null;
		for (Map.Entry<Integer, Tuple<Integer, Region>> entry : set) {

			region = entry.getValue().getVal2();
			if (region.isContainInView(id)) {

				if (logger.isDebugEnabled()) {
					logger.debug("A node is removed from queue of region: "
							+ region
							+ ", in the presence of existing sequencer crash");
				}

				if (region.isSequenceCollectionFinished()) {

					Tuple<Integer, Region> tuple = collectedSequences.remove(entry.getKey());
					if(tuple != null)
					  viewId = tuple.getVal1();
					if (logger.isDebugEnabled()) {
						logger
								.debug("The sent sequences of existing correct nodes for region: "
										+ region
										+ " has been collected completely");
					}

					// Unblock acceptance of sequence request, this need FIFO
					// property
					// synchronized(region){
					// region.setSequencerConsensus(false);
					// }
					// This would trigger a broadcast
					if (list == null)
						list = new LinkedList<Sequence>();
					list.addAll(region.extractSkippedSequences());
				}

			}

			if (list != null)
				down(Command.FT_FINAL_BROADCAST, new Token(
						new Tuple<Integer, List<Sequence>>(viewId, list)));

		}

		return null;

	}

	private Tuple<Integer, List<Sequence>> collectSequence(
			Triple<Integer, Sequence, Sequence[]>[] triples, Object address) {

		Region region = null;
		Sequence seq = null;

		List<Sequence> list = null;
		int uuid = util.getUUIDFromAddress(address);
		Integer viewId = null;
		// Running consensus process
		for (Triple<Integer, Sequence, Sequence[]> triple : triples) {

			if (collectedSequences.containsKey(triple.getVal1()))
				region = collectedSequences.get(triple.getVal1()).getVal2();
			// Only for join consensus
			else
				collectedSequences.put(triple.getVal1(),
						new Tuple<Integer, Region>(null,
								region = serviceManager.getRegion(triple
										.getVal1())));

			seq = triple.getVal2();
			region.addCollectedSequences(null, uuid, triple.getVal3(), seq);

			if (logger.isTraceEnabled()) {
				trace(logger, "FT_COLLECT", "Collect sequences for region: "
						+ region);
			}

			// Add to assgined cache as a sequencer
			createAssignedSequenceCache(uuid, triple.getVal3());
			// If all tokens have been collected
			if (region.isSequenceCollectionFinished()) {

				collectionSuspend(triple.getVal1());
				Tuple<Integer, Region> tuple = collectedSequences.remove(triple.getVal1());
				if(tuple != null)
				  viewId = tuple.getVal1();
				

				if (logger.isTraceEnabled()) {
					trace(logger, "FT_COLLECT", "Sequences for region: "
							+ region + " has been collected completetly");
				}
				// Unblock acceptance of sequence request, this need FIFO
				// property
				// synchronized(region){
				// region.setSequencerConsensus(false);
				// }
				// This would trigger a broadcast
				if (list == null)
					list = new LinkedList<Sequence>();
				list.addAll(region.extractSkippedSequences());

			}

		}

		if (list != null)
			return new Tuple<Integer, List<Sequence>>(viewId, list);

		return null;

	}

	private Tuple<Integer, List<Sequence>> collectSessionalSequence(
			Triple<String, Sequence, Sequence[]>[] triples, Object address) {

		Region region = null;
		Sequence seq = null;

		List<Sequence> list = null;
		int uuid = util.getUUIDFromAddress(address);
		Integer viewId = null;
		// Running consensus process
		for (Triple<String, Sequence, Sequence[]> triple : triples) {

			if (collectedSequences.containsKey(SessionRegion.SESSION_REGION))
				region = collectedSequences.get(SessionRegion.SESSION_REGION)
						.getVal2();
			// Only for join consensus
			else
				collectedSequences.put(SessionRegion.SESSION_REGION,
						new Tuple<Integer, Region>(null,
								region = serviceManager.getSessionRegion()));

			seq = triple.getVal2();
			region.addCollectedSequences(triple.getVal1(), uuid, triple
					.getVal3(), seq);

			if (logger.isTraceEnabled()) {
				trace(logger, "FT_COLLECT",
						"Collect sequences for session region: " + region);
			}

			// Add to assigned cache as a sequencer
			createAssignedSequenceCache(uuid, triple.getVal3());

			// If all tokens have been collected
			if (region.isSequenceCollectionFinished()) {

				collectionSuspend(SessionRegion.SESSION_REGION);
				viewId = collectedSequences.remove(SessionRegion.SESSION_REGION).getVal1();
			

				if (logger.isTraceEnabled()) {
					trace(logger, "FT_COLLECT",
							"Sequences for session region: " + region
									+ " has been collected completetly");
				}
				// Unblock acceptance of sequence request, this need FIFO
				// property
				// synchronized(region){
				// region.setSequencerConsensus(false);
				// }
				// This would trigger a broadcast
				if (list == null)
					list = new LinkedList<Sequence>();
				list.addAll(region.extractSkippedSequences());

			}

		}

		if (list == null && triples.length == 0) {

			region = serviceManager.getSessionRegion();
			region.isContainInView(uuid);
			if (region.isSequenceCollectionFinished()) {
				// Ensure the installing of view of this node can be complete before process collection
				collectionSuspend(SessionRegion.SESSION_REGION);
				viewId = collectedSequences
						.remove(SessionRegion.SESSION_REGION).getVal1();

				if (logger.isTraceEnabled()) {
					trace(logger, "FT_COLLECT",
							"Sequences for session region: " + region
									+ " has been collected completetly");
				}
				return new Tuple<Integer, List<Sequence>>(viewId,
						new LinkedList<Sequence>());
			}
		}

		if (list != null)
			return new Tuple<Integer, List<Sequence>>(viewId, list);

		return null;
	}

	private void collectionSuspend(Integer regionNumber) {
		if (!collectedSequences.containsKey(regionNumber)) {
			final Region region = serviceManager.getRegion(regionNumber);
			synchronized (region) {
				while (!collectedSequences.containsKey(regionNumber)) {
					try {
						region.wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}

	private class LeavingProtocolThread extends Thread {

		private Token value;

		private short command;

		public LeavingProtocolThread(short command, Token value) {
			super();
			this.value = value;
			this.command = command;
		}

		public void run() {

			final Token token = up(command, value);
			// Trigger Broadcast
			// this phase can be skipped, since how the region and new sequencer
			// are allocated is deterministic across all nodes.
			if ((token.getDataForNextProtocol() instanceof HashMap)) {
				@SuppressWarnings("unchecked")
				final Map<Integer, List<Integer>> allocation = (Map<Integer, List<Integer>>) token
						.getDataForNextProtocol();
				final Set<Map.Entry<Integer, List<Integer>>> set = allocation
						.entrySet();
				Message message = null;
				// System.out.print("*************:" + set.size()+ "\n");
				for (Map.Entry<Integer, List<Integer>> entry : set) {
					message = new Message(null, entry.getValue().toArray(
							new Integer[entry.getValue().size()]), false);
					// set the fake srouce address
					message.setSrc(regionDistributionManager.getAddress(entry
							.getKey()));
					stack.up(Command.ELECTION_AGREEMENT, new Token(message));
					stack.down(Command.FT_UNICAST, new Token(message));
				}
			}

			// if (token.getDataForNextProtocol() != null)
			// stack.down(Command.ELECTION_SECOND_BROADCAST, new
			// Token(token.getDataForNextProtocol()));

		}
	}

	@Override
	public void setCommunicationAdaptor(CommunicationAdaptor adaptor) {
		this.adaptor = adaptor;

	}

	@Override
	public void setCachedSentMessages(Map<String, Message> sentMessages) {
		this.sentMessages = sentMessages;

	}

}
