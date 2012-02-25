package org.ssor.protocol.election;

import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ssor.CollectionFacade;
import org.ssor.Node;
import org.ssor.Region;
import org.ssor.annotation.ProtocolBinder;
import org.ssor.gcm.CommunicationAdaptor;
import org.ssor.listener.CommunicationListener;
import org.ssor.protocol.Command;
import org.ssor.protocol.Message;
import org.ssor.protocol.RequirementsAwareProtocol;
import org.ssor.protocol.Token;
import org.ssor.util.Callback;
import org.ssor.util.CommonExecutor;
import org.ssor.util.Environment;
import org.ssor.util.Tuple;

/**
 * This work strongly with view synchrony, take care of new member join and
 * election of new sequencer
 * 
 * @author Tao Chen
 * 
 */

@ProtocolBinder(managerClass=org.ssor.protocol.election.ElectionManager.class, 
		headers = {org.ssor.protocol.election.InterestHeader.class, org.ssor.protocol.election.DecisionHeader.class, org.ssor.protocol.election.AgreementHeader.class})
public class REP extends RequirementsAwareProtocol implements CommunicationListener{

	private static final Logger logger = LoggerFactory
			.getLogger(REP.class);

	// This is used in case that new sequencer failed before it broadcast
	// when the node is added into node manager, this would be removed
	// If the new one failed and the old one would re-broadcast, only the old
	// sequencer would
	// record, and only record if there is need to change sequencer
	// private Map<Byte, Decision[]> nextSequencerCollection;
	// Collect all the sequencer decision and broadcast when it is complete
	// This is completely controlled by the current node, and does not triggered
	// by message
	private Queue<Integer> assignedRegions;
	private CommunicationAdaptor adaptor;
	
	private static final int tempSequencerID = 0;

	


	@SuppressWarnings("unchecked")
	public REP() {
		assignedRegions = CollectionFacade.getConcurrentQueue();
	}

	@SuppressWarnings("unchecked")
	@Override
	public Token down(final short command, final Token value) {

		switch (command) {

		case Command.ELECTION_FIRST_BROADCAST: {

			if (value == null)
				return doDown(command, value);

			regionDistributionManager.setView((Queue<Integer>) value.getData());

			Integer[] interests = regionDistributionManager.getInterestedRegions();

			if (logger.isTraceEnabled()) {
				trace(logger, "ELECTION_FIRST_BROADCAST",
						"Number of interested region: "
								+ (interests == null ? 0 : interests.length));
			}

			Message message = new Message(new InterestHeader(), regionDistributionManager
					.getInterestedRegions(), false);
			adaptor.multicast(message);
			value.setDataForNextProtocol(message);
			return doDown(command, value);
		}
		case Command.ELECTION_UNICAST: {

			if (value == null)
				return doDown(command, value);
			if (logger.isTraceEnabled()) {
				trace(logger, "ELECTION_UNICAST", "Responding new join node");
			}

			final ElectionUnit unit = (ElectionUnit) value.getData();

			Message message = new Message(new DecisionHeader(regionDistributionManager
					.getInterestedRegions()), unit.getDecision(), false);
			//System.out.print(Environment.getUUID_ARRR(group) + " response join for " + unit.getAddress().hashCode() + "\n");
			adaptor.unicast(message, unit.getAddress());
			value.setDataForNextProtocol(message);
			return doDown(command, value);
		}
		case Command.ELECTION_SECOND_BROADCAST: {

			if (logger.isTraceEnabled()) {
				trace(logger, "ELECTION_SECOND_BROADCAST",
						"Broadcasting new join node's responsibility");
			}
			Message message = null;

			if (Token.ELECTION_RECORD_FINAL_BROADCAST_FOR_THIS == value.getNextAction()) {

				final Integer[] array = assignedRegions.size() == 0 ? null
						: new Integer[assignedRegions.size()];
				final Queue<Integer> view = regionDistributionManager.getView();
				int i = 0;
				for (Integer number : assignedRegions) {
					serviceManager.getRegion(number).setConsensusView(view);
					array[i] = number;
					i++;
				}

				message = new Message(new AgreementHeader(), array, false);
				assignedRegions.clear();
			// Use when it becomes new sequencer.
			} else
				message = new Message(new AgreementHeader(), value.getData(), false);
			adaptor.multicast(message);

			value.setDataForNextProtocol(message);
			
			return doDown(command, value);
		}
		}

		return doDown(command, value);
	}

	@Override
	public Token up(final short command, final Token value) {

		// TODO implement FT
		switch (command) {

		case Command.ELECTION_JOIN: {

			if (value == null)
				return doUp(command, value);

			final Message message = (Message) value.getData();

			int id = util.getUUIDFromAddress(message.getSrc());

			// This may occur if this is the subsequence join node, V1V2-D1D2,
			// then it would receive request from ex-join node,
			// which it should discard
			if (regionDistributionManager.isHasNode(id)){
				//System.out.print("discard: " + id + "***\n");
				if (logger.isTraceEnabled()) {
					trace(
							logger,
							"ELECTION_JOIN",
							"Join request from node: " + id + " is discard");
				}
				
				value.setNextAction(Token.ELECTION_JOIN_DISCARD);
				return doUp(command, value);
			}
			// Maintain view order, extends to protocol scope
			if (!regionDistributionSynchronyManager.suspendAndRunAsNewThread(id,
					new Callback() {

						@Override
						public Object run() {
							Token token =  up(command, value);
							if (Token.ELECTION_JOIN_DISCARD == token.getNextAction())
								return null;
							
							if (Token.ELECTION_JOIN_INTERSTS_ONLY == token.getNextAction()) {

								if (logger.isTraceEnabled()) {
									logger
											.trace("Receive join request and reply to new join node, but reply its interests only");
								}

							} else if (logger.isTraceEnabled()) {
								logger.trace("Receive join request and reply to new join node");
							}

							CommonExecutor.releaseForReceiveInAnotherThread(null, adaptor.getGroup(), null);	
							
							return stack.down(Command.ELECTION_UNICAST, new Token(new ElectionUnit(
									message.getSrc(), Token.ELECTION_JOIN_INTERSTS_ONLY == token.getNextAction()? null : (Decision[]) token.getDataForNextProtocol())));

						}

					})){
				Environment.isDeliverySuspended.set(true);
				value.setNextAction(Token.ELECTION_JOIN_DISCARD);
			    return doUp(command, value);
			}
			
			final Node node = regionDistributionManager.get(UUID_ADDR);
			Node newNode = new Node(id, message.getSrc());
			
			// Set interests
			if (message.getBody() != null)
				newNode.setInterestedRegions((Integer[]) message.getBody());
			// Does not need to perform this itself
			// Itself has been filtered at gcm adaptor
			if (!node.isHasResponsibleRegion()) {

				if (logger.isTraceEnabled()) {
					trace(
							logger,
							"ELECTION_JOIN",
							"Join request for new node receive but ignored due to this node does not have responsible regions");
				}

				regionDistributionManager.register(newNode, UUID_ADDR);

				value.setNextAction(Token.ELECTION_JOIN_INTERSTS_ONLY);
				return doUp(command, value);

			}
			// If the new node have indicate interested regions
			Decision[] interests = null;
			if (message.getBody() != null) {

				synchronized (node) {
					interests = node.findInterestedRegion((Integer[]) message
							.getBody());
					resetRegionSequencer(interests, node, newNode
							.getUUID_ADDR());
				}

				if (logger.isTraceEnabled()) {
					trace(logger, "ELECTION_JOIN",
							"The new node have indicated interested regions, and this node has "
									+ interests.length + " responsible regions");
				}
				// If no indicated interests, then see if there is need to split
				// the sequencer
			} else {

				Object lock = regionDistributionManager.getMutalLock();
				// For maintain the right head on the priority queue
				synchronized (lock) {
					Node head = regionDistributionManager.getOverheadSequencer(UUID_ADDR);

					// It means head == node
					if (head != null) {

						if (logger.isTraceEnabled()) {
							trace(logger, "ELECTION_JOIN",
									"This node is the one that assigns regions to the new joined node, when the joined node have no indicated interests");
						}

						interests = head.splitRegion();
						resetRegionSequencer(interests, head, newNode
								.getUUID_ADDR());
						regionDistributionManager.putBack(head);
					} else
						interests = node.getResponsibleRegionsWithDecision();
				}

				if (logger.isTraceEnabled()) {
					trace(logger, "ELECTION_JOIN",
							"The new node have no indicated interested regions, and reply "
									+ interests.length + " regions");
				}
			}

			if (interests != null)
				newNode.addNewResposibleRegion(interests);

			regionDistributionManager.register(newNode, UUID_ADDR);
			value.setDataForNextProtocol(Token.ELECTION_JOIN_INTERSTS_AND_REGIONS);
			value.setDataForNextProtocol(interests);
			return doUp(command, value);
		}
		case Command.ELECTION_RECORD: {

			if (value == null)
				return doUp(command, value);

			final Message message = (Message) value.getData();
			final Decision[] decisions = (Decision[]) message.getBody();
			final DecisionHeader header = (DecisionHeader) message.getHeader();

			int id = util.getUUIDFromAddress(message.getSrc());
			if (!regionDistributionManager.receive(id)) {

				if (logger.isTraceEnabled()) {
					trace(
							logger,
							"ELECTION_RECORD",
							"The decision from node: "
									+ id
									+ " receive, but it does not contains in the coressponding view");
				}

				
				return doUp(command, value);
			}
			

			if (logger.isTraceEnabled()) {
				trace(logger, "ELECTION_RECORD",
						"Receive from node: " + id + ", number of interests: " + (header.getInterests() != null? header.getInterests().length : "0"));
			}
			// Set interests
			regionDistributionManager.get(id).setInterestedRegions(header.getInterests());
			Region region = null;

			if (decisions != null) {
				for (Decision decision : decisions) {

					region = serviceManager.getRegion(decision.getRegion());
					if (decision.isChnage()) {

						if (logger.isTraceEnabled()) {
							trace(logger, "ELECTION_RECORD",
									"Record sequencer of region: "
											+ decision.getRegion()
											+ ", to itself");
						}
						// synchronized(region){
						region.setSequencer(UUID_ADDR);
						// }
						assignedRegions.add(decision.getRegion());
					} else {

						if (logger.isTraceEnabled()) {
							trace(logger, "ELECTION_RECORD",
									"Record sequencer of region: "
											+ decision.getRegion()
											+ ", to node: "
											+ util.getUUIDFromAddress(message
													.getSrc()));
						}
						// synchronized(region){
						region.setSequencer(id);
						regionDistributionManager.get(id).addRegion(decision.getRegion());
						// }

					}

				}

			}
			if (regionDistributionManager.isCollectionEnough(id)) {

				if (logger.isTraceEnabled()) {
					trace(logger, "ELECTION_RECORD",
							UUID_ADDR + " Recording of sequencers finished!, from: " + id);
				}

				value.setNextAction(Token.ELECTION_RECORD_FINAL_BROADCAST);
				return doUp(command, value);
			}

			if (logger.isTraceEnabled()) {
				trace(logger, "ELECTING_RECORD",
						UUID_ADDR + " Recording of sequencers have not finished yet, from: " + id);
			}
			
			
			return doUp(command, value);

		}
		case Command.ELECTION_AGREEMENT: {

			if (value == null)
				return doUp(command, value);

			final Message message = (Message) value.getData();
			int id = util.getUUIDFromAddress(message.getSrc());
			// final boolean isCurrentNode = id != Environment.UUID_ADDR;

			Node node = null;
			// If the new node is current node, then should execute this
			if (regionDistributionManager.get(id) == null)
				regionDistributionManager.register(node = new Node(id, message.getSrc()), UUID_ADDR);
			// This is for new elected sequencer when the old one failed
			else 
				node = regionDistributionManager.get(id);
				
				
			
			// If the new node does not become any new sequencer
			if (message.getBody() == null) {

				if (logger.isTraceEnabled()) {
					trace(logger, "ELECTION_AGREEMENT", "New node: " + id
							+ " does not become any sequencer");
				}

				regionDistributionSynchronyManager.releaseOnRetransmission(id, null);


				// This is needed for resorting
				regionDistributionManager.resort(node);
				value.setNextAction(Token.ELECTION_AGREEMENT_NO_NEW_REGION);
				return doUp(command, value);
			}

			final Integer[] regions = (Integer[]) message.getBody();

			Region region = null;

			int oldId;
			for (int regionNumber : regions) {
				region = serviceManager.getRegion(regionNumber);

				if (logger.isTraceEnabled()) {
					trace(
							logger,
							"ELECTION_AGREEMENT",
							"Change sequencer of region: "
									+ regionNumber
									+ (id == UUID_ADDR ? ", to itself"
											: ", to node: " + id));
				}
				// Update the new sequencer
				node.addRegion(regionNumber);
				oldId = region.changeSequencerAndSuspendRequests(id);
				// If the new node gain regions from this node, then remove the
				// cached sequences when it was the sequencer
				if (node.isLocalUncompleteRegion(regionNumber)) {
					doDown(Command.FT_RELEASE_WHEN_JOIN, new Token(regionNumber));
				}

				// Update the old sequencer, if it is tempSequencerID then
				// it means this is the current node and it has been remove
				// during the
				// assign phase
				if (oldId != tempSequencerID) {
					if (regionDistributionManager.get(oldId) != null)
						regionDistributionManager.get(oldId).removeRegion(regionNumber);

				} else
					// This is ensure order/correctness by VS
					node.clearUncompletedRegionWhenJoining(regionNumber);

				node.clearUncompletedNodeWhenCrash(id);
			}

			regionDistributionSynchronyManager.releaseOnAgreement(id);
			

			// This is needed for resorting
			regionDistributionManager.resort(node);

			return doUp(command, value);
		}
		case Command.INSTALL_VIEW_ON_LEAVING: {

			if (value == null)
				return doUp(command, value);
			if (isConsensus)
				return doUp(command, value);

			@SuppressWarnings("unchecked")
			final Tuple<Integer, Queue<Integer>> tuple = (Tuple<Integer, Queue<Integer>>) value.getData();

			regionDistributionManager.crashDuringConsensus(tuple.getVal1());

			return doUp(command, value);
		}
		}

		return doUp(command, value);
	}


	private void resetRegionSequencer(Decision[] interests, Node node,
			int newNode) {

		Region region = null;
		for (Decision d : interests) {

			if (d.isChnage()) {

				if (logger.isDebugEnabled()) {
					logger.debug("Sequencer of region id: " + d.getRegion()
							+ " will be changed to " + newNode);
				}

				region = serviceManager.getRegion(d.getRegion());
				// Lock the incoming request for that region
				// synchronized (region) {
				// Free the original sequencer, which is itself
				// The annocement of new sequencer would be done
				// by the new sequencer
				region.setSequencer(tempSequencerID);
				node.removeRegion(d.getRegion());
				// }
			} else {

				if (logger.isDebugEnabled()) {
					logger.debug("Sequencer of region id: " + d.getRegion()
							+ " stay remind");
				}

			}

		}
	}

	@Override
	public void setCommunicationAdaptor(CommunicationAdaptor adaptor) {
		this.adaptor = adaptor;
		
	}


}
