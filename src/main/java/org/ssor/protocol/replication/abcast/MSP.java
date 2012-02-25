package org.ssor.protocol.replication.abcast;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.ssor.Region;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.ssor.Sequence;
import org.ssor.AtomicService;
import org.ssor.ServiceStackContext;
import org.ssor.CompositeService;
import org.ssor.exception.SendToFaultyNodeException;
import org.ssor.gcm.CommunicationAdaptor;
import org.ssor.invocation.InvocationUnit;
import org.ssor.listener.CommunicationListener;
import org.ssor.protocol.Command;
import org.ssor.protocol.Header;
import org.ssor.protocol.Message;
import org.ssor.protocol.ProtocolSharableInstances;
import org.ssor.protocol.RequirementsAwareProtocol;
import org.ssor.protocol.Token;
import org.ssor.protocol.replication.BatchRequestHeader;
import org.ssor.protocol.replication.ReplicationUnit;
import org.ssor.protocol.replication.RequestHeader;
import org.ssor.protocol.replication.ResponseHeader;
import org.ssor.protocol.replication.ResponsePacket;
import org.ssor.util.Callback;
import org.ssor.util.CommonExecutor;
import org.ssor.util.Environment;
import org.ssor.util.Tuple;

public class MSP extends RequirementsAwareProtocol implements CommunicationListener, ProtocolSharableInstances {

	public static final short FIFO_SCOPE = 1;

	private static final Logger logger = LoggerFactory.getLogger(MSP.class);

	// MSP closely work with different replication model (active/passive/quorum)

	// Used when new thread create in case of suspend
	private ThreadLocal<Boolean> marker = new ThreadLocal<Boolean>();
	private ExecutorService pool = Executors.newCachedThreadPool();

	// Cache of sent messages , although FIFO has been guaranteed, it can
	// be used by request to retrieval data throughout the abcast protocol
	// The same instance as the one in FT
	private Map<String, Message> sentMessages;
	

	private CommunicationAdaptor adaptor;



	@Override
	public Token down(short command, Token value) {
		switch (command) {

		case Command.ABCAST_FIRST_UNICAST: {

			if (value == null)
				return doDown(command, value);
		
			final InvocationUnit unit = (InvocationUnit) value.getData();
			if (logger.isTraceEnabled()) {
				trace(logger, "ABCAST_FIRST_UNICAST", "service: "
						+ unit.getService().getName()
						+ " delivery to lower layer");
			}
			final RequestHeader header = new RequestHeader(unit.getService()
					.getName(), UUID_ADDR, isSessional(unit.getService().getName()));
			
			Message message = null;
			if (!unit.getService().isRequireBlocking()) {
				message = new Message(header, null, true);
				// This would trigger the agreement locally
				message.setRequirePreBroadcasting(false);
				header.setOuter(new ResponseHeader());

				if (logger.isTraceEnabled()) {
					trace(logger, "ABCAST_FIRST_UNICAST", "ReqId: "
							+ message.getReqId() + " Service: "
							+ unit.getService().getName()
							+ ", does not need the first broadcasting");
				}

				if (unit.getService() instanceof CompositeService)
					// Composed service does not use this, use TriggerOrder
					// instead
					message.setIsNeedRetransmited(false);

				value.setDataForNextProtocol(message);
				return doDown(command, value);
			}
			// If it is trigger service, then cache it for order timestamps of
			// nested services
			if (unit.getService() instanceof CompositeService) {

				message = new Message(header, null, true);

				// Composed service does not use this, use TriggerOrder instead
				message.setIsNeedRetransmited(false);

				final CompositeService triggerService = (CompositeService) unit
						.getService();
				final Sequence[] timestamps = new Sequence[triggerService
						.getNestedServiceLength()];

				if (logger.isTraceEnabled()) {
					trace(
							logger,
							"ABCAST_FIRST_UNICAST",
							"ReqId: "
									+ message.getReqId()
									+ " Service: "
									+ unit.getService().getName()
									+ ", which is composite service, therefore cache it for"
									+ timestamps.length + " nested service");

				}
				// Create cache for all sequence for nested services
				message.createSequenceCache(timestamps, triggerService
						.getNonOrderedServiceCount());

				
				if (Environment.ENABLE_CHANGE_FIFO)
					message
							.setFIFO_Scope(unit.getService().isFIFOEnabled() ? FIFO_SCOPE
									: util.getRandomShort());

				final Set<Region> involedRegions = triggerService.getInvoledRegions();

				Object address = null;
				Integer uuid = null;
				final Set<Object> addresses = new HashSet<Object>();

				sentMessages.put(message.getReqId(), message);
				for (Region reg : involedRegions) {
					uuid = reg.getSequencerWhenRequest();

					if (uuid == null)
						continue;
					
					address = regionDistributionManager.getAddress(uuid);
					if (address == null)
						continue;

					addresses.add(address);
				}
				

				try {

					for (Object addr : addresses)
						adaptor.unicast(message, addr);

				} catch (Throwable t) {
					if (t instanceof SendToFaultyNodeException)
						throw (SendToFaultyNodeException) t;

					throw new SendToFaultyNodeException(uuid, message);
				}

			} else {

				message = new Message(header, null, true);
				final Message cached = new Message(header, unit.getArguments(),
						false);
				cached.setReqId(message.getReqId());
				
				if (Environment.ENABLE_CHANGE_FIFO)
					message
							.setFIFO_Scope(unit.getService().isFIFOEnabled() ? FIFO_SCOPE
									: util.getRandomShort());

				if (logger.isTraceEnabled()) {
					trace(logger, "ABCAST_FIRST_UNICAST", "ReqId: "
							+ message.getReqId() + " Service: "
							+ unit.getService().getName());

				}
				sentMessages.put(message.getReqId(), cached);
				final Integer uuid = unit.getService()
						.getSequencerWhenRequest();
				value.setDataForNextProtocol(cached);
				if (uuid == null){
					value.setNextAction(Token.REPLICATION_REQUEST_SEQUENCER_NOT_EXIST);
					return doDown(command, value);
				}
				final Object address = regionDistributionManager
						.getAddress(uuid);
				// If try to send message to faulty sequencer
				if (address == null){
					value.setNextAction(Token.REPLICATION_REQUEST_SEQUENCER_NOT_EXIST);
					return doDown(command, value);
				}
				

				try {
					adaptor.unicast(message, address);
				} catch (Throwable t) {
					
					throw new SendToFaultyNodeException(uuid, cached);
				}

				
				return doDown(command, value);
			}

			// The requester would buffer this here for notify the trigger
			// thread
			value.setDataForNextProtocol(message);
			return doDown(command, value);
		}
		case Command.ABCAST_NON_ORDERED_BROADCAST: {

			if (value == null)
				return doDown(command, value);

			final InvocationUnit unit = (InvocationUnit) value.getData();

			if (logger.isTraceEnabled()) {
				trace(logger, "ABCAST_NON_ORDERED_BROADCAST", "Service: "
						+ unit.getService().getName()
						+ " delivery to lower layer without order protocol");
			}

			RequestHeader header = null;
			final Message message = new Message(header = new RequestHeader(unit
					.getService().getName(), UUID_ADDR, serviceManager
					.isSessional(unit.getService().getName())), unit
					.getArguments(), true);

			message.setRequirePreBroadcasting(false);
			header.setNonOrdered(true);

			if (Environment.ENABLE_CHANGE_FIFO)
				message
						.setFIFO_Scope(unit.getService().isFIFOEnabled() ? FIFO_SCOPE
								: util.getRandomShort());

			adaptor.multicast(message);

			// After mutilcast, set a fake response header
			header.setOuter(new ResponseHeader());

			value.setDataForNextProtocol(message);
			return doDown(command, value);

		}
		case Command.ABCAST_SECOND_UNICAST: {

			if (value == null)
				return doDown(command, value);

			final ReplicationUnit unit = (ReplicationUnit) value.getData();
			// If this is not sequencer, then timestamp should be null

			final ResponseHeader header = new ResponseHeader(unit.getTuple());

			// Send back the order timestamp to requester if this is the
			// sequencer, this would use new response header instance

			Message message = new Message(header, null, false);

			if (Environment.ENABLE_CHANGE_FIFO)
				message.setFIFO_Scope(util.getRandomShort());
			message.setReqId(unit.getReqId());

			if (logger.isTraceEnabled()) {
				trace(logger, "ABCAST_SECOND_UNICAST", "ReqId: "
						+ message.getReqId() + " Service: " + unit.getService()
						+ " coordinate with timestamp: "
						+ header.getTimestamp());
			}

			adaptor.unicast(message, unit.getAddress());

			value.setDataForNextProtocol(message);
			return doDown(command, value);
			// The 2nd response of BUB, the 2nd B one which broadcast timestamp
			// as well as delayed data
			// at this point, the service of requester should have been executed
		}

		case Command.ABCAST_FINAL_BROADCAST: {

			if (value == null)
				return doDown(command, value);

			// This is the action of requester, that send timstamps to other
			// nodes
			final ReplicationUnit unit = (ReplicationUnit) value.getData();
			// If this is not sequencer, then timestamp should be null

			final ResponseHeader header = (ResponseHeader) unit.getTuple();

			final Message original = sentMessages.get(unit.getReqId());
			Message message = new Message(original.getHeader(), null, false);
			message.getHeader().setOuter(header);

			message.setBody(original.getBody());
			if (Environment.ENABLE_CHANGE_FIFO)
				message.setFIFO_Scope(util.getRandomShort());
			message.setReqId(unit.getReqId());

			if (logger.isTraceEnabled()) {
				trace(logger, "ABCAST_FINAL_BROADCAST", "ReqId: "
						+ message.getReqId() + " Service: " + unit.getService()
						+ " agreement and broadcasting with timestamp: "
						+ header.getTimestamp());
			}
			
			adaptor.multicast(message);

			value.setDataForNextProtocol(message);
		
			return doDown(command, value);

		}
		case Command.ABCAST_DELAY_FINAL_BROADCAST: {

			if (value == null)
				return doDown(command, value);

			final Message message = (Message) value.getData();

			final ResponsePacket packet = (ResponsePacket) message.getBody();

			if (logger.isTraceEnabled()) {
				trace(
						logger,
						"ABCAST_DELAY_FINAL_BROADCAST",
						"ReqId: "
								+ message.getReqId()
								+ " delay the agreement execution for triggered and deterministic service");
			}

			// Free the extra headers, the violidate attributed
			// isRequirePreBroadcasting would still work
			// since this is only execute on the requester site
			if (message.isRequirePreBroadcasting()) {

				// The message here is the one that was previously cache on
				// requester site

				if (logger.isTraceEnabled()) {
					trace(logger, "ABCAST_DELAY_FINAL_BROADCAST", "ReqId: "
							+ message.getReqId()
							+ " reform with ResponseHeader");
				}

				Header header = message.getHeader();
				if (Environment.ENABLE_CHANGE_FIFO)
					message.setFIFO_Scope(util.getRandomShort());
				// Remove InvocationHeader
				header.getOuter().setOuter(null);
				// message.setIsFinishDelayBroadcast(true);

				Message msg = new Message(header, packet, false);
				msg.setReqId(message.getReqId());

				adaptor.multicast(msg);
			} else {

				// Because to this stage, the local service has been executed,
				// therefore no need to set fake response header

				if (logger.isTraceEnabled()) {
					trace(logger, "ABCAST_DELAY_FINAL_BROADCAST", "ReqId: "
							+ message.getReqId() + " reform with RequestHeader");
				}
				message.getHeader().setOuter(null);
				((RequestHeader) message.getHeader()).setNonOrdered(true);
				// This FIFO judgement has been done on replicaing layer

				adaptor.multicast(message);
			}
			
			value.setDataForNextProtocol(message);

			return doDown(command, value);

		}
			/*
			 * case Command.ORDERING_VALIDATE_IS_BLOCK: {
			 * 
			 * if(value == null) doDown(command, value);
			 * 
			 * Message message = (Message) value; Header header =
			 * message.getHeader(); // Only work for the last B, which may by
			 * suspended if (header instanceof ResponseHeader) { // Message
			 * original = null; // Even null would trigger new thread // if
			 * ((original = unsortedMessages.get(message.getReqId())) == //
			 * null) // return null; // This ensure that it would not be removed
			 * in the requester // failed before // this thread enter CS //
			 * original.setIsReceive2ndBroadcast(true); // return true; // final
			 * RequestHeader requestHeader = (RequestHeader) // unsortedMessages //
			 * .get(message.getReqId()).getHeader(); // return new String[] {
			 * requestHeader.getService(), // requestHeader.getSessionId() }; }
			 * 
			 * return false; }
			 */

		}
		return doDown(command, value);
	}

	@Override
	public Token up(short command, Token value) {

		switch (command) {

		case Command.ABCAST_COORDINATE: {

			if (value == null)
				return doUp(command, value);

			Message message = (Message) value.getData();

			RequestHeader requestHeader = (RequestHeader) message.getHeader();
			AtomicService service = serviceManager.get(requestHeader
					.getService());
			
			Object sequence = null;

			if (service instanceof CompositeService) {

				message.setHoldSeq(true);
				SequenceVector[] vectors = null;
				// If this is retransmission of composed service
				if (message.getHeader() instanceof BatchRequestHeader) {

					if (logger.isTraceEnabled()) {

						trace(
								logger,
								"ABCAST_COORDINATE",
								"ReqId: "
										+ message.getReqId()
										+ ", receive message for coordination of triggered service (retransmission): "
										+ requestHeader.getService());
					}
					vectors = service.getNextSeqno(
							requestHeader.getSessionId(),
							((BatchRequestHeader) message.getHeader())
									.getRetransmitedIndices(), UUID_ADDR).getVector();
				} else {

					if (logger.isTraceEnabled()) {
						trace(
								logger,
								"ABCAST_COORDINATE",
								"ReqId: "
										+ message.getReqId()
										+ ", receive message for coordination of triggered service: "
										+ requestHeader.getService());
					}
					vectors = service.getNextSeqno(
							requestHeader.getSessionId(), UUID_ADDR)
							.getVector();
				}

				value.setDataForNextProtocol( new Tuple<Integer, Object>(requestHeader
						.getRequester(), vectors.length == 0 ? null : vectors));
				return doUp(command, value);
			} else if ((sequence = service.getNextSeqno(requestHeader
					.getSessionId(), UUID_ADDR)) != null) {

				if (logger.isTraceEnabled()) {
					trace(
							logger,
							"ABCAST_COORDINATE",
							"ReqId: "
									+ message.getReqId()
									+ ", receive message for coordination of meta service: "
									+ requestHeader.getService());
				}
				message.setHoldSeq(true);

				value.setDataForNextProtocol(new Tuple<Integer, Object>(requestHeader
						.getRequester(), sequence));
				return doUp(command, value);

			}

			value.setNextAction(Token.REPLICATION_COORDINATE_NOT_SEQUENCER);
			return doUp(command, value);

			// This should only be executed by a total order requester
		}
		case Command.ABCAST_ACQUIRE_SEQUENCE: {

			if (value == null)
				return doUp(command, value);

			final Message message = (Message) value.getData();
			final ResponseHeader header = (ResponseHeader) message.getHeader();
		
			
			// This should always non-null, if it is null then it may be an
			// error
			Message original = sentMessages.get(message.getReqId());
			//System.out.print("normal get: "+header.getTimestamp()+"\n");
			if (original == null) {

				if (logger.isErrorEnabled()) {
					logger
							.error("Error occurs during on receive of coordination message, the cached message is null and thus this "
									+ "request was discard");
				}
				System.out.print("get: "+header.getTimestamp()+"\n");
				System.out.print("msg: " + message.getReqId()+ "\n");
				value.setNextAction(Token.REPLICATION_ACQUIRE_SEQUENCE_DISCARDED);
				return value;
			}

			final RequestHeader requestHeader = (RequestHeader) original
					.getHeader();
			final String service = requestHeader.getService();
			// UUID of the sequencer, used for FT
			final int sender = util.getUUIDFromAddress(message.getSrc());
			if (header.getTimestamp() instanceof SequenceVector[]) {

				SequenceVector[] vector = (SequenceVector[]) header.getTimestamp();
				SequenceCollector order = original.getSequenceCache();
				// Sync has been done inside
				for (int i = 0; i < vector.length; i++) {

					if (logger.isTraceEnabled()) {

						trace(logger, "ABCAST_EXECUTE", "ReqId: "
								+ message.getReqId() + ", index: "
								+ vector[i].getIndex() + ", timestamp: "
								+ vector[i].getSequence());
					}
					vector[i].getSequence().setSessionId(
							requestHeader.getSessionId());
					if (order.add(vector[i])) {
						// original.releaseSequenceCache();
						// Copy sequence[] to header
						header.setTimestamp(order.getSequences());

						if (logger.isTraceEnabled()) {
							trace(
									logger,
									"ABCAST_EXECUTE",
									"ReqId: "
											+ message.getReqId()
											+ " Complete: receive all total ordered timestamp from sequencer for triggered service");
						}
						
						value.setDataForNextProtocol(new Tuple<Tuple<String, Message>, Tuple<Integer, Object>>(new Tuple<String, Message>(service, original), new Tuple<Integer, Object>(
								sender, vector)));
						return doUp(command, value);
					
					}
				}

				// If for meta service then simply forward to other nodes
			} else {

				if (logger.isTraceEnabled()) {

					trace(
							logger,
							"ABCAST_EXECUTE",
							"ReqId: "
									+ message.getReqId()
									+ ", timestamp: "
									+ header.getTimestamp()
									+ " receive total ordered timestamp from sequencer for meta service");
				}
				((Sequence) header.getTimestamp()).setSessionId(requestHeader
						.getSessionId());
				
				value.setDataForNextProtocol(new Tuple<Tuple<String, Message>, Tuple<Integer, Object>>(new Tuple<String, Message>(service, original), new Tuple<Integer, Object>(
						sender, header.getTimestamp())));
				return doUp(command, value);
			}

			if (logger.isTraceEnabled()) {
				trace(
						logger,
						"ABCAST_EXECUTE",
						"ReqId: "
								+ message.getReqId()
								+ " Incomplete: receive total ordered timestamp from sequencer for triggered service");
			}
			
			value.setNextAction(Token.REPLICATION_ACQUIRE_SEQUENCE_NOT_COLLECTED);
			
			value.setDataForNextProtocol(new Tuple<Tuple<String, Message>, Tuple<Integer, Object>>(new Tuple<String, Message>(service, original), new Tuple<Integer, Object>(
					sender, header.getTimestamp())));
			return doUp(
					command, value);

		}
		case Command.ABCAST_AGREEMENT: {
			
			if (value == null)
				return doUp(command, value);

			final Message message = (Message) value.getData();
			
			if (message.isExecuteInAdvance())
				return parseExecutedService(message, value);

			Message data = null;
			RequestHeader requestHeader = null;
			// if(message.getBody()!=null && message.getHeader() instanceof
			// ResponseHeader)
			// System.out.print("receive
			// "+((ResponseHeader)message.getHeader()).getTimestamp() + " : " +
			// message.getBody() + "\n");
			data = sentMessages.remove(message.getReqId());
			// If it is on replica site
			if (data == null) {

				requestHeader = (RequestHeader) message.getHeader();
				// The requester has been executed without trigger
				if (requestHeader.isRequester(UUID_ADDR))
					return null;

				// Requester would contains it for trigger
				data = message;
				// If non-ordered message, it should have only one RequestHeader
				if (requestHeader.getOuter() == null) {

					if (logger.isTraceEnabled()) {
						trace(logger, "ABCAST_AGREEMENT", "ReqId: "
								+ message.getReqId()
								+ " Receive non-total ordered required message");
					}
					requestHeader.setOuter(new ResponseHeader());
				}
				// If it is on requester site
			} else {

				// This also delay the remove at the last B for delay service
				/*
				 * if (message.isNotifyForDelayService() != null) data =
				 * unsortedMessages.get(message.getReqId()); else data =
				 * unsortedMessages.remove(message.getReqId());
				 * 
				 * if (data.isFinishDelayBroadcast() != null) { return null; }
				 */

				requestHeader = (RequestHeader) data.getHeader();

				requestHeader
						.setOuter(message.getHeader().getOuter() instanceof ResponseHeader ? message
								.getHeader().getOuter()
								: message.getHeader());
			}
			//System.out.print("msg: " + message.getReqId()+ " , " + ((ResponseHeader)message.getHeader().getOuter()).getTimestamp()+ "\n");
			// The requester would execute the request thread by client, this
			// would received by any node including itself
			// thus non-ordered message would be triggered quickly
			// For requester, data should always non-null
			if (requestHeader.isRequester(UUID_ADDR)) {

				synchronized (data) {
					data.setIsExecutable(true);
					Environment.isDeliverySuspended.set(true);
					data.notifyAll();
				}

				if (logger.isTraceEnabled()) {
					trace(
							logger,
							"ABCAST_AGREEMENT",
							"ReqId: "
									+ message.getReqId()
									+ " Trigger the requester's thread for executing, (if this is the second one for delay service, then it does nothing)");
				}
				return doUp(command, null);
			}

			/*
			 * For replica in case of fetch before put, during consensus but due
			 * to the reliable property, it would eventually received
			 * 
			 * The synchronized here does nothing but suspend the thread
			 * 
			 * 
			 * if (data == null) { synchronized (message) { while ((data =
			 * unsortedMessages.remove(message.getReqId())) == null) {
			 * 
			 * if (logger.isErrorEnabled()) { logger .error("ReqId: " +
			 * message.getReqId() + " the cache message from first B seems
			 * missing, this may due to consensus protocol, thread would wait
			 * for 500 ms"); } try { // This number can be changed, or use
			 * maximum time message.wait(500); } catch (InterruptedException e) {
			 * 
			 * e.printStackTrace(); } } } }
			 */

			// Copy delay data for triggered service, mainly copy
			// ResponsePacket, which has nested arugments and return values
			if (data.getBody() instanceof ResponsePacket) {

				if (logger.isTraceEnabled()) {
					trace(
							logger,
							"ABCAST_AGREEMENT",
							"ReqId: "
									+ message.getReqId()
									+ " Receive total ordered required message for triggered service or meta service with nested undeterministic service");
				}
				/*
				 * if (data.getBody() != null) ((ResponsePacket)
				 * message.getBody()) .setNestedArguments((Object[])
				 * data.getBody()); data.setBody(message.getBody());
				 */
			} else

			if (logger.isTraceEnabled()) {
				trace(
						logger,
						"ABCAST_AGREEMENT",
						"ReqId: "
								+ message.getReqId()
								+ " Receive total ordered required message for meta service");
			}
			// This should only effect the trigger thread of the requester

			return parseExecutedService(data, value);
		}
		}
		return doUp(command, value);
	}

	private Token parseExecutedService(Message message, Token value) {

		final RequestHeader requestHeader = (RequestHeader) message.getHeader();
		// The root service
		final AtomicService service = serviceManager.get(requestHeader
				.getService());

		final ResponseHeader responseHeader = (ResponseHeader) requestHeader
				.getOuter();
		
		// This only occurs on replica site, the requester sit has already
		// executed, this is for nested service and deterministic service
		if (message.getBody() instanceof ResponsePacket) {

			ResponsePacket packet = (ResponsePacket) message.getBody();
			Object[] arguments = packet.getNestedArguments();
			ServiceStackContext.setRedundantReturnValues(service, packet.getReturns());
			if (service instanceof CompositeService) {

				// The timestamp would be array of integer
				final AtomicService[] nestedServices = ((CompositeService) service)
						.getServices();

				final Sequence[] timestamps = (Sequence[]) responseHeader
						.getTimestamp();

				// Execute the nested service one by one, the trigger
				// service itself
				// does not need sync
				final DeliveryPacket deliveryPacket = new DeliveryPacket();
				for (int i = 0; i < nestedServices.length; i++) {

					if (logger.isTraceEnabled()) {
						trace(logger, "REPLICATION_AGREEMENT", "ReqId: "
								+ message.getReqId() + ", timestamp: "
								+ timestamps[i] + ", Service: "
								+ service.getName());
					}
					value.setDataForNextProtocol(deliveryPacket.reset(nestedServices[i],
							(Object[]) arguments[i], timestamps[i]));
					doAtomicDelivery(value,
							requestHeader.getSessionId());
				}
			} else {

				if (logger.isTraceEnabled()) {
					trace(logger, "REPLICATION_AGREEMENT", "ReqId: "
							+ message.getReqId() + ", timestamp: "
							+ responseHeader.getTimestamp() + ", Service: "
							+ service.getName());
				}
				
				value.setDataForNextProtocol(new DeliveryPacket(service, arguments, null,
						responseHeader.getTimestamp()));
				doAtomicDelivery(value, requestHeader
						.getSessionId());
			}
			ServiceStackContext.releaseReturnValues();
			
			

		} else {

			if (logger.isTraceEnabled()) {
				trace(logger, "REPLICATION_AGREEMENT", "ReqId: "
						+ message.getReqId() + ", timestamp: "
						+ responseHeader.getTimestamp() + ", Service: "
						+ service.getName() + ", is on requester: "
						+ (responseHeader.getOuter() != null));
			}

			value.setDataForNextProtocol(new DeliveryPacket(service, message
					.getBody() == null ? null : (Object[]) message.getBody(),
					responseHeader.getOuter(), responseHeader.getTimestamp()));
			// The timestamp would be integer, the non-order meta service
			// can be executed
			// outside, therefore here the timestamp always non-null
			return doAtomicDelivery(value,
					requestHeader.getSessionId());
			
		}
		
		return value;
		
	}

	private Token doAtomicDelivery(final Token token,
			final String sessionId) {
	
		final DeliveryPacket packet = (DeliveryPacket)token.getDataForNextProtocol();
		Token result = null;
		Sequence sequence = packet.getSequence() instanceof Sequence ? (Sequence) packet
				.getSequence()
				: null;

		AtomicService service = packet.getService();
		if (service instanceof CompositeService)
			sequence = null;

		
		// This is need for nested non-ordered required service
		// No sync is made since it does not sync on the lock of a region
		if (sequence == null) {

			if (logger.isInfoEnabled()) {
				logger.info("Execute service: " + service.getName()
						+ ", non total ordered message");
			}
			// Increase even for non-ordered message, in order to maintain
			// passive related services
			// If it is blocking require, then this should have been executed by
			// the 1st B already
			/*
			 * if(!root.isRequireBlocking()){
			 * 
			 * if(logger.isDebugEnabled()){ logger.debug("Execute service: " +
			 * service.getName() + ", no 1st broadcasting, thus execute
			 * getNextTimestamp"); } // For unblocking nested service, this is
			 * only the trigger service since nested services would never come
			 * to here service.getNextTimestamp(sessionId); }
			 */
			// timestamp is sequence[] when execution of composed service on
			// requester site
		
			return doUp(Command.ABCAST_AGREEMENT, token);
		}

		Object lock = packet.getService().getMutualLock(sessionId);
		int decision = 0;
		synchronized (lock) {

			while ((decision = service.isExecutable(sessionId, sequence)) > 0) {

				try {

					// This means it is a new thread already, and only for
					// execution on replica sites
					if (marker.get() == null && packet.getIHeader() == null) {					
						Environment.isDeliverySuspended.set(true);
						pool.execute(new Runnable() {

							@Override
							public void run() {

								// System.out.print(packet.getSequence() + " Run
								// new!!!!***** \n");
								marker.set(true);
								CommonExecutor.releaseForReceiveInAnotherThread(new Callback(){

									@Override
									public Object run() {
										return doAtomicDelivery(token.clone(packet.clone(), token.getData()), sessionId);
									}
									
								}, adaptor.getGroup(), null);				
								marker.set(null);
							}

						});
						return null;

					} else {

						if (logger.isDebugEnabled()) {
							logger.debug("Suspend service: "
									+ service.getName() + ", sequence: "
									+ sequence + ", is session level: "
									+ (sessionId != null));
						}

						lock.wait();
					}

				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			// }
			// System.out.print(timestamp + ":" + decision + "\n" );

			// If it needs to be ordered
			if (decision == 0) {

				if (logger.isInfoEnabled()) {
					logger.info(UUID_ADDR + " Execute service: "
							+ service.getName() + ", sequence: " + sequence
							+ ", sessionId: " + sessionId);
				}
			
				result = doUp(Command.ABCAST_AGREEMENT, token);
				service.increaseSeqno(sessionId);
				releaseFTSequencesAfterward(token);
				// lock.notifyAll();
			}

			// synchronized (lock) {
			// TODO move this into the decision == 0 block?
			lock.notifyAll();
		}

		// For concurrent service, no need for increasing the sequencer
		// timestamp
		// ABBA, the 1st B sync, while 2nd B is concurrent
		if (decision == -1) {
			if (logger.isInfoEnabled()) {
				logger
						.info(UUID_ADDR
								+ " Execute service: "
								+ service.getName()
								+ ", sequence: "
								+ sequence
								+ ", sessionId : "
								+ sessionId
								+ ", this service is executed concurrently");
			}
		
			result = doUp(Command.ABCAST_AGREEMENT, token);
			// Notify the next ordered message to execute the corresponding
			// service
			synchronized (lock) {
				service.increaseConcurrentno(sessionId);
				lock.notifyAll();
			}
			

			releaseFTSequencesAfterward(token);
		}
		return result;
	}
	
	
	private void releaseFTSequencesAfterward(Token token){
		
		
		final DeliveryPacket deliveryPacket = (DeliveryPacket) token.getDataForNextProtocol();
		// Release executed cache sequence, we do not return though FT layer
		// here, only run on requester site
		if (deliveryPacket.getSequence() != null && deliveryPacket.getIHeader() != null) {
			doUp(Command.FT_AFTER_ABCAST_AGREEMENT, token);
		};
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
