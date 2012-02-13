package org.ssor.protocol.replication;

import org.ssor.AtomicService;
import org.ssor.CompositeService;
import org.ssor.annotation.ProtocolBinder;
import org.ssor.protocol.Command;
import org.ssor.protocol.Message;
import org.ssor.protocol.Token;
import org.ssor.protocol.replication.abcast.DeliveryPacket;
import org.ssor.protocol.replication.abcast.MSP;
import org.ssor.util.Environment;
import org.ssor.util.Tuple;

@ProtocolBinder(managerClass=org.ssor.protocol.replication.ReplicationManager.class, 
		headers = {org.ssor.protocol.replication.RequestHeader.class, org.ssor.protocol.replication.ResponseHeader.class})
public class AR extends Replication {

	@Override
	public Token down(short command, Token value) {
		switch (command) {

		case Command.ABCAST_FIRST_UNICAST: {

			return request(command, value);
		}
		case Command.ABCAST_DELAY_FINAL_BROADCAST: {

			if (value == null)
				return doDown(command, value);

			final Message message = (Message) value.getData();
			// Only for single B, which means non-ordered needed message
			if (Environment.ENABLE_CHANGE_FIFO
					&& !message.isRequirePreBroadcasting()) {

				message.setFIFO_Scope(serviceManager.get(
						((RequestHeader) message.getHeader()).getService())
						.isFIFOEnabled() ? MSP.FIFO_SCOPE : util
						.getRandomShort());
			}

			return doDown(command, value);

		}/*
			 * case Command.REPLICATION_RELEASE_CACHED_SEQUENCE: {
			 * 
			 * if(value == null) doDown(command, value);
			 * 
			 * 
			 * return null; }
			 */
		}
		return doDown(command, value);
	}

	@Override
	public Token up(short command, Token value) {
		switch (command) {

		// Used when order timestamp received
		case Command.ABCAST_ACQUIRE_SEQUENCE: {

			if (value == null)
				return doUp(command, value);

			@SuppressWarnings("unchecked")
			final Tuple<Tuple<String, Message>, Tuple<Integer, Object>> tuple = (Tuple) value.getDataForNextProtocol();
			final AtomicService service = serviceManager.get(tuple.getVal1().getVal1());

			// Decide if can execute the last broadcast
			if (Token.REPLICATION_ACQUIRE_SEQUENCE_NOT_COLLECTED != value.getNextAction()) {
				if (service.isRedundant()
						|| service instanceof CompositeService) {

					if (logger.isTraceEnabled()) {
						trace(
								logger,
								"ABCAST_ACQUIRE_SEQUENCE",
								"Service: "
										+ service
										+ " requires delay the final broadcast util execution complete");
					}
					
					value.setNextAction(Token.REPLICATION_ACQUIRE_LAZY_BORADCAST);

				} else {

					if (logger.isTraceEnabled()) {
						trace(logger, "ABCAST_ACQUIRE_SEQUENCE", "Service: "
								+ service
								+ " allows execute the final broadcast");
					}
					
					value.setNextAction(Token.REPLICATION_ACQUIRE_BORADCAST);

				}
			}

			return doUp(command, value);

		}
		case Command.ABCAST_AGREEMENT: {

			if (value == null)
				return doUp(command, value);

			final DeliveryPacket packet = (DeliveryPacket)value.getDataForNextProtocol();
			/*
			 * Only react for certain data, otherwise pass it forward
			 */
			if (packet.getService().isReplicateOnReplica()){
				packet.setResult(delivery(packet));
				
			}

			return doUp(command, value);
		}
		}
		return doUp(command, value);
	}

}
