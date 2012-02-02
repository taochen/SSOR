package org.ssor.protocol.replication;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ssor.ServiceStackContext;
import org.ssor.CompositeService;
import org.ssor.RedundantService;
import org.ssor.invocation.InvocationAdaptor;
import org.ssor.invocation.InvocationCallback;
import org.ssor.invocation.InvocationHeader;
import org.ssor.invocation.InvocationUnit;
import org.ssor.listener.InvocationAware;
import org.ssor.protocol.Command;
import org.ssor.protocol.RequirementsAwareProtocol;
import org.ssor.protocol.Token;
import org.ssor.protocol.replication.abcast.DeliveryPacket;

public abstract class Replication extends RequirementsAwareProtocol implements InvocationAware{

	protected static final Logger logger = LoggerFactory
	.getLogger(Replication.class);

	protected InvocationAdaptor invocater;
	
	
	protected Token request(short command, Token value){
		

		if (value == null)
			return doDown(command, value);

		InvocationUnit unit = (InvocationUnit) value.getData();
		if (ServiceStackContext.pushService(unit.getService(), unit
				.getArguments())) {

			if (unit.getService() instanceof RedundantService) {
				if (logger.isTraceEnabled())
					trace(logger, "ABCAST_FIRST_UNICAST", "service: "
							+ unit.getService()
							+ " is util service");
				value.setNextAction(Token.REPLICATION_REQUEST_NON_REPLICABLE);
				value.setDataForNextProtocol(null);
				return value;
			}

			if (unit.getService() instanceof CompositeService
					|| unit.getService().isRedundant()) {
				if (logger.isTraceEnabled())
					trace(logger, "ABCAST_FIRST_UNICAST", "service: "
							+ unit.getService()
							+ " is util service");
				return doDown(Command.ABCAST_FIRST_UNICAST, value);

			}

			if (!unit.getService().isRequireBlocking()) {
				if (logger.isTraceEnabled())
					trace(logger, "ABCAST_FIRST_UNICAST", "service: "
							+ unit.getService()
							+ " does not need total order");
				// This should be null
				return doDown(Command.ABCAST_NON_ORDERED_BROADCAST, value);

			}
			
			if (logger.isTraceEnabled())
				trace(logger, "ENTER_SERVICE", "service: "
						+ unit.getService() + " is replicatable");
			
			return doDown(Command.ABCAST_FIRST_UNICAST, value);
		} else {

			if (logger.isTraceEnabled())
				trace(logger, "ENTER_SERVICE", "service: "
						+ unit.getService()
						+ " is not replicatable");
			value.setNextAction(Token.REPLICATION_REQUEST_NON_REPLICABLE);
			value.setDataForNextProtocol(null);
			return value;
		}
	}
	/**
	 * 
	 * @param packet
	 * @return
	 */
	protected Object delivery(DeliveryPacket packet) {

		if (null == packet.getIHeader()) {

			// Avoid nested invocation of replica
			ServiceStackContext.markReplica();
			// Replication normally do not return value
			invocater.invoke(packet.getService(), packet.getArguments());

			ServiceStackContext.releaseReplicaMarker();

			return null;
			// This is the execution for trigger requester thread
			// Therefore it is not a replica in this moment
		} else {

			/* Release executed cache sequence, we do not return though FT layerhere
			 *
			if (packet.getSequence() != null) {
				doUp(Command.ABCAST_AGREEMENT, new Tuple<AtomicService, Object>(
						packet.getService(), packet.getSequence()));
			}*/

			return ((InvocationHeader) packet.getIHeader()).invoke();
		}
	}
	
	public void setInvocationAdaptor(InvocationAdaptor adaptor){
		invocater = adaptor;
	}
	
	public void setCallback(InvocationCallback callback) {
		this.invocater.setCallback(callback);
	}
}
