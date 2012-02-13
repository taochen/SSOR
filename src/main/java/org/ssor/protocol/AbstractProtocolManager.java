package org.ssor.protocol;

import org.slf4j.Logger;
import org.ssor.Sequence;
import org.ssor.protocol.replication.RequestHeader;
import org.ssor.protocol.replication.ResponseHeader;
import org.ssor.util.Callback;
import org.ssor.util.Group;

public abstract class AbstractProtocolManager implements ProtocolManager{

	protected Group group;
	protected ProtocolStack protocolStack;

	private static ThreadLocal<Boolean> local = new ThreadLocal<Boolean>();

	public void initializeProtoclManager (Group group) {
		this.group = group;
		protocolStack = group.getProtocolStack();
	}
	
	protected boolean suspendProcess(Message message, Logger logger,
			final Callback callback) {

		// This ensure it only sync if during consensus
		// This also allow maximum concurrency, and guarantee sync
		if (group.isConsensus()) {

			if (local.get() == null) {
				new Thread(new Runnable() {

					@Override
					public void run() {
						local.set(true);
						callback.run();

					}

				}).start();
				

				group.increaseSuspendedNo();
				return false;
			}

			if (logger.isErrorEnabled()) {
				logger
						.error("Message received for agreement but suspend due to cnosensus");
			}

			// During consensus, no process is allowed
			synchronized (group.getCONSENSUS_LOCK()) {
				while (group.isConsensus()) {

					try {
						// This would be notify when consensus finish
						group.getCONSENSUS_LOCK().wait();
					} catch (InterruptedException e) {
						logger
						    .error("Waitting on group consensus failed");
					}
				}

				if (logger.isErrorEnabled()) {
					logger.error("suspend finish!");
				}
			}

			setStateFromMessage(message);
			group.isFinishSuspendMsgsProcess();
			local.remove();
			
		} else if(local.get() != null) {
			setStateFromMessage(message);
			group.isFinishSuspendMsgsProcess();
			local.remove();
		}
		
		return true;

	}
	
	private void setStateFromMessage(Message message){
		/*
		 * For catching the latest state
		 */
		final RequestHeader header = (RequestHeader) message.getHeader();
		final ResponseHeader responseHeader = (ResponseHeader) header.getOuter();
		if (responseHeader.getTimestamp() != null) {
			if (responseHeader.getTimestamp() instanceof Sequence[]) {

				Sequence[] sequences = (Sequence[]) responseHeader
						.getTimestamp();
				for (Sequence sequence : sequences) {
					group.setState(header.getService(), header
							.getSessionId(), sequence);
				}
				
			} else {
				group.setState(header.getService(), header.getSessionId(),
						(Sequence) responseHeader.getTimestamp());
			}
		}
	}
}
