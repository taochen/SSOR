package org.ssor;


/**
 * 
 * The non-deterministic invocation problem can be reduced to RNI problem, since
 * they are both case by redundant invocation. Therefore the solution that solve
 * non-deterministic problem can also solved RNI. Service can be identified as
 * Redundant Service (RS) if it involves non-deterministic invocation or RNI,
 * and it is stateless service, otherwise the service may be better to be
 * considered as AS or CS. In order to avoid redundant invocation, invocation of
 * RS is only allowed to occur on one site, and the update state is replicated
 * to other sites, thus it becomes deterministic. RS itself is non-replicable,
 * but AS or CS may contain one or more RS, and a RS may associate with multiple
 * AS or CS. RS also needs to obey the lazy replication, since it is
 * only invoked once and the return values are used identically on the other RM.
 * 
 * @author Tao Chen
 * 
 */
public class RedundantService extends AtomicService {

	public RedundantService(Class<?>[] parameterTypes, String name) {
		super(parameterTypes, name);
		if(logger.isDebugEnabled())
			logger.debug("Redundant service: " + name + " created");
	}

	public boolean isRequireBlocking() {
		return false;
	}

	public boolean isSequencer(int id) {
		return false;
	}

	public Object getMutualLock(String sessionId) {
		return new Object();
	}

	public int isExecutable(String sessionId, Sequence sequence) {
		return 0;
	}

	public void increaseSeqno(String sessionId) {
	}

	public void increaseConcurrentno(String sessionId) {

	}

	public Sequence getNextSeqno(String sessionId, int UUID_ADDR) {
		return null;
	}
}
