package org.ssor;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class SessionRegion extends Region {

	private Map<String, Region> sessions;

	public final static int SESSION_REGION = -1;

	@SuppressWarnings("unchecked")
	public SessionRegion() {
		super(Region.SESSIONAL_CONFLICT_REGION);
		region = SESSION_REGION;
		sessions = CollectionFacade.getConcurrentHashMap(100);
	}

	public Region add(String sessionId) {
		Region region = new Region();
		sessions.put(sessionId, region);
		return region;
	}

	public void remove(String sessionId) {
		sessions.remove(sessionId);
	}

	public int isExecutable(String sessionId, Sequence sequence,
			AtomicService service) {
		final Region sessionRegion = get(sessionId);
		int result = 0;
		// TODO FD handler when session expiry unexptecly
		if (sequence.getConcurrentno() == null
				&& sessionRegion.expectedSeqno == sequence.getSeqno())
			result = 0;
		else if (sequence.getConcurrentno() != null
				&& sessionRegion.expectedConcurrentno == sequence
						.getConcurrentno()
				&& sessionRegion.expectedSeqno == sequence.getSeqno()) {
			sessionRegion.expectedConcurrentno = 0;
			result = 0;
		} else if (sessionRegion.expectedSeqno > sequence.getSeqno())
			result = -1;
		else
			result = 1;

		result = faultTolerance(sessionId, sequence, service, result,
				sessionRegion);

		return result;
	}

	public int isBlocking(String sessionId, Integer order, Integer concurrent) {
		Region sessionRegion = get(sessionId);
		// TODO FD handler when session expiry unexptecly
		if (concurrent == null && sessionRegion.expectedSeqno == order)
			return 0;
		else if (concurrent != null
				&& sessionRegion.expectedConcurrentno == concurrent
				&& sessionRegion.expectedSeqno == order)
			return 0;
		else if (sessionRegion.expectedSeqno > order)
			return -1;
		else
			return 1;
	}

	public void increaseConcurrentno(String sessionId) {
		Region sessionRegion = get(sessionId);
		// synchronized (this) {
		sessionRegion.expectedConcurrentno++;
		// if(!isSequencer())
		// sessionRegion.concurrentSequencerTimestamp =
		// sessionRegion.exceptedConcurrentServiceTimestamp;
		// }
	}

	/**
	 * This can only be invoked by the thread that finishes invocation, or by
	 * failure manager in case of failure.
	 */
	public void increaseSeqno(String sessionId) {
		// TODO FD handler when session expiry unexptecly
		Region sessionRegion = get(sessionId);
		// This is need since this may be changed by other services that
		// register this region as passively interested
		// synchronized (sessionRegion){
		// if(originalExceptedTimestamp == sessionRegion.expectedTimestamp){
		sessionRegion.expectedSeqno++;

		// For node to keep sync in case of failure
		// if(!isSequencer()){
		// sessionRegion.sequencerTimestamp = sessionRegion.expectedTimestamp;
		// sessionRegion.lastServiceName = service.name;
		// }

		// }
		// }

		if (logger.isDebugEnabled()) {
			logger.debug("Region: " + sessionRegion.region
					+ ", excepted timestamp: " + sessionRegion.expectedSeqno);
		}
	}

	@Override
	public Sequence getNextSeqno(String sessionId, AtomicService service,
			int UUID_ADDR) {
		// TODO FD handler when session expiry unexptecly
		final Region sessionRegion = get(sessionId);
		int nextTimestamp = 0;
		Integer last = null;
		boolean isPass = true;
		// This is need since this may be changed by other services that
		// register this region as passively interested
		synchronized (sessionRegion.assignLock) {
			if (isSequencer(UUID_ADDR)) {
				// If the previous one does not need relax treatment
				if (!sessionRegion.isConcurrentDeliverable(service)) {
					sessionRegion.seqno++;
					//sessionRegion.lastServiceName = service.name;
					last = sessionRegion.concurrentno;
					sessionRegion.concurrentno = 0;
					sessionRegion.lastServices.clear();
				} else {
					last = -1;
					sessionRegion.concurrentno++;
				}
				nextTimestamp = sessionRegion.seqno;
				sessionRegion.lastServices.add(service.name);

			} else
				isPass = false;
		}

		if (logger.isDebugEnabled()) {
			logger.debug("Region: " + sessionRegion.region
					+ ", sequencer timestamp: " + nextTimestamp);
		}

		return isPass ? new Sequence(region, sessionId, nextTimestamp,
				last == 0 ? null : last) : null;
	}

	public void addSkippedSequence(Sequence sequence) {
		get(sequence.getSessionId()).skippedSequences.add(sequence);
	}

	public synchronized void addCollectedSequences(String sessionId,
			Integer id, Sequence[] sequences, Sequence latestSequence) {
		if (consensusView != null
				&& (!consensusView.remove(id) || sequences.length == 0))
			return;
		get(sessionId).addCollectedSequences(sessionId, id, sequences,
				latestSequence);

	}

	public Object getMutualLock(String sessionId) {
		return sessionId == null ? mutualLock : get(sessionId).mutualLock;
	}

	public Sequence getExpectedSequence(String sessionId) {
		return new Sequence(get(sessionId).expectedSeqno,
				get(sessionId).expectedConcurrentno);
	}

	public void setState(String sessionId, Sequence sequence) {
		get(sessionId).expectedSeqno = sequence.getSeqno();
		if (sequence.getConcurrentno() == null
				|| sequence.getConcurrentno() == -1)
			get(sessionId).expectedConcurrentno = 0;
		else
			get(sessionId).expectedConcurrentno = sequence.getConcurrentno();
	}

	public void triggerAll() {
		Object lock = null;
		for (Map.Entry<String, Region> entry : sessions.entrySet()) {

			lock = entry.getValue().mutualLock;
			synchronized (lock) {
				lock.notifyAll();
			}
		}

	}

	public void extractExpectedSequence(List<State> list) {

		for (Map.Entry<String, Region> entry : sessions.entrySet()) {

			list.add(new State(SESSION_REGION, entry.getKey(), entry.getValue()
					.getExpectedSequence()));

		}
	}

	public void trigger(String sessionId) {
		get(sessionId).trigger(null);
	}

	private Region get(String sessionId) {
		Region region = null;
		if ((region = sessions.get(sessionId)) == null)
			return add(sessionId);

		return region;
	}

	public String toString() {

		String str = "(Sessional conflict region, include: ";
		Set<Map.Entry<String, Region>> set = sessions.entrySet();
		for (Map.Entry<String, Region> entry : set) {
			str += "seqno#: " + entry.getValue().seqno + ", concurrentno#: "
					+ entry.getValue().concurrentno + ", expected_seqno#: "
					+ entry.getValue().expectedSeqno
					+ ", expected_concurrentno#: "
					+ entry.getValue().expectedConcurrentno + "\n";
		}

		str += ")";

		return str;
	}
	


}
