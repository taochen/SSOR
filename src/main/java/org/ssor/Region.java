package org.ssor;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ssor.exception.ConfigurationException;

/**
 * This region represents for different consistency requirement, however, this
 * class itself has no concurrent control, therefore synchronizations need to
 * specified in the protocol that invokes it.
 * 
 * Even the notion of concurrent delivery is still suffering transitive ordering
 * when notion of region does not
 * 
 * @author Tao Chen
 * 
 */

public class Region {

	protected static final Logger logger = LoggerFactory
			.getLogger(Region.class);

	public static final int NON_CONFLICT_REGION = 0;

	public static final int SESSIONAL_CONFLICT_REGION = 1;

	public static final int CONFLICT_REGION = 2;
	// -1 is session level region
	protected int region;
	protected int scope = CONFLICT_REGION;
	protected volatile int expectedSeqno = 0;
	protected int seqno = -1;
	protected volatile int sequencer;

	/*
	 * When change sequencer during election, lock would be placed on the entire
	 * region instance
	 */

	// Used to sync ordering execution
	// Should not use service instance, since it needs to
	// perform some concurrent works
	protected Object mutualLock = new Byte[0];
	// Maintain mutual exclusion while assigning sequence
	protected Object assignLock = new Byte[0];

	// Use hash set here since it only need to add 1 entry,
	// iteration and remove all entries, no duplicate entry needed
	protected Set<String> lastServices = new HashSet<String>();
	//protected String lastServiceName = "";

	protected int concurrentno = 0;

	protected int expectedConcurrentno = 0;

	@SuppressWarnings("unchecked")
	protected Queue<Sequence> skippedSequences = CollectionFacade
			.getPriorityQueue();

	// Used by the new sequencer to collect current state of other nodes, so it
	// that can reach the same state
	@SuppressWarnings("unchecked")
	protected Queue<Sequence> collectedSequences = CollectionFacade
			.getPriorityQueue();

	protected Sequence latestSequence;

	// The view during decentralized consensus
	protected Queue<Integer> consensusView;
	protected volatile boolean isAllowRequest = true;

	// protected volatile boolean isSequencerConsensus = false;

	// Used for sessional region only
	protected Region() {
		super();
		this.scope = CONFLICT_REGION;
	}

	public Region(int scope) {
		super();
		this.scope = scope;
	}

	public Region(int region, int scope) {
		super();
		if (region == SessionRegion.SESSION_REGION) {
			throw new ConfigurationException(
					"The region number -1 is used for session level consistency service");
		}
		this.region = region;
		this.scope = scope;
	}

	public Region(int region, int scope, int sequencer) {
		super();
		if (region == SessionRegion.SESSION_REGION) {
			throw new ConfigurationException(
					"The region number -1 is used for session level consistency service");
		}

		this.region = region;
		this.scope = scope;
		this.sequencer = sequencer;
	}

	public void setSequencer(Integer sequencer) {
		synchronized (this) {
			this.sequencer = sequencer;
		}
	}

	public int getRegion() {
		return region;
	}

	public int getScope() {
		return scope;
	}

	public Sequence getExpectedSequence() {
		return new Sequence(expectedSeqno, expectedConcurrentno);
	}

	public void setState(Sequence sequence) {
		expectedSeqno = sequence.getSeqno();
		if (sequence.getConcurrentno() == null
				|| sequence.getConcurrentno() == -1)
			expectedConcurrentno = 0;
		else
			expectedConcurrentno = sequence.getConcurrentno();
	}

	public int getSeqno() {
		return seqno;
	}

	public int getSequencer() {
		return sequencer;
	}

	public int isBlocking(String sessionId, Integer order, Integer concurrent) {
		// System.out.print(expectedTimestamp + ":" + order + ", " +
		// exceptedConcurrentServiceTimestamp+ ":"+concurrent+" execute
		// *********\n");

		if (!isRequireBlocking())
			return 0;
		// System.out.print(expectedTimestamp + ":" + order + "*********\n");
		if (concurrent == null && expectedSeqno == order)
			return 0;
		else if (concurrent != null && expectedConcurrentno == concurrent
				&& expectedSeqno == order)
			return 0;
		else if (expectedSeqno > order)
			return -1;
		else
			return 1;

		// return i;

	}

	public int isExecutable(String sessionId, Sequence sequence,
			AtomicService service) {
		// System.out.print(expectedTimestamp + ":" + order + ", " +
		// exceptedConcurrentServiceTimestamp+ ":"+concurrent+" execute
		// *********\n");

		int result = 0;

		if (!isRequireBlocking())
			result = 0;
		// System.out.print(expectedTimestamp + ":" + order + "*********\n");
		if (sequence.getConcurrentno() == null
				&& expectedSeqno == sequence.getSeqno())
			result = 0;
		else if (sequence.getConcurrentno() != null
				&& expectedConcurrentno == sequence.getConcurrentno()
				&& expectedSeqno == sequence.getSeqno()) {
			// We can put the reset of expectedConcurrentno here
			// since there is not two sequences with the same seqno
			// and concurrentno is not null
			expectedConcurrentno = 0;
			result = 0;
		} else if (expectedSeqno > sequence.getSeqno())
			result = -1;
		else
			result = 1;

		if(result != 1)
		System.out.print("Region: " + region + " Result: " + result + ", " +
		 sequence + ", except: " + expectedSeqno + ", concurrent: " +
		 expectedConcurrentno+ " service " + service.name + "\n");

		result = faultTolerance(sessionId, sequence, service, result, this);

		return result;

	}

	public void increaseConcurrentno(String sessionId) {

		// synchronized (this) {

		expectedConcurrentno++;
		// System.out.print(exceptedConcurrentServiceTimestamp + ":\n" );
		// if(!isSequencer())
		// concurrentSequencerTimestamp = exceptedConcurrentServiceTimestamp;
		// }
	}

	/**
	 * This can only be invoked by the thread that finishes invocation, or by
	 * failure manager in case of failure.
	 * 
	 * All original Region instance does not make use of sessionId at all!
	 */
	public void increaseSeqno(String sessionId) {

		if(region==3)System.out.print("pass: " + expectedSeqno + "\n");
		if (isRequireBlocking()) {

			// This is need since this may be changed by other services that
			// register this region as passively interested
			// synchronized (this){
			// If no concurrent change, then should be the same
			// if(originalExceptedTimestamp == expectedTimestamp){
			// System.out.print(expectedSeqno + "\n");
			expectedSeqno++;

			// For node to keep sync in case of failure
			// if(!isSequencer()){
			// sequencerTimestamp = expectedTimestamp;
			// lastServiceName = service.name;
			// }
			//
			// }
			// System.out.print(expectedTimestamp + "finish *********\n");
			// }

			if (logger.isDebugEnabled()) {
				logger.debug("Region: " + region + ", excepted timestamp: "
						+ expectedSeqno);
			}
		}
	}

	public Sequence getNextSeqno(String sessionId, AtomicService service,
			int UUID_ADDR) {

		int nextTimestamp = 0;
		Integer last = null;
		boolean isPass = true;
		if (isRequireBlocking()) {
			// This is need since this may be changed by other services that
			// register this region as passively interested
			synchronized (assignLock) {
				if (isSequencer(UUID_ADDR)) {

					// If the previous ones do not need concurrent treatment
					if (!isConcurrentDeliverable(service)) {
						seqno++;
						//lastServiceName = service.name;
						last = concurrentno;
						concurrentno = 0;
						// System.out.print(last + "end concurrent
						// *********\n");
						// Clear the CDS set
						lastServices.clear();
					} else {
						last = -1;
						concurrentno++;
					}
					nextTimestamp = seqno;
					// Add the service into CDS set
					lastServices.add(service.name);
                    // lastServiceName = service.name;
					// System.out.print("Sequencer: " + sequencer + " Service: "
					// + service.name + " New sequence: " + new Sequence(region,
					// nextTimestamp, last == 0 ? null : last)+ "\n" );
				} else
					isPass = false;

			}

			if (logger.isDebugEnabled()) {
				logger.debug("Region: " + region + ", sequencer timestamp: "
						+ nextTimestamp + " concurrent timestamp: "
						+ concurrentno);
			}

		}
		 if(region==1)
		System.out.print("seqno: " + nextTimestamp + "\n");
		// last may be null
		return isPass ? new Sequence(region, nextTimestamp, last == 0 ? null
				: last) : null;
	}

	public boolean isRequireBlocking() {
		return !(scope == NON_CONFLICT_REGION);
	}

	public Object getMutualLock(String sessionId) {

		// If no blocking need, then return a new object everytime
		// since no flag can be placed on a identical instance
		if (!isRequireBlocking())
			return new Byte[0];

		return mutualLock;
	}

	public void addSkippedSequence(Sequence sequence) {

		System.out.print("for FT: " + sequence + "\n");
		skippedSequences.add(sequence);
	}

	public synchronized void addCollectedSequences(String sessionId,
			Integer id, Sequence[] sequences, Sequence latestSequence) {
		if (consensusView != null && !consensusView.remove(id)) {

			if (logger.isDebugEnabled()) {
				logger.debug("No view (node) or no sequence array");
			}

			return;
		}

		if (this.latestSequence == null) {

			for (Sequence sequence : sequences)
				collectedSequences.add(sequence);

			this.latestSequence = latestSequence;
			return;
		}

		switch (this.latestSequence.compareTo(latestSequence)) {

		case 1: {

			for (Sequence sequence : sequences) {
				if (this.latestSequence.compareTo(sequence) <= 0)
					collectedSequences.add(sequence);
			}

			if (logger.isDebugEnabled()) {
				logger.debug("Previous latest sequence: " + this.latestSequence
						+ ", courrent sequence: " + latestSequence
						+ ", previous one is later");
			}

			break;
		}

		case -1: {

			int loop = 0;
			for (Sequence sequence : collectedSequences) {
				if (latestSequence.compareTo(sequence) > 0)
					loop++;
				else
					break;
			}

			// Remove
			for (int i = 0; i < loop; i++)
				collectedSequences.poll();

			for (Sequence sequence : sequences) {
				collectedSequences.add(sequence);
			}

			if (logger.isDebugEnabled()) {
				logger.debug("Previous latest sequence: " + this.latestSequence
						+ ", current sequence: " + latestSequence
						+ ", current one is later");
			}

			this.latestSequence = latestSequence;

			break;
		}

		case 0: {

			// Does not need to take care concurrent sequence since they can
			// only be removed after the subsequent normal sequence
			// has been processed
			for (Sequence sequence : sequences) {
				collectedSequences.add(sequence);
			}

			if (logger.isDebugEnabled()) {
				logger.debug("Previous latest sequence: " + this.latestSequence
						+ ", courrent sequence: " + latestSequence
						+ ", they are equals in terms of normal sequence");
			}

			break;
		}
		}

	}

	public boolean isSequenceCollectionFinished() {
		return consensusView == null? true : consensusView.size() == 0;
	}

	/**
	 * This should be thread safe
	 * 
	 * @return
	 */
	public List<Sequence> extractSkippedSequences() {

		// Collection of the sequenecs that needs to avoid
		final List<Sequence> sequences = new ArrayList<Sequence>();

		if (latestSequence == null)
			return sequences;

		final int length = collectedSequences.size();
		Sequence previous = new Sequence(latestSequence.getSeqno() - 1, null);
		Sequence current = null;
		int concurrent = 0;
		for (int i = 0; i < length; i++) {

			if (current == null)
				current = collectedSequences.poll();

			if (logger.isDebugEnabled()) {
				logger.debug("Previous sequence: " + previous
						+ ", current sequence: " + current);
			}

			switch (previous.isSubsequent(current, concurrent)) {

			// If it is the subsequent one, and can be end up with this
			case 0: {

				if (logger.isDebugEnabled()) {
					logger.debug("Current sequence: " + current
							+ " is the subsequence of: " + previous);
				}

				concurrent = 0;
				previous = current;
				current = null;
				break;
				// If it is not the subsequent one, and can not be end up
				// with this
			}
			case 1: {

				if (current.getConcurrentno() == null) {

					// <1,null>, <3, null> or <1,2>, <3,null>
					if (null == previous.getConcurrentno()
							|| (null != previous.getConcurrentno() && -1 != previous
									.getConcurrentno())) {
						previous = new Sequence(previous.getSeqno() + 1, null);

						if (logger.isDebugEnabled()) {
							logger
									.debug("Sequence is missing after a normal seqeunce, the missing sequence is: "
											+ previous);
						}

						// <1,-1>, <3, null>
					} else {
						concurrent++;
						previous = new Sequence(previous.getSeqno() + 1,
								concurrent);
						concurrent = 0;

						if (logger.isDebugEnabled()) {
							logger
									.debug("Sequence that following serial of concurrent sequence is missing, the missing sequence is: "
											+ previous);
						}
					}
				} else if (current.getConcurrentno() == -1) {
					// <1,-1>, <3, -1>
					if (previous.getConcurrentno() != null
							&& previous.getConcurrentno() == -1) {
						previous = new Sequence(previous.getSeqno() + 1,
								concurrent);
						concurrent = 0;
						// <1,null/2>, <3, -1> or <2,null/2>, <3,-1>
					} else
						previous = new Sequence(previous.getSeqno() + 1, null);

					if (logger.isDebugEnabled()) {
						logger
								.debug("Sequence is missing, the current sequence is concurrent, and the missing sequence is: "
										+ previous);
					}

				} else {

					// <2,null>, <3, 2> or <2,-1/2>, <3, 2>
					if (previous.getSeqno().equals(current.getSeqno() - 1)) {
						previous = new Sequence(previous.getSeqno(), -1);
						concurrent++;

						if (logger.isDebugEnabled()) {
							logger
									.debug("Concurrent sequence is missing, the missing sequence is: "
											+ previous
											+ ", concurrent count: "
											+ concurrent);
						}

						// <1,-1/2>, <3, 2> or <1,null>, <3, 2>
					} else {

						if (null != previous.getConcurrentno()
								&& previous.getConcurrentno() == -1) {
							previous = new Sequence(previous.getSeqno() + 1,
									concurrent);
							concurrent = 0;
						} else
							previous = new Sequence(previous.getSeqno() + 1,
									null);

						if (logger.isDebugEnabled()) {
							logger
									.debug("Sequence is missing, the current sequence is normal, and the missing sequence is: "
											+ previous);
						}
					}
				}

				// Add 1 more round
				i--;
				previous.setRegionNumber(region);
				sequences.add(previous);
				break;
				// If it is the concurrent one, and can be end up with this
			}
			case -1: {

				if (logger.isDebugEnabled()) {
					logger
							.debug("(Concurrent )Current sequence: "
									+ current
									+ " is the subsequence of: "
									+ previous
									+ ", number of this serial of concurrent sequence: "
									+ concurrent);
				}

				concurrent++;
				previous = current;
				current = null;
				break;
			}

			}

		}

		synchronized (mutualLock) {
			if (previous != null && previous != latestSequence) {
				seqno = previous.getSeqno();
				if (previous.getConcurrentno() == null
						|| previous.getConcurrentno() == -1)
					concurrentno = 0;
				else
					concurrentno = previous.getConcurrentno();
			} else {
				seqno = latestSequence.getSeqno() - 1;
				if (latestSequence.getConcurrentno() == null
						|| latestSequence.getConcurrentno() == -1)
					concurrentno = 0;
				else
					concurrentno = latestSequence.getConcurrentno() - 1;
			}
			lastServices.clear();
			//lastServiceName = "";
		}
		// System.out.print("region: " + region + "last: "
		// + (latestSequence.getSeqno() - 1) + "seqno: " + seqno + "\n");
		collectedSequences.clear();
		latestSequence = null;
		consensusView = null;
		System.out.print("new setting, seqno: " + seqno + " concurrentno: " + concurrentno + "\n");
		return sequences;
	}

	/**
	 * This itself should be used in thread safe way, the skippedSequences is
	 * not thread safe, however, the entire function should be used within the
	 * sync block on the mutualLock object
	 * 
	 * @param sessionId
	 * @param incoming
	 * @param service
	 * @param result
	 * @param region
	 * @return
	 */
	protected int faultTolerance(String sessionId, Sequence incoming,
			AtomicService service, int result, Region region) {

		// Only work for execution of service that is blocked
		if (!skippedSequences.isEmpty() && result == 1) {
			Sequence sequence = skippedSequences.peek();
			System.out.print("FT: " + sequence + "\n");

			if (logger.isDebugEnabled()) {
				logger.debug("Sequence that needs to be skipped: " + sequence);
			}
			// If the skipped one is normal timestamped
			if (sequence.getConcurrentno() == null
					&& region.expectedSeqno == sequence.getSeqno()) {

				synchronized (region.getMutualLock(sessionId)) {
					// This may need for the first one, such as identified as
					// <12, null>, while the
					// current except is <12,3> and all the 12 are missing
					region.expectedConcurrentno = 0;
					region.expectedSeqno++;
				}
				skippedSequences.poll();

				if (logger.isDebugEnabled()) {
					logger.debug("Skip nomral sequence: " + sequence
							+ ", incoming: " + incoming);
				}

				return isExecutable(sessionId, incoming, service);
				// If the skipped one is the first ordered message after
				// concurrent event
			} else if (sequence.getConcurrentno() != null
					&& region.expectedConcurrentno == sequence
							.getConcurrentno()
					&& region.expectedSeqno == sequence.getSeqno()) {

				synchronized (region.getMutualLock(sessionId)) {
					region.expectedConcurrentno = 0;
					region.expectedSeqno++;
				}
				skippedSequences.poll();

				if (logger.isDebugEnabled()) {
					logger
							.debug("Skip normal sequence that following a concurrent sequence: "
									+ sequence + ", incoming: " + incoming);
				}

				return isExecutable(sessionId, incoming, service);

				// If the skipped one is concurrent
			} else if (sequence.getConcurrentno() != null
					&& -1 == sequence.getConcurrentno()) {

				synchronized (region.getMutualLock(sessionId)) {
					region.expectedConcurrentno++;
				}
				skippedSequences.poll();

				if (logger.isDebugEnabled()) {
					logger.debug("Skip concurrent sequence: " + sequence
							+ ", incoming: " + incoming);
				}

				return isExecutable(sessionId, incoming, service);
			}

		}

		return result;
	}

	public boolean isSequencer(int id) {
		return sequencer == id;

	}

	public boolean isAllowRequest() {
		return isAllowRequest;

	}

	public Integer getSequencerWhenRequest() {

		synchronized (this) {
			if (!isAllowRequest) {

				if (logger.isErrorEnabled()) {
					logger
							.error("The incoming request is suspended due to sequencer failed");
				}

				/*
				 * try { this.wait(); } catch (InterruptedException e) {
				 * e.printStackTrace(); }
				 */

				return null;
			}

			return sequencer;
		}

	}

	public void allowRequest() {

		if (logger.isDebugEnabled()) {
			logger.debug("Unblock the region: " + region);
		}
		synchronized (this) {
			this.isAllowRequest = true;
			// this.notifyAll();
		}

	}

	public int changeSequencerAndSuspendRequests(int id) {
		int old = 0;
		synchronized (this) {
			// This should be done in record phase, if this is false
			if (sequencer != id) {
				old = sequencer;
				sequencer = id;
			}

			isAllowRequest = false;

		}

		return old;
	}

	@SuppressWarnings("unchecked")
	public void setConsensusView(Queue<Integer> view) {
		Queue<Integer> q = CollectionFacade.getConcurrentQueue();
		q.addAll(view);
		this.consensusView = q;

	}

	public boolean isContainInView(Integer id) {
		if (consensusView == null)
			return false;

		return consensusView.remove(id);
	}

	public void trigger(String sessionId) {
		synchronized (mutualLock) {
			mutualLock.notifyAll();
		}
	}

	public String toString() {
		
		if(scope == Region.NON_CONFLICT_REGION)
			return "(non-conflict region)";
		
		
		return "(Region number: " + region + ", seqno#: " + seqno
				+ ", concurrentno#: " + concurrentno + ", expected_seqno#: "
				+ expectedSeqno + ", expected_concurrentno#: "
				+ expectedConcurrentno + ")";
	}

	/**
	 * This function should only be used in a thread safe and synchronized (via assignLock) manner.
	 * @param service the given service
	 * @return true if allow concurrent delivery, false otherwise
	 */
	protected boolean isConcurrentDeliverable (AtomicService service) {
		if(lastServices.isEmpty()) {
			return false;
		}
		
		final Iterator<String> itr = lastServices.iterator();
		while (itr.hasNext()) {
			if(!service.isConcurrentDeliverable(itr.next())) {
				// Return false even if only one of the service in the set
				// can not be delivered concurrently with the given service.
				return false;
			}
		}
		return true;
	}
}
