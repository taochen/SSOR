package org.ssor;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ssor.protocol.Message;
import org.ssor.util.Callback;
import org.ssor.util.Group;

/**
 * 
 * For Region Distribution Synchrony (RDS), this also implement VS6 and VS7 (we
 * don not consider Byzantine failure here)
 * 
 * Traditional Virtual Synchrony (VS) consists of the following 5 properties:
 * 
 * VS1: Equivalents to RM1, that is, it can be satisfied if RM1 is hold. VS2:
 * Equivalents to RM2, that is, it can be satisfied if RM2 is hold. VS3: If a
 * process p delivery message m in view V, then p belongs to V. VS4: If a
 * process delivery view V1 then V2, then no other processes delivery V2 before
 * V1. VS5: if a process sends message m in view V1, and another process
 * delivery m in view V2, then V1=V2.
 * 
 * We additionally identify the following two:
 * 
 * VS6: If a process delivery message m, then the sender of m belongs to current
 * view VS7: The processes that triggered by message delivery are serializable
 * with view delivery.
 * 
 * We identify Region Distribution Synchrony (This does not include tolerance of
 * non-sequencer crash, since it does not require any information regarding to
 * the distribution of regions), based on the properties provided by view
 * synchrony. Note that although each delivery of view may implies numbers of
 * new/crash nodes, each run of REP and sequencer crash tolerance protocol can
 * only work for a particular node, and this can be achieved by running the
 * protocol for each new/crash node according to their order in the delivered
 * view, since the order within the view is guaranteed by GCS.
 * 
 * Region Distribution Synchrony specifies rules for maintaining synchronization
 * when changing regions between protocols:
 * 
 * Rule 1: All nodes handle protocol for joining node in the same order This is
 * essential since two or more simultaneous joining node may competing for the
 * same region, therefore to handle them in order gain better balanced
 * distribution, and this can be easily realized based on VS4.
 * 
 * Rule 2: All nodes handle protocol for sequencer crash in arbitrary order
 * Handling crash different sequencers is feasible because of the fact that only
 * one sequencer for each region in the group for a certain period of time.
 * 
 * Rule 3: All nodes handle protocol for joining node and protocol for sequencer
 * crash in the same order This is needed because the joining node may demands a
 * region that currently handled by a tolerance protocol due to crash of
 * original sequencer, in addition, extra regions assign to a sequencer (due to
 * sequencer crash) during the election may differ the global view of node
 * amongst different nodes. Protocol for joining node only allowed to be run
 * after the completion of protocol for sequencer crash, however, tolerance
 * protocol only needs to wait until the Agreement phase of REP complete, since
 * the new sequencer is elected for certain regions upon that phase in REP. This
 * rule can be easily realized based on VS4.
 * 
 * 
 * @author Tao Chen
 * 
 */
@SuppressWarnings("unchecked")
public class RegionDistributionSynchronyManager {

	private static final Logger logger = LoggerFactory
			.getLogger(RegionDistributionSynchronyManager.class);

	private ViewID current;

	private Queue<ViewID> queue;

	private Object mutualLock = new Byte[0];

	private ThreadLocal<Object> isNeedNewThread = new ThreadLocal<Object>();

	// <View Id, list of node ID that willing to become new sequencer of
	// distinct regions>,
	// this only used for view change of leaving
	private Map<Integer, Set<Integer>> map;

	private Group group;

	// For satisfying VS6 and VS7
	private Long viewSeqno = null;
	// For satisfying VS6 and VS7
	private long onProcessedNo = 0;

	//private long onDeliveredNo = 0;
	// For satisfying VS6 and VS7
	private Object msgMutualLock = new Byte[0];
	private RegionDistributionManager regionDistributionManager;


	/**
	 * 
	 * @param group
	 *            the corresponding group.
	 */
	public RegionDistributionSynchronyManager(Group group) {

		this.group = group;
		queue = CollectionFacade.getConcurrentQueue();
		map = CollectionFacade.getConcurrentHashMap();
		regionDistributionManager = group.getRegionDistributionManager();
	}

	/**
	 * Adding the nodes that willing to become new sequencer for regions, to the
	 * associating view id.
	 * 
	 * @param viewId
	 *            the id of current view (the crash node id).
	 * @param nodeId
	 *            the completed node id.
	 */
	public void addDecidedSequencerOnViewChange(Integer viewId, Integer nodeId) {
		map.get(viewId).add(nodeId);
	}

	/**
	 * Suspend the current thread according to the three rules in RDS.
	 * 
	 * @param id
	 *            the id of the crash/joining node, this is essentially used as
	 *            view ID.
	 */
	public void suspend(int id) {
		synchronized (mutualLock) {
			if (!isAllowProcess(id)) {
				while (!isAllowProcess(id)) {

					if (logger.isDebugEnabled()) {
						logger.debug(group.getUUID_ADDR()
								+ " Suspending the view for node: " + id);
					}

					try {
						mutualLock.wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}

	/**
	 * Suspend the following process with a new thread, according to the three
	 * rules in RDS.
	 * 
	 * @param id
	 *            the id of the crash/joining node, this is essentially used as
	 *            view ID.
	 * @param callback
	 *            the processes that should carried out on the new thread, when
	 *            suspend ends.
	 * @return true if no suspend needs, false otherwise.
	 */
	public boolean suspendAndRunAsNewThread(int id, Callback callback) {
		synchronized (mutualLock) {
			if (!isAllowProcess(id)) {

				if (isNeedNewThread.get() == null) {

					if (logger.isDebugEnabled()) {
						logger
								.debug(group.getUUID_ADDR()
										+ " Create new thread, then executing protocol on the view and suspending the view for node: "
										+ id);
					}

					new CallbackThread(callback).start();
					return false;
					// If this is the new thread already, then simply suspend
					// until it can be processed
				} else {
					while (!isAllowProcess(id)) {

						if (logger.isDebugEnabled()) {
							logger.debug(group.getUUID_ADDR()
									+ " Suspending the view for node: " + id);
						}

						try {
							mutualLock.wait();
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}

					if (logger.isDebugEnabled()) {
						logger.debug(group.getUUID_ADDR()
								+ " finish suspend for node: " + id);
					}

					return true;
				}
			} else
				return true;
		}

	}

	/**
	 * This should be called within the delivery of view, it queues a view
	 * change of joining node. The recording order should be following the order
	 * indicate in the delivered view, produced by GCS, thus all nodes can see
	 * the same order and the queue for RDS is deterministic and identical.
	 * 
	 * If the recorded id is the current node, then it is set to the 'current'
	 * variable immediately since when the first view is received, we only queue
	 * the nodes after the current node (inclusive), therefore the current node
	 * would be always the first one in the queue, which means it can be setup
	 * immediately.
	 * 
	 * REP ensures that before the current node is added into registry (imply
	 * that the election protocol for current node is completed, which is
	 * impossible if there is still joining nodes before it that is
	 * incompleted), any messages received in REP would be simply dropped.
	 * Therefore even the joining protocol for nodes before current node do not
	 * complete, this node would never perform incorrect behaviors. Correctness
	 * is ensured by VS4 and VS5.
	 * 
	 * @param id
	 *            the id of the crash/joining node, this is essentially used as
	 *            view ID.
	 */
	public void recordJoin(int id) {

		if (logger.isDebugEnabled()) {
			logger.debug(group.getUUID_ADDR()
					+ " Record view change for joining node: " + id);
		}

		synchronized (mutualLock) {

			if (group.getUUID_ADDR() == id) {

				if (logger.isDebugEnabled()) {
					logger.debug(group.getUUID_ADDR()
							+ " Record view change for itself");
				}
				current = new ViewID(id, false);
				return;

			}

			// If this is true means the current joining node crash (since no
			// possible to get leave before join)
			if (current != null && current.getUUID_ADDR() == id)
				return;

			queue.add(new ViewID(id, false));
		}
	}

	/**
	 * This should be called within the delivery of view, it queues a view
	 * change of crashed node. The recording order should be following the order
	 * indicate in the delivered view, produced by GCS, thus all nodes can see
	 * the same order and the queue for RDS is deterministic and identical.
	 * 
	 * @param id
	 *            the id of the crash/joining node, this is essentially used as
	 *            view ID.
	 */
	public void recordLeave(int id) {

		if (logger.isDebugEnabled()) {
			logger.debug(group.getUUID_ADDR()
					+ " Record view change for leaving node: " + id);
		}

		map.put(id, new HashSet<Integer>());

		synchronized (mutualLock) {
			// If this is true means the current joining node crash (since no
			// possible to get leave before join)
			if (current != null && current.getUUID_ADDR() == id)
				return;

			queue.add(new ViewID(id, true));
		}
	}

	/**
	 * This should be called within the delivery of view, it is used only by the
	 * very first member of the group.
	 * 
	 * @param id
	 *            the id of the crash/joining node, this is essentially used as
	 *            view ID.
	 */
	public void recordFirst(int id) {

		if (logger.isDebugEnabled()) {
			logger.debug(group.getUUID_ADDR()
					+ " Record view change for first view");
		}

		synchronized (mutualLock) {
			current = new ViewID(id, false);
		}
	}

	/**
	 * Release the view upon agreement complete, this is mainly used for REP,
	 * since the fault tolerance protocol can be triggered immediately after
	 * this phase, which maxminze the concurrency.
	 * 
	 * @param id
	 *            the id of the crash/joining node, this is essentially used as
	 *            view ID.
	 */
	public void releaseOnAgreement(int id) {
		synchronized (mutualLock) {
			/*
			 * if (current.isSuspect() && current.getUUID_ADDR() == id) {
			 * current = null; mutualLock.notifyAll();
			 * 
			 * if(logger.isDebugEnabled()){ logger.debug("Releasing current view
			 * for leaving node " + id + " upon agreement completion"); } } else
			 */
			if (current != null && !current.isCrashed()
					&& current.getUUID_ADDR() == id) {
				current.setAgreementFinish(true);
				mutualLock.notifyAll();

				if (logger.isDebugEnabled()) {
					logger.debug(group.getUUID_ADDR()
							+ " Releasing current view for joining node " + id
							+ " upon agreement completion");
				}
			}

		}
	}

	/**
	 * Release the view upon retransmission complete, if this is triggered by
	 * REP, then nodeId should by null. Otherwise it is triggered by crash
	 * tolerance, in which case the view is released only if all the new
	 * sequencers (due to the original regions are distributed again) are
	 * elected and tolerated completely. Therefore, it implies to resolve
	 * termination issue when the involved nodes crash.
	 * 
	 * Reset 'current' variable points to null would indicate the protocol
	 * complete.
	 * 
	 * @param viewId
	 *            the id of current view (the crash node id)
	 * @param nodeId
	 *            the completed node id
	 */
	public void releaseOnRetransmission(Integer viewId, Integer nodeId) {

		if (nodeId != null) {
			Set set = map.get(viewId);
			set.remove(nodeId);
			if (set.size() == 0) {

				synchronized (mutualLock) {
					if (current != null && current.getUUID_ADDR() == viewId) {

						if (logger.isDebugEnabled()) {
							logger
									.debug(group.getUUID_ADDR()
											+ " Releasing current view for leaving node "
											+ viewId
											+ " upon retransmission completion, current view Id: "
											+ viewId);
						}

						current = null;
						mutualLock.notifyAll();
					}
				}
			}
		} else {

			synchronized (mutualLock) {

				if (current != null && current.getUUID_ADDR() == viewId) {

					if (logger.isDebugEnabled()) {
						logger
								.debug(group.getUUID_ADDR()
										+ " Releasing current view for joining node "
										+ viewId
										+ " upon retransmission completion, current view Id: "
										+ viewId);
					}
					current = null;
					mutualLock.notifyAll();
				}
			}
		}
	}

	/**
	 * Decide if the processes triggered by view delivery is allowed, in order
	 * to satisfy RDS.
	 * 
	 * @param id
	 *            the id of the crash/joining node, this is essentially used as
	 *            view ID.
	 * @return true if allowed, false otherwise.
	 */
	private boolean isAllowProcess(int id) {

		// Clear
		if (current == null && queue.peek().getUUID_ADDR() == id) {
			current = queue.poll();

			if (logger.isDebugEnabled()) {
				logger
						.debug(group.getUUID_ADDR()
								+ " View changing protocol is allowed to processed for node : "
								+ id
								+ ", because there is not currently runing view changing protocol");
			}

			return true;
		}

		// The joining node crash
		if (current != null && current.getUUID_ADDR() == id) {
			current.setCrashed(true);

			if (logger.isDebugEnabled()) {
				logger
						.debug(group.getUUID_ADDR()
								+ " View changing protocol is allowed to processed for node : "
								+ id
								+ ", because it crash before its joining election complete");
			}
			return true;
		}

		// Concurrent is allowed for leaving
		if (current != null && current.isCrashed() && queue.peek().isCrashed()) {
			current = queue.poll();

			if (logger.isDebugEnabled()) {
				logger
						.debug(group.getUUID_ADDR()
								+ " View changing protocol is allowed to processed for leaving node : "
								+ current.getUUID_ADDR()
								+ ", because FT for crash can be done concurrently");
			}
			return true;

		}

		// For leaving after join, is allowed after agreement
		if (current != null && !current.isCrashed()
				&& current.isAgreementFinish() && queue.peek().isCrashed()) {
			current = queue.poll();

			if (logger.isDebugEnabled()) {
				logger
						.debug(group.getUUID_ADDR()
								+ " View changing protocol is allowed to processed for leaving node : "
								+ current.getUUID_ADDR()
								+ ", because the agreement of previous join election protocol has been completed");
			}
			return true;

		}

		if (logger.isDebugEnabled()) {
			logger.debug(group.getUUID_ADDR() + " View changing for node : "
					+ id + " is suspended");
		}
		return false;

	}

	/**
	 * Actions when 1). the joining node crash before REP complete. 2). the new
	 * sequencer crash before the tolerance protocol complete. 3). normal FT.
	 * 
	 * If the corresponding view id is in the queue, then it should be removed.
	 * 
	 * @param id
	 *            the id of the crash/joining node, this is essentially used as
	 *            view ID.
	 * @return true if it can be triggered for fault tolerance, false otherwise.
	 */
	public boolean releaseOnCrashDuringConsensus(int id) {
		boolean isAddToQueue = true;
		synchronized (mutualLock) {

			// This only used for join-crash
			if (current != null && !current.isCrashed()
					&& current.getUUID_ADDR() == id) {
				current = null;
				mutualLock.notifyAll();
				if (logger.isDebugEnabled()) {
					logger
							.debug(group.getUUID_ADDR()
									+ " The joining view change of node: "
									+ id
									+ " is detected on the current state, and it is released");
				}

				isAddToQueue = false;

			} else {

				List<Integer> viewIds = new LinkedList<Integer>();
				// This is for the join-crash node
				viewIds.add(id);
				for (Map.Entry<Integer, Set<Integer>> entry : map.entrySet()) {

					if (entry.getValue().remove((Integer) id)
							&& entry.getValue().size() == 0) {

						if (current != null && current.isCrashed()
								&& current.getUUID_ADDR() == entry.getKey()) {

							current = null;
							mutualLock.notifyAll();
							if (logger.isDebugEnabled()) {
								logger
										.debug(group.getUUID_ADDR()
												+ " The leaving view change of node: "
												+ entry.getKey()
												+ " is detected on the current state, and it is released");
							}

						} else
							viewIds.add(entry.getKey());
					}
				}

				// See if it is in queue
				for (ViewID ele : queue) {

					if (viewIds.contains(ele.getUUID_ADDR())) {
						queue.remove(ele);
						viewIds.remove((Integer) ele.getUUID_ADDR());

						// This means crash twice, which should not happen
						if (id == ele.getUUID_ADDR())
							isAddToQueue = false;

						if (logger.isDebugEnabled()) {
							logger
									.debug("The view change of node: "
											+ ele.getUUID_ADDR()
											+ " is detected in the queue, and it is released");
						}

					}

				}
			}
		}

		return isAddToQueue;
	}

	/**
	 * The alternative thread when the current one can not be suspended.
	 * 
	 * @author Tao Chen
	 * 
	 */
	private class CallbackThread extends Thread {

		private Callback callback;

		public CallbackThread(Callback callback) {
			super();
			this.callback = callback;
		}

		public void run() {

			isNeedNewThread.set(true);
			callback.run();
			isNeedNewThread.remove();
		}
	}

	/**
	 * Ensure VS6, by an incremental timestamp 'onProcessedNo'.
	 * 
	 * @param viewSeqno
	 *            the seqno of view of the receiving message.
	 * @return true if with in the right view, false otherwise.
	 */
	public boolean cacheMsgOnView(Integer srcId, Message msg, Long viewSeqno) {
		// Only work for unicast

		synchronized (msgMutualLock) {

			//System.out.print(msg + " \n");
			if (logger.isDebugEnabled()) {
				logger.debug("If discard the message: "
						+ (viewSeqno != this.viewSeqno) + " incoming view: "
						+ viewSeqno + ", current view: " + this.viewSeqno);
			}

			if (msg.isNeedUnicastConsistency()
					&& !regionDistributionManager.hasNode(srcId)
					&& viewSeqno != this.viewSeqno) {
				if (logger.isErrorEnabled()) {
					logger
							.error("Discard the message, this view "
									+ this.viewSeqno + ", the given view: "
									+ viewSeqno);
				}

				System.out.print(msg + " discard \n");
				return false;
			}
			onProcessedNo++;
		}

		return true;
	}

	/**
	 * Release the 'onProcessedNo' upon the processes triggered by message
	 * delivery complete, this ensure serilizability (VS7).
	 */
	public void releaseMsgOnView(Message msg) {
		synchronized (msgMutualLock) {
			//System.out.print(msg + " release\n");
			if (onProcessedNo > 0) {
				onProcessedNo--;
				if (logger.isDebugEnabled()) {
					logger.debug("Remaining message number: " + onProcessedNo);
				}
				msgMutualLock.notifyAll();
			}
		}
	}

	/**
	 * Ensure VS6 and VS7 of the view delivery, which means the current thread
	 * may be suspended for ensuring serilizability property.
	 */
	public void isViewDeliverable() {
		synchronized (msgMutualLock) {

			while (onProcessedNo != 0) {
				System.out.print("no: " + onProcessedNo + "\n");
				if (logger.isDebugEnabled()) {
					logger.debug("Suspend view delivery, current view no: "
							+ viewSeqno);
				}
				try {
					msgMutualLock.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

			if (logger.isDebugEnabled()) {
				logger.debug("Enable view delivery, current view no: "
						+ viewSeqno);
			}
			viewSeqno++;
		}
	}

	/*
	public void tryViewDelivery() {
		synchronized (viewMutualLock) {
			onDeliveredNo++;
		}
		
	}*/

	/**
	 * This class represent the node that change (join or leave) within a
	 * delivered view
	 * 
	 * @author Tao Chen
	 * 
	 */
	private class ViewID {

		private int UUID_ADDR;

		private boolean isCrashed;

		private boolean isAgreementFinish = false;

		public void setCrashed(boolean isSuspect) {
			this.isCrashed = isSuspect;
		}

		public ViewID(int uuid_addr, boolean isSuspect) {
			super();
			UUID_ADDR = uuid_addr;
			this.isCrashed = isSuspect;
		}

		public int getUUID_ADDR() {
			return UUID_ADDR;
		}

		public boolean isCrashed() {
			return isCrashed;
		}

		public boolean isAgreementFinish() {
			return isAgreementFinish;
		}

		public void setAgreementFinish(boolean isAgreementFinish) {
			this.isAgreementFinish = isAgreementFinish;
		}
	}

	/**
	 * Get lock for VS6 and VS7.
	 * 
	 * @return the lock
	 */
	public Object getmsgMutualLock() {
		return msgMutualLock;
	}

	/**
	 * Get current seqno of view.
	 * 
	 * @return seqno of view.
	 */
	public Long getViewSeqno() {
		return viewSeqno;
	}

	/**
	 * Set the initial seqno.
	 * 
	 * @param viewSeqno
	 *            seqno of the view.
	 */
	public void setViewSeqno(Long viewSeqno) {
		if (logger.isDebugEnabled()) {
			logger.debug("Obtain view seqno: " + viewSeqno);
		}
		this.viewSeqno = viewSeqno;

	}
	/*
	public boolean isMsgDeliverable() {
		// delivery of view also ensure serliazability (partial)
		if (onDeliveredNo != 0) {
			return false;
		}
		
		return true;
	}

	
	public void suspendMsg(Message msg){
		if (onDeliveredNo != 0) {
			synchronized (viewMutualLock) {

				while (onDeliveredNo != 0) {
					System.out.print(msg + "view no: " + onDeliveredNo + "\n");
					if (logger.isDebugEnabled()) {
						logger.debug("Suspend msg delivery");
					}
					try {
						viewMutualLock.wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}

			}
		}
	}
	
	public void releaseView() {
		synchronized (viewMutualLock) {
			if (onDeliveredNo > 0) {
				onDeliveredNo--;
				viewMutualLock.notifyAll();
			}
		}
	}
	*/



}
