package org.ssor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.ssor.protocol.replication.abcast.SequenceVector;

/**
 * The major idea for Composite Service (CS) is to achieve partial service
 * redundancy, and it can be stateful or stateless services whose content needs
 * to be known. CS consists of 2 forms: local composition and remote
 * composition. The former one mean combine different AS that provided by
 * existing system while the later one is invoke services provided by other
 * providers, which is the form of service composition that commonly occurring
 * in SOA. CS itself is non-replicable, the only replicable part is the atomic
 * services that are contained, a CS can involve another CS as well. Therefore
 * the level of redundancy can be controlled (achieve partial redundancy), in
 * addition, the RNI problem can be avoided for the local composition. Remote
 * composition is treated as redundant service, since there is no way to know if
 * the invoked service is deterministic or idempotent.
 * 
 * Nested service composition arise the cyclic reference problem, that is, a CS
 * may be contained in another CS that it contains. However, this problem is
 * caused by improper specification and design of service, which happen on
 * typical centralized context as well, therefore it is not a problem in the
 * context of replication.
 * 
 * The CS needs to fulfil lazy replication, because the arguments for nested
 * service are unknown until they are executed. This does not affect the
 * replication logic for centralized replication model (primary backup),
 * however, as for active replication/quorum model, broadcast can only be
 * triggered upon completion of service on the current RM who receives the
 * request from client. This may downgrade the performance since it delay the
 * broadcast, however, because of the reduction of redundancy, the computation
 * effort of the system can be reduced. CS can also become AS, if full service
 * redundancy is desirable.
 * 
 * Note that if it contains another CS, that the contained one would be
 * decomposed as it actually contain numbers of ASs
 * 
 * @author Tao Chen
 * 
 */
public class CompositeService extends AtomicService {

	// The workflow of nested service must be match as its program order
	// This should be either atomic service or redundant service
	private AtomicService[] services;
	private AtomicService[] blockingServices;
	private int nonOrderedServiceCount;
	private boolean isFIFO = false;
	private boolean isNeedSessionId = false;
	// The regions that associated by any nested service
	private Set<Region> involedRegions = new HashSet<Region>();

	public CompositeService(String name, AtomicService[] services) {
		super(null, name);
		System.out.print(services.length + "\n");
		this.services = services;

		countNonOrderedService();

	}

	public CompositeService(Class<?>[] parameterTypes, String name) {
		super(parameterTypes, name);
	}

	/**
	 * Invoked by invocation service, determine if a given service is a nested
	 * service within this composite service.
	 * 
	 * @param service
	 *            the given atomic service
	 * @param index
	 *            the index of such service
	 * @return true if yes, false otherwise
	 */
	public boolean isNestedReplicationRequired(AtomicService service, int index) {
		if (index < services.length) {

			// Only judge memory address
			if (services[index] == service)
				return true;
		}

		return false;
	}

	/*
	 * public boolean isExecutable(int index, int order){ return
	 * services[index].activeRegion.isExecutable(order); }
	 * 
	 * public void increase(int index){
	 * 
	 * if(services[index].pasiveRegions != null) for(Region region :
	 * services[index].pasiveRegions) region.increase();
	 * 
	 * services[index].activeRegion.increase(); }
	 */

	/*
	 * (non-Javadoc)
	 * @see org.ssor.AtomicService#isExecutable(java.lang.String, org.ssor.Sequence)
	 */
	public int isExecutable(String sessionId, Sequence sequence) {
		return 0;
	}

	/*
	 * (non-Javadoc)
	 * @see org.ssor.AtomicService#increaseSeqno(java.lang.String)
	 */
	public void increaseSeqno(String sessionId) {
	}

	/*
	 * (non-Javadoc)
	 * @see org.ssor.AtomicService#increaseConcurrentno(java.lang.String)
	 */
	public void increaseConcurrentno(String sessionId) {

	}

	/**
	 * (non-Javadoc)
	 * @see org.ssor.AtomicService#getNextSeqno(java.lang.String, java.lang.Integer[], int)
	 */
	public Sequence getNextSeqno(String sessionId, Integer[] indices,
			int UUID_ADDR) {

		List<SequenceVector> sequences = new LinkedList<SequenceVector>();
		Sequence sequence = null;
		for (int i = 0; i < indices.length; i++) {
			// This is need thus the judgement of wether this is sequencer can
			// be put into
			// the sync block
			// synchronized (services[indics[i]].activeRegion){
			if ((sequence = services[indices[i]].region.getNextSeqno(sessionId,
					services[indices[i]], UUID_ADDR)) != null)
				sequences.add(new SequenceVector(indices[i], sequence));
			// }
		}

		return new Sequence(sequences.toArray(new SequenceVector[sequences
				.size()]));
	}

	/**
	 * (non-Javadoc)
	 * 
	 * @see org.ssor.AtomicService#getNextSeqno(java.lang.String, int)
	 */
	public Sequence getNextSeqno(String sessionId, int UUID_ADDR) {

		List<SequenceVector> sequences = new LinkedList<SequenceVector>();
		Sequence sequence = null;
		for (int index = 0; index < services.length; index++) {
			// This is need thus the judgement of wehther this is sequencer can
			// be put into
			// the sync block
			// synchronized (services[index].activeRegion){
			// if(services[index].isSequencer())
			if ((sequence = services[index].region.getNextSeqno(sessionId,
					services[index], UUID_ADDR)) != null)
				sequences.add(new SequenceVector(index, sequence));

		}
		// }

		return new Sequence(sequences.toArray(new SequenceVector[sequences
				.size()]));
	}

	public int getNestedServiceLength() {
		return services.length;
	}

	public AtomicService[] getServices() {
		return services;
	}

	public int getNonOrderedServiceCount() {
		return nonOrderedServiceCount;
	}

	public boolean isSequencer(int id) {
		return false;
	}

	public Object getMutualLock(String sessionId) {
		return new Object();
	}

	public boolean isRequireBlocking() {
		return services.length != nonOrderedServiceCount;
	}

	public boolean isRequireBlocking(int index) {
		return services[index].isRequireBlocking();
	}

	public boolean isFIFOEnabled() {
		return isFIFO;
	}

	public boolean isNestedOrderedService(AtomicService service, int index) {

		// If null means no order needed
		if (blockingServices == null || index >= blockingServices.length)
			return false;
		if (logger.isDebugEnabled()) {
			logger.debug("Blocking nested service index: " + index
					+ ", is needed ordered "
					+ (blockingServices[index] == service));
		}
		return blockingServices[index] == service;
	}

	private void countNonOrderedService() {

		final List<AtomicService> list = new ArrayList<AtomicService>();
		final List<AtomicService> idempotentList = new ArrayList<AtomicService>();
		for (AtomicService service : services) {

			if (service.redundantServices != null)
				idempotentList.addAll(Arrays.asList(service.redundantServices));

			if (service instanceof RedundantService)
				idempotentList.add(service);

			if (!isFIFO && service.isFIFOEnabled())
				isFIFO = true;

			if (!service.isRequireBlocking())
				nonOrderedServiceCount++;
			else {

				if (service.isSessional())
					isNeedSessionId = true;

				list.add(service);
			}

			involedRegions.add(service.region);

		}

		blockingServices = list.size() == 0 ? null : list
				.toArray(new AtomicService[list.size()]);
		redundantServices = idempotentList.size() == 0 ? null : idempotentList
				.toArray(new AtomicService[idempotentList.size()]);

		if (logger.isDebugEnabled()) {
			logger.debug("Composite service: " + name
					+ ", non-ordered nested service: " + nonOrderedServiceCount
					+ ", ordered nested service: " + list.size()
					+ ", is FIFO enabled: " + isFIFO);
		}
	}

	public Integer getSequencerWhenRequest() {
		return null;
	}

	public boolean isSessional() {
		return isNeedSessionId;
	}

	public Set<Region> getInvoledRegions() {
		return involedRegions;
	}

	public void setServices(AtomicService[] services) {
		if (this.services == null) {
			this.services = services;
			countNonOrderedService();
		}
	}
}
