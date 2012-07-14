package org.ssor;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ssor.exception.ConfigurationException;

/**
 * Atomic Service (AS) is stand-alone service that does not interact with other
 * service or the interactions can be hidden, in principle, arbitrary service
 * can be AS. They are seen as meta-replicable service and can involve
 * deterministic or non-deterministic actions. AS also can be those composite
 * services whose nested invocation of services can be hidden, that is, the
 * existence of nested services of a composite service does not known by the
 * invocation logic, thus full service redundancy can be achieved. AS can be
 * either statefull or stateless.
 * 
 * @author Tao Chen
 * 
 */
public class AtomicService {

	protected static final Logger logger = LoggerFactory
			.getLogger(AtomicService.class);
	// This is normally the name of class + '.' + name of method
	protected String name;
	protected Region region;
	protected AtomicService[] redundantServices;
	protected Method interfaceMethod;
	// By default this is the same as service name
	protected String proxyAlias;

	protected Map<AtomicService, Consistency> interferedServices;
	protected Set<AtomicService> dependentServices; 

	// protected boolean isConcurrentDeliverable = false;

	protected boolean isFIFO = false;
	// This can avoid execution of this service on replica site
	// if this is true, useful when this is read-only process and
	// byzantine failure is not need to be tolerated. (mainly for ROWA model)
	protected boolean isReplicateOnReplica = true;

	// The magic number of service, avoid sending service name
	protected int magicNumber;

	public AtomicService(String name, Region activeRegion,
			Class<?>[] parameterTypes) {
		super();
		this.name = name + getNameTrailer(parameterTypes);
		this.region = activeRegion;
		proxyAlias = name.substring(0, name.lastIndexOf("."));

		String methodName = name.replace(proxyAlias + ".", "");
		Class<?> clazz = null;
		try {
			clazz = Class.forName(proxyAlias);
			for (Class<?> interf : clazz.getInterfaces()) {
				try {
					interfaceMethod = interf.getDeclaredMethod(methodName,
							parameterTypes);
					break;
				} catch (Throwable t) {
					continue;
				}
			}

			if (interfaceMethod == null) {
				interfaceMethod = clazz.getDeclaredMethod(methodName,
						parameterTypes);
			}

		} catch (Throwable e) {
			throw new ConfigurationException(e);
		}
	}

	public String getProxyAlias() {
		return proxyAlias;
	}

	public void setProxyAlias(String proxyAlias) {
		this.proxyAlias = proxyAlias;
	}

	public AtomicService(String name, Region activeRegion,
			AtomicService[] redundantServices, Class<?>[] parameterTypes) {
		super();
		this.name = name + getNameTrailer(parameterTypes);
		this.region = activeRegion;
		this.redundantServices = redundantServices;
		proxyAlias = name.substring(0, name.lastIndexOf("."));

		String methodName = name.replace(proxyAlias + ".", "");
		Class<?> clazz = null;
		try {
			clazz = Class.forName(proxyAlias);
			for (Class<?> interf : clazz.getInterfaces()) {
				try {
					interfaceMethod = interf.getDeclaredMethod(methodName,
							parameterTypes);
					break;
				} catch (Throwable t) {
					continue;
				}
			}

			if (interfaceMethod == null) {
				interfaceMethod = clazz.getDeclaredMethod(methodName,
						parameterTypes);
			}

		} catch (Throwable e) {
			throw new ConfigurationException(e);
		}
	}

	public AtomicService(String name, Class<?>[] parameterTypes) {
		super();
		this.name = name + getNameTrailer(parameterTypes);
		proxyAlias = name.substring(0, name.lastIndexOf("."));

		String methodName = name.replace(proxyAlias + ".", "");
		Class<?> clazz = null;
		try {
			clazz = Class.forName(proxyAlias);
			for (Class<?> interf : clazz.getInterfaces()) {
				try {
					interfaceMethod = interf.getDeclaredMethod(methodName,
							parameterTypes);
					break;
				} catch (Throwable t) {
					continue;
				}
			}

			if (interfaceMethod == null) {
				interfaceMethod = clazz.getDeclaredMethod(methodName,
						parameterTypes);
			}

		} catch (Throwable e) {
			throw new ConfigurationException(e);
		}

		if (logger.isDebugEnabled())
			logger.debug("Atomic service: " + this.name + " created");
	}

	/**
	 * This is only used by sub classes.
	 * 
	 * @param name
	 */
	protected AtomicService(Class<?>[] parameterTypes, String name) {
		super();
		this.name = name + getNameTrailer(parameterTypes);
		proxyAlias = name.substring(0, name.lastIndexOf("."));
	}

	protected String getNameTrailer(Class<?>[] parameterTypes) {
		if (parameterTypes == null)
			return "";

		String key = "";
		for (Class<?> clazz : parameterTypes)
			key += "," + clazz.getName();

		return key;
	}

	public int isBlocking(String sessionId, Integer order, Integer concurrent) {
		return region.isBlocking(sessionId, order, concurrent);
	}

	/**
	 * Examine if a message is deliverable, the same as if the service is
	 * executable. This implement the delivery conditions
	 * 
	 * @param sessionId
	 *            session ID that a message carries
	 * @param sequence
	 *            the corresponding sequence
	 * @return 0 indicate deliverable, -1 indicates concurrent deliverable,
	 *         otherwise 1
	 */
	public int isExecutable(String sessionId, Sequence sequence) {
		return region.isExecutable(sessionId, sequence, this);
	}

	/**
	 * Increase the seqno by 1.
	 * 
	 * @param sessionId
	 *            the sessiond ID
	 */
	public void increaseSeqno(String sessionId) {
		region.increaseSeqno(sessionId);
	}

	/**
	 * Increase the concurrentno by 1.
	 * 
	 * @param sessionId
	 *            the sessiond ID
	 */
	public void increaseConcurrentno(String sessionId) {
		region.increaseConcurrentno(sessionId);
	}

	/**
	 * This is used when nested services of a CS request sequence after
	 * tolerance of sequencer crashes.
	 * 
	 * Acquire the next sequence, either as (i, null), (i, j) for sequential
	 * sequence or (i, -1) for concurrent sequence, this essentially implement
	 * the assignment conditions.
	 * 
	 * @param sessionId
	 *            the session ID
	 * @param indices
	 *            the indices of nested services which require sequence
	 * @param UUID_ADDR
	 *            the UUID of current node
	 * @return
	 */
	public Sequence getNextSeqno(String sessionId, Integer[] indices,
			int UUID_ADDR) {
		return null;
	}

	/**
	 * Acquire the next sequence, either as (i, null), (i, j) for sequential
	 * sequence or (i, -1) for concurrent sequence, this essentially implement
	 * the assignment conditions.
	 * 
	 * @param sessionId
	 *            the session ID
	 * @param UUID_ADDR
	 *            the UUID of current node
	 * @return the corresponding sequence or null if this is not the sequencer
	 */
	public Sequence getNextSeqno(String sessionId, int UUID_ADDR) {
		return region.getNextSeqno(sessionId, this, UUID_ADDR);
	}

	/**
	 * True would trigger FIFO, if it is adjustable
	 * 
	 * @return
	 */
	public boolean isFIFOEnabled() {
		return isFIFO;
	}

	/**
	 * Validate if the given UUID of node is the sequencer of the region that
	 * this service associates with.
	 * 
	 * @param UUID_ADDR
	 *            the UUID
	 * @return if yes then return true, false otherwise
	 */
	public boolean isSequencer(int UUID_ADDR) {
		// For all session level region, sequencer is the same
		return region.isSequencer(UUID_ADDR);
	}

	public boolean isRequireBlocking() {
		return region.isRequireBlocking();
	}

	/**
	 * If the service contains redundant service.
	 * 
	 * @return if yes then return true, false otherwise
	 */
	public boolean isRedundant() {
		return redundantServices != null;
	}

	/**
	 * If the service's region is SCR.
	 * 
	 * @return if yes then return true, false otherwise
	 */
	public boolean isSessional() {
		return region.scope == Region.SESSIONAL_CONFLICT_REGION;
	}

	/**
	 * This may suspend the thread in case the region is under region
	 * distribution.
	 * 
	 * @return if allow then return the sequencer UUID, otherwise suspend
	 */
	public Integer getSequencerWhenRequest() {
		return region.getSequencerWhenRequest();
	}

	/**
	 * This is used for invocation interception, for validating if a given
	 * service is the correct redundant service in a given index. So that the
	 * replication logic of RS can be triggered.
	 * 
	 * @param service
	 *            the given service
	 * @param index
	 *            the given index
	 * @return if yes then return true, false otherwise
	 */
	public boolean isRedundantServiceRequired(AtomicService service, int index) {
		if (redundantServices != null) {

			if (index >= redundantServices.length)
				return false;
			// Only judge memory address
			if (redundantServices[index] == service)
				return true;
		}

		return false;
	}

	/**
	 * Count the number of included RS
	 * 
	 * @return the count
	 */
	public int getRedundantServiceCount() {
		return (redundantServices == null) ? 0 : redundantServices.length;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (!(o instanceof AtomicService))
			return false;

		AtomicService that = (AtomicService) o;

		if (!name.equals(that.name))
			return false;

		return true;
	}

	public String getName() {
		return name;
	}

	public Region getRegion() {
		return region;
	}

	/*
	 * Compare @param that @return
	 * 
	 * public int compareConsistencyLevel(AtomicService that) {
	 * 
	 * if (this instanceof CompositeService) return 1; else if (that instanceof
	 * RedundantService) return 1; else if (that.region.getScope() >
	 * region.getScope()) return 1; else if (that.region.getScope() <
	 * region.getScope()) return -1; else return 0; }
	 */

	/**
	 * Obtain the mutualLock of the associated region.
	 * 
	 * @param sessionId
	 *            the session ID
	 * @return the lock
	 */
	public Object getMutualLock(String sessionId) {
		return region.getMutualLock(sessionId);
	}

	/**
	 * Call by replica site only.
	 * 
	 * @return the interface method of this service
	 */
	public Method getInterfaceMethod() {
		return interfaceMethod;
	}

	/**
	 * Call by replica site only.
	 * 
	 * @param instance
	 *            the service instance
	 * @param arguments
	 *            the args
	 * @return the return results of this service
	 * @throws Exception
	 */
	public Object invoke(Object instance, Object[] arguments) throws Exception {
		return interfaceMethod.invoke(instance, arguments);
	}

	/**
	 * Record the service that concurrent deliverable with this service <ASthis,
	 * ASi> belongs to CDS (i belongs to N).
	 * 
	 * @param service
	 *            the target service.
	 */
	public void addInterferedService(AtomicService service) {
	}
	
	public void addInterferedService(AtomicService service, int tolerance) {
		if (interferedServices == null)
			interferedServices = new HashMap<AtomicService, Consistency>();
		interferedServices.put(service, new Consistency(tolerance));
	}
	
	public void addDependantService(AtomicService service) {
		if (dependentServices == null) {
			dependentServices = new HashSet<AtomicService>();
		}
		dependentServices.add(service);
	}

	/**
	 * Decide if a given service is concurrent deliverable with this service.
	 * 
	 * @param lastServiceName
	 *            the name of given service
	 * @return true if yes, false otherwise.
	 */
	public boolean isConcurrentDeliverable(String lastServiceName) {
		boolean cd = true;
		if (interferedServices == null && dependentServices == null) {
			return cd;
		}
		
		if (interferedServices != null) {
			// Update interfered services, determine if order needed
			Iterator<Request> itr = null;
			Request request = null;
			for (Map.Entry<AtomicService, Consistency> e : interferedServices.entrySet()) {
				itr = e.getValue().requests.iterator();
				while(itr.hasNext()){
					request = itr.next();
					if (request.count == e.getValue().tolerance){
						cd = false;
						itr.remove();
						break;
					} else {
						request.increase();
					}
				}			
			}
		}
		if (dependentServices != null) {
			  // Update services which interfered by this
			  for (AtomicService service : dependentServices) {
				  service.updateDependentService(this);
			  }
		}
		return cd;
	}
	
	public void updateDependentService(AtomicService service) {
		for (Map.Entry<AtomicService, Consistency> e : interferedServices.entrySet()) {
			if (e.getKey().name.equals(service.name)) {
				e.getValue().addRequest();
				break;
			}
		}
	}
	
	public void updateTolerance(AtomicService service, int tolerance) {
		for (Map.Entry<AtomicService, Consistency> e : interferedServices.entrySet()) {
			if (e.getKey().name.equals(service.name)) {
				e.getValue().setTolerance(tolerance);
				break;
			}
		}
	}

	public void setFIFO(boolean isFIFO) {
		this.isFIFO = isFIFO;
	}

	public int getMagicNumber() {
		return magicNumber;
	}

	public Integer getRegionNumber() {
		return region == null ? null : region.region;
	}

	public void setMagicNumber(int magicNumber) {
		this.magicNumber = magicNumber;
	}

	public boolean isReplicateOnReplica() {
		return isReplicateOnReplica;
	}

	public void setReplicateOnReplica(boolean isReadOnly) {
		this.isReplicateOnReplica = isReadOnly;
	}

	public void setRegion(Region region) {
		if (this.region == null)
			this.region = region;
	}

	public void setRedundantServices(AtomicService[] redundantServices) {
		this.redundantServices = redundantServices;
	}

	public String toString() {
		return "(Service: " + name + ", with proxy alias: " + proxyAlias + ", region: " + region + ")";
	}

	private class Consistency {
		private List<Request> requests;
		// 0 means order one by one
		private int tolerance;
		public Consistency(int tolerance){
			requests = new LinkedList<Request>();
			this.tolerance = tolerance;
		}
		
		public void addRequest(){
			requests.add(new Request());
		}
		
		public void setTolerance(int tolerance) {
			this.tolerance = tolerance;
		}
	}
	
	private class Request {
		private int count = 0;
		public void increase(){
			count++;
		}
	}
}
