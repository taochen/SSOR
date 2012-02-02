package org.ssor;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

@SuppressWarnings("unchecked")
public class ServiceManager {

	private Map<String, AtomicService> registry;
	// Mapping between number and service name
	private static Map<Integer, String> magicNumber;
	private Map<Integer, Region> regionRegistry;
	private static Map<String, AtomicService> globalRegistry;
	// The increased number used to generate magic number for service
	private static Integer number = 0;

	
	
	static {
		globalRegistry = CollectionFacade.getConcurrentHashMap(50);
		magicNumber = CollectionFacade.getConcurrentHashMap(50);
	}

	public ServiceManager() {

		regionRegistry = CollectionFacade.getConcurrentHashMap(50);
		registry = CollectionFacade.getConcurrentHashMap(50);
		// Initailize session level region
		regionRegistry.put(SessionRegion.SESSION_REGION, new SessionRegion());

	}

	public void register(AtomicService value) {

		// This may be duplicate if run multi-configurator
		if (!registry.containsKey(value.getName())) {

			synchronized (globalRegistry) {
				if (!globalRegistry.containsKey(value.getName())) {
					magicNumber.put(number, value.getName());
					value.setMagicNumber(number);
					number++;

					globalRegistry.put(value.getName(), value);
				}

			}

			registry.put(value.getName(), value);
		}
		
		if (value.getRegionNumber() != null && value.isRequireBlocking()
				&& !regionRegistry.containsKey(value.getRegionNumber())) {
			//System.out.print(value.getRegionNumber() + "\n");
			regionRegistry
					.put(value.getRegionNumber(), value.getRegion());

		}
	}

	public AtomicService get(String key) {
		return registry.get(key);
	}

	public Region getRegion(int key) {
		return regionRegistry.get(key);
	}
	
	public SessionRegion getSessionRegion() {
		return (SessionRegion) regionRegistry.get(SessionRegion.SESSION_REGION);
	}

	public Set<Map.Entry<Integer, Region>> getAllRegions() {
		return regionRegistry.entrySet();
	}
	
	public  boolean isSessional(String name) {
		// This is always not null
		return registry.get(name).isSessional();
	}

	public static int getMagicNumber(String name) {
		// This is always not null
		return globalRegistry.get(name).getMagicNumber();
	}

	public static String getServiceName(Integer number) {
		// This is always not null
		return magicNumber.get(number);
	}
	
	public State.StateList getRegionStates(){
		
		final Set<Map.Entry<Integer, Region>> set = regionRegistry.entrySet();
		final List<State> list = new LinkedList<State>();
		for(Map.Entry<Integer, Region> entry : set){
			
			if(entry.getValue() instanceof SessionRegion)
				((SessionRegion)entry.getValue()).extractExpectedSequence(list);
			else
			  list.add(new State(entry.getKey(), entry.getValue().getExpectedSequence()));
			
		}
		
		return new State.StateList(list);
		
	}
	
	
	
}
