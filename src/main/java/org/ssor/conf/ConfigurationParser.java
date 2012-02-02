package org.ssor.conf;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.ssor.AtomicService;
import org.ssor.CollectionFacade;
import org.ssor.CompositeService;
import org.ssor.RedundantService;
import org.ssor.Region;
import org.ssor.RegionDistributionManager;
import org.ssor.ServiceManager;
import org.ssor.exception.ConfigurationException;
import org.ssor.util.Environment;
import org.ssor.util.Group;
import org.ssor.util.Util;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

@SuppressWarnings("unchecked")
public class ConfigurationParser {

	private static final Map<String, Class<?>> primitiveMap = CollectionFacade
			.getConcurrentHashMap(); // key=magic number, value=Class

	public static final String CONFIGURATION_FILE = "ssor.xml";
	
	// Delegate of corresponding adaptor
	private static AdaptorDelegate adaptor;

	private final static Region nonConflictRegion = new Region(0,
			Region.NON_CONFLICT_REGION);

	static {

		primitiveMap.put("int", Integer.TYPE);
		primitiveMap.put("short", Short.TYPE);
		primitiveMap.put("long", Long.TYPE);
		primitiveMap.put("byte", Byte.TYPE);
		primitiveMap.put("boolean", Boolean.TYPE);
		primitiveMap.put("float", Float.TYPE);
		primitiveMap.put("double", Double.TYPE);
		primitiveMap.put("char", Character.TYPE);
		Properties pro = System.getProperties();
		pro.setProperty("java.net.preferIPv4Stack", "true");

	}

	protected static void init() throws Exception {

		// Read mapping file
		String prefix = ConfigurationParser.class.getProtectionDomain()
				.getCodeSource().getLocation().getFile();
	
		if (prefix.endsWith("ConfigurationParser.class"))
			prefix = prefix.replace("org/ssor/conf/ConfigurationParser.class", "");

		read(prefix + CONFIGURATION_FILE);
	}

	/**
	 * try to read the magic number configuration file as a Resource form the
	 * classpath using getResourceAsStream if this fails this method tries to
	 * read the configuration file from mMagicNumberFile using a FileInputStream
	 * (not in classpath but somewhere else in the disk)
	 * 
	 * @return an array of ClassMap objects that where parsed from the file (if
	 *         found) or an empty array if file not found or had en exception
	 */
	protected static void read(String name) throws Exception {
		InputStream stream;
		try {
			stream = Util.getResourceAsStream(name, ConfigurationParser.class);
			// try to load the map from file even if it is not a Resource in the
			// class path
			if (stream == null) {
				// if(logger.isTraceEnabled())
				// logger.trace("Could not read " + name + " from the CLASSPATH,
				// will try to read it from file");
				stream = new FileInputStream(name);
			}
		} catch (Exception x) {
			throw new ConfigurationException(name
					+ " not found. Please make sure it is on the classpath", x);
		}
		parse(stream);
	}

	protected static void parse(InputStream stream) throws Exception {
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		factory.setValidating(false); // for now
		DocumentBuilder builder = factory.newDocumentBuilder();
		Document document = builder.parse(stream);
		NodeList nodeList = document.getElementsByTagName("group");
		for (int i = 0; i < nodeList.getLength(); i++) {
			if (nodeList.item(i).getNodeType() == Node.ELEMENT_NODE)
				parseGroup(nodeList.item(i));
		}

	}

	protected static void parseGroup(Node node) throws Exception {
		final NodeList nodeList = node.getChildNodes();
		// Record all service, including as, cs and rs 
		final Map<String, AtomicService> services = new HashMap<String, AtomicService>();
		// Record the pair of concurrent deliverable service
		final Map<String, List<String>> concurrentServices = new HashMap<String, List<String>>();
		// Record CS and its nested services
		final Map<String, List<String>> compositeServices = new HashMap<String, List<String>>();
		NodeList sessionalServiceList = null;

		List<String> list = null;
		// avoid duplicated declaration of region identifier
		final List<Integer> regionNumbers = new LinkedList<Integer>();
		for (int i = 0; i < nodeList.getLength(); i++) {
			if (nodeList.item(i).getNodeType() == Node.ELEMENT_NODE) {
				if (nodeList.item(i).getNodeName().equals("redundantService")) {
					services.put(nodeList.item(i).getAttributes().getNamedItem(
							"name").getNodeValue(),
							parseRedundantService(nodeList.item(i)));
				} else if (nodeList.item(i).getNodeName().equals(
						"atomicService")) {
					list = parseAtomicService(nodeList.item(i), services);
					if (list != null)
						concurrentServices.put(nodeList.item(i).getAttributes()
								.getNamedItem("name").getNodeValue(), list);
				} else if (nodeList.item(i).getNodeName().equals(
						"compositeService")) {
					compositeServices.put(nodeList.item(i).getAttributes()
							.getNamedItem("name").getNodeValue(),
							parseCompositeService(nodeList.item(i), services));
				} else if (nodeList.item(i).getNodeName().equals("region")) {
					if (nodeList.item(i).getAttributes().getNamedItem("scope")
							.getNodeValue().equals("sessional-conflict-region"))
						sessionalServiceList = node.getChildNodes();
					else {
						int id = parseRegion(nodeList.item(i), services);
						if(regionNumbers.contains(id))
							throw new ConfigurationException(
									"duplicated declaration of region identifier: " + id);
						regionNumbers.add(id);	
					}
				}
			}

		}
	
		// Initiailize the group
		final Group group = adaptor.getGroup(node.getAttributes().getNamedItem(
				"name").getNodeValue());
		final RegionDistributionManager regionDistributionManager = group
				.getRegionDistributionManager();

		final ServiceManager serviceManager = group.getServiceManager();

		Environment.setGroup(group);
		/*
		 * Setup interests
		 */
		if (node.getAttributes().getNamedItem("interests") != null) {
			String[] interests = node.getAttributes().getNamedItem("interests")
					.getNodeValue().split(",");

			for (String regionNumber : interests)
				regionDistributionManager.setInterests(Integer
						.parseInt(regionNumber));
		}

		/*
		 * Setup concurrent deliverable services
		 */
		Set<Map.Entry<String, List<String>>> set = concurrentServices
				.entrySet();
		AtomicService atomicService = null;
		for (Map.Entry<String, List<String>> entry : set) {
			atomicService = services.get(entry.getKey());

			
			for (String name : entry.getValue()) {

				atomicService.addConcurrentDeliverableService(acquireService(services, name, entry.getKey()));
			}
		}

		/*
		 * Setup composite services
		 */

		// Including redundant services
		set = compositeServices.entrySet();
		for (Map.Entry<String, List<String>> entry : set) {

			recusiveBuildNestedService(new LinkedList<String>(),
					entry.getKey(), entry.getValue(), compositeServices,
					services);
		}
		/*
		 * Setup sessional conflict region's services
		 */
		if (sessionalServiceList != null) {
			String key = null;
			for (int i = 0; i < sessionalServiceList.getLength(); i++) {
				key = sessionalServiceList.item(i).getAttributes()
						.getNamedItem("name").getNodeValue();
				atomicService = acquireService(services, key, "sessional conflict region");
			
				if (atomicService instanceof CompositeService
						|| atomicService instanceof RedundantService)
					throw new ConfigurationException(
							"Service: "
									+ key
									+ " can not be associated  sessional conflict region, only atomic service is allowed");

				atomicService.setRegion(serviceManager.getSessionRegion());
			}
		}
		
		// Register all services
		Set<Map.Entry<String, AtomicService>> servicesSet = services.entrySet();
		for(Map.Entry<String, AtomicService> entry : servicesSet)
			serviceManager.register(entry.getValue());
		
	
		adaptor.init();
		
	}

	protected static int parseRegion(Node node,
			Map<String, AtomicService> services) {

		int id = 0;
		final NodeList nodeList = node.getChildNodes();
		final Region region = (node.getAttributes().getNamedItem("scope")
				.getNodeValue().equals("non-conflict-region")) ? nonConflictRegion
				: new Region(id = Integer.parseInt(node.getAttributes()
						.getNamedItem("id").getNodeValue()),
						Region.CONFLICT_REGION);
		AtomicService service = null;
		String key = null;
		for (int j = 0; j < nodeList.getLength(); j++) {

			if (nodeList.item(j).getNodeType() == Node.ELEMENT_NODE) {
				key = nodeList.item(j).getAttributes().getNamedItem("name")
						.getNodeValue();

				service = acquireService(services, key, " region: " + region.getRegion());
			
				if (service instanceof CompositeService
						|| service instanceof RedundantService)
					throw new ConfigurationException("Service: " + key
							+ " can not be associated with region: "
							+ region.getRegion()
							+ ", only atomic service is allowed");

				service.setRegion(region);
			}
		}
		return id;
	}

	protected static List<String> parseAtomicService(Node node,
			Map<String, AtomicService> services) throws DOMException,
			ClassNotFoundException {

		final NodeList nodeList = node.getChildNodes();

		final List<String> concurrentServices = new LinkedList<String>();
		final List<AtomicService> redundantServices = new LinkedList<AtomicService>();
		NodeList subList = null;
		final List<Class<?>> parameterTypes = new LinkedList<Class<?>>();
		for (int j = 0; j < nodeList.getLength(); j++) {

			if (nodeList.item(j).getNodeType() == Node.ELEMENT_NODE) {
				if (nodeList.item(j).getNodeName().equals("arguments")) {
					subList = nodeList.item(j).getChildNodes();

					for (int i = 0; i < subList.getLength(); i++) {
						if (subList.item(i).getNodeType() == Node.ELEMENT_NODE) {

							parameterTypes.add(filterPrimitives(subList.item(i)
									.getAttributes().getNamedItem("type")
									.getNodeValue()));
						}
					}

				} else if (nodeList.item(j).getNodeName().equals(
						"concurrentDeliverableServices")) {
					subList = nodeList.item(j).getChildNodes();

					for (int i = 0; i < subList.getLength(); i++) {
						if (subList.item(i).getNodeType() == Node.ELEMENT_NODE) {

							concurrentServices.add(subList.item(i)
									.getAttributes().getNamedItem("name")
									.getNodeValue());
						}
					}
				} else {
					String key = nodeList.item(j).getAttributes().getNamedItem(
							"name").getNodeValue();
					AtomicService service = acquireService(services, key, node.getAttributes().getNamedItem("class").getNodeValue());
					if(!(service instanceof RedundantService))
						throw new ConfigurationException("Service: " + key
								+ " can only associated with redundant services");
					redundantServices.add(service);
				}
			}
		}

		AtomicService service = new AtomicService(
				node.getAttributes().getNamedItem("class").getNodeValue()
						+ "."
						+ node.getAttributes().getNamedItem("interface")
								.getNodeValue(), parameterTypes
						.toArray(new Class<?>[parameterTypes.size()]));
		
		if(node.getAttributes().getNamedItem("alias") != null)
		   service.setProxyAlias(node.getAttributes().getNamedItem("alias").getNodeValue());
		services.put(node.getAttributes().getNamedItem("name").getNodeValue(),
				service);
		if (redundantServices.size() != 0)
			service.setRedundantServices(redundantServices
					.toArray(new AtomicService[redundantServices.size()]));
		return concurrentServices.size() != 0 ? concurrentServices : null;
	}

	protected static AtomicService parseRedundantService(Node node)
			throws DOMException, ClassNotFoundException {

		final List<Class<?>> parameterTypes = new LinkedList<Class<?>>();
		if (node.getChildNodes().item(0) != null) {
			final NodeList nodeList = node.getChildNodes().item(0)
					.getChildNodes();

			for (int i = 0; i < nodeList.getLength(); i++) {
				if (nodeList.item(i).getNodeType() == Node.ELEMENT_NODE) {
					parameterTypes.add(filterPrimitives(nodeList.item(i)
							.getAttributes().getNamedItem("type")
							.getNodeValue()));
				}
			}

		}
		RedundantService service = new RedundantService(parameterTypes
				.toArray(new Class<?>[parameterTypes.size()]), node
				.getAttributes().getNamedItem("class").getNodeValue()
				+ "."
				+ node.getAttributes().getNamedItem("interface").getNodeValue());
		
		if(node.getAttributes().getNamedItem("alias") != null)
			   service.setProxyAlias(node.getAttributes().getNamedItem("alias").getNodeValue());
		
		return service;

	}

	protected static List<String> parseCompositeService(Node node,
			Map<String, AtomicService> services) throws DOMException,
			ClassNotFoundException {

		final NodeList nodeList = node.getChildNodes();
		NodeList subList = null;
		final List<Class<?>> parameterTypes = new LinkedList<Class<?>>();
		final List<String> nestedServices = new LinkedList<String>();
		for (int j = 0; j < nodeList.getLength(); j++) {
			if (nodeList.item(j).getNodeType() == Node.ELEMENT_NODE) {
				if (nodeList.item(j).getNodeName().equals("arguments")) {
					subList = nodeList.item(j).getChildNodes();

					for (int i = 0; i < subList.getLength(); i++) {
						if (subList.item(i).getNodeType() == Node.ELEMENT_NODE) {
							parameterTypes.add(filterPrimitives(subList.item(i)
									.getAttributes().getNamedItem("type")
									.getNodeValue()));
						}
					}

				} else
					nestedServices.add(nodeList.item(j).getAttributes()
							.getNamedItem("name").getNodeValue());
			}
		}

		CompositeService service = new CompositeService(parameterTypes
				.toArray(new Class<?>[parameterTypes.size()]), node
				.getAttributes().getNamedItem("class").getNodeValue()
				+ "."
				+ node.getAttributes().getNamedItem("interface").getNodeValue());

		services.put(node.getAttributes().getNamedItem("name").getNodeValue(),
				service);
		if(node.getAttributes().getNamedItem("alias") != null)
			   service.setProxyAlias(node.getAttributes().getNamedItem("alias").getNodeValue());
		return nestedServices;
	}

	protected static void recusiveBuildNestedService(List<String> keys,
			String key, List<String> value,
			Map<String, List<String>> compositeServices,
			Map<String, AtomicService> services) {

		if (keys.contains(key))
			throw new ConfigurationException(
					"Service: "
							+ key
							+ " is detectd involved cyclic-reference problem, please check your configuration file!");

		keys.add(key);
		CompositeService compositeService = (CompositeService) services
				.get(key);
		List<AtomicService> nestedServices = new LinkedList<AtomicService>();
		AtomicService atomicService = null;
		AtomicService[] nestedArray = null;
		for (int i = 0; i < value.size(); i++) {

			atomicService = acquireService(services, value.get(i), key);
			if (atomicService instanceof CompositeService) {

				if (((CompositeService) atomicService).getServices() == null)
					recusiveBuildNestedService(keys, value.get(i),
							compositeServices.get(value.get(i)),
							compositeServices, services);
				else {
					nestedArray = ((CompositeService) atomicService)
							.getServices();
					for (AtomicService service : nestedArray)
						nestedServices.add(service);
				}

			} else
				nestedServices.add(atomicService);
		}

		compositeService.setServices(nestedServices
				.toArray(new AtomicService[nestedServices.size()]));
	}

	private static Class<?> filterPrimitives(String clazzName)
			throws ClassNotFoundException {
		Class<?> clazz = primitiveMap.get(clazzName);
		if (clazz == null)
			return Class.forName(clazzName);

		return clazz;
	}
	
	private static AtomicService acquireService(Map<String, AtomicService> services, String key, String context){
		

		AtomicService service = services.get(key);
		if (service == null)
			throw new ConfigurationException("Service " + key
					+ " can not be found in " + context);
		
		return service;
	}

	public static AdaptorDelegate getAdaptor() {
		return adaptor;
	}

	public static void initialize(AdaptorDelegate adaptor) {
		if (ConfigurationParser.adaptor == null) {
			ConfigurationParser.adaptor = adaptor;
			try {
			
				init();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public interface AdaptorDelegate {
		public Group getGroup(String name);
		public void init();
	}
}
