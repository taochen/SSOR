package org.ssor.conf;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ssor.CollectionFacade;
import org.ssor.exception.ConfigurationException;
import org.ssor.protocol.Header;
import org.ssor.util.Tuple;
import org.ssor.util.Util;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.IOException;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.NamedNodeMap;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;

/**
 * 
 * @author Tao Chen  extends from JGroup
 *
 */
@SuppressWarnings("unchecked")
public class ClassConfigurator {
	 
	private static final Logger logger = LoggerFactory
	.getLogger(ClassConfigurator.class);
	
	public static final String MAGIC_NUMBER_FILE = "ssor-streaming.xml";
	public static final String PROTOCOL_FILE = "ssor-protocols.xml";
	private static final Map<Class<?>,Short> streamableClassMap= CollectionFacade.getConcurrentHashMap(); // key=Class, value=magic number
    private static final Map<Short,Class<?>> streamableMagicMap= CollectionFacade.getConcurrentHashMap(); // key=magic number, value=Class

    /*
     * For mapping of array objects, class here is only the meta class
     */
	private static final Map<Class<?>,Short> primitiveClassMap= CollectionFacade.getConcurrentHashMap(); // key=Class, value=magic number
    private static final Map<Short,Class<?>> primitiveMagicMap= CollectionFacade.getConcurrentHashMap(); // key=magic number, value=Class

    
    private static String[] protocolClasses;
    static {
        try {
            init();
        }
        catch(Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }
    
    
    protected static void init() throws Exception {
    	
    	Util.loadClass("javax.xml.parsers.DocumentBuilderFactory", ClassConfigurator.class);
    	
    	// Read mapping file
    	String prefix = ClassConfigurator.class.getProtectionDomain()
		                              .getCodeSource().getLocation().getFile();
    	if(prefix.endsWith("ClassConfigurator.class"))
    		prefix = prefix.replace("org/ssor/conf/ClassConfigurator.class", "");
        List<Tuple<Short,String>> mapping= (List<Tuple<Short,String>>)read(prefix + MAGIC_NUMBER_FILE, true);
        
        List<String> list = null;
        protocolClasses = (list = (List<String>)read(prefix + PROTOCOL_FILE, false)).toArray(new String[list.size()]);
        
        short m = -1;
        for(Tuple<Short,String> tuple: mapping) {
            m = tuple.getVal1();
            // Can not be end of header number
            if(m == Header.EOH)
            	 throw new ConfigurationException(Header.EOH + " is predefined for end of header mark, therefore it can not be used as magic number");
            try {
            	
                Class clazz=Util.loadClass(tuple.getVal2(), ClassConfigurator.class);
                if(streamableMagicMap.containsKey(m))
                    throw new ConfigurationException("key " + m + " (" + clazz.getName() + ')' +
                            " is already in magic map; please make sure that all keys are unique");
                
                streamableMagicMap.put(m, clazz);
                streamableClassMap.put(clazz, m);
            } catch(ClassNotFoundException e) {
            	throw new ConfigurationException("failed loading class", e);
                
            }
        }
        
        primitiveClassMap.put(int.class, (short)1);
        primitiveClassMap.put(boolean.class, (short)2);
        primitiveClassMap.put(byte.class, (short)3);
        primitiveClassMap.put(char.class, (short)4);
        primitiveClassMap.put(long.class, (short)5);
        primitiveClassMap.put(float.class, (short)6);
        primitiveClassMap.put(double.class, (short)7);
        primitiveClassMap.put(short.class, (short)8);
        primitiveClassMap.put(Integer.class, (short)9);
        primitiveClassMap.put(Boolean.class, (short)10);
        primitiveClassMap.put(Byte.class, (short)11);
        primitiveClassMap.put(Character.class, (short)12);
        primitiveClassMap.put(Long.class, (short)13);
        primitiveClassMap.put(Float.class, (short)14);
        primitiveClassMap.put(Double.class, (short)15);
        primitiveClassMap.put(Short.class, (short)16);
        primitiveClassMap.put(String.class, (short)17);
        primitiveClassMap.put(Object.class, (short)18);
        
        
        for(Map.Entry<Class<?>, Short> entry: primitiveClassMap.entrySet()){
        	primitiveMagicMap.put(entry.getValue(), entry.getKey());
        }
        
    }
    
    
    public static Class get(short magic) {
        return streamableMagicMap.get(magic);
    }
    
    public static short getMagicNumber(Class clazz) {
        Short i=streamableClassMap.get(clazz);
        if(i == null)
            return -1;
        else
            return i;
    }
    
    public static Class getArray(short magic) {
        return primitiveMagicMap.get(magic);
    }
    
    public static short getMagicNumber(String name) {
    
    	Class<?> clazz = null;
		try {
			clazz = Class.forName(name);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
    	
        if(primitiveClassMap.containsKey(clazz))
        	return primitiveClassMap.get(clazz);
        
        return -1;
    }
    
    public static Class get(String clazzname) {
        try {
            return Util.loadClass(clazzname, ClassConfigurator.class);
        }
        catch(Exception x) {
            if(logger.isErrorEnabled()) 
            	logger.error("failed loading class " + clazzname, x);
        }
        return null;
    }
    
    
    /**
     * try to read the magic number configuration file as a Resource form the classpath using getResourceAsStream
     * if this fails this method tries to read the configuration file from mMagicNumberFile using a FileInputStream (not in classpath but somewhere else in the disk)
     *
     * @return an array of ClassMap objects that where parsed from the file (if found) or an empty array if file not found or had en exception
     */
    protected static Object read(String name, boolean isMapping) throws Exception {
        InputStream stream;
        try {
            stream=Util.getResourceAsStream(name, ClassConfigurator.class);
            // try to load the map from file even if it is not a Resource in the class path
            if(stream == null) {
                if(logger.isTraceEnabled())
                	logger.trace("Could not read " + name + " from the CLASSPATH, will try to read it from file");
                stream=new FileInputStream(name);
            }
        }
        catch(Exception x) {
            throw new ConfigurationException(name + " not found. Please make sure it is on the classpath", x);
        }
        return isMapping ? parseMapping(stream) : parseProtocol(stream);
    }

    protected static List<Tuple<Short,String>> parseMapping(InputStream stream) throws Exception {
        DocumentBuilderFactory factory=DocumentBuilderFactory.newInstance();
        factory.setValidating(false); // for now
        DocumentBuilder builder=factory.newDocumentBuilder();
        Document document=builder.parse(stream);
        NodeList class_list=document.getElementsByTagName("class");
        List<Tuple<Short,String>> list=new LinkedList<Tuple<Short,String>>();
        for(int i=0; i < class_list.getLength(); i++) {
            if(class_list.item(i).getNodeType() == Node.ELEMENT_NODE) {
                list.add(parseClassData(class_list.item(i)));
            }
        }
        return list;
    }

    protected static Tuple<Short,String> parseClassData(Node protocol) throws IOException {
        try {
            protocol.normalize();
            NamedNodeMap attrs=protocol.getAttributes();
            String clazzname;
            String magicnumber;

            magicnumber=attrs.getNamedItem("id").getNodeValue();
            clazzname=attrs.getNamedItem("name").getNodeValue();
            return new Tuple<Short,String>(Short.valueOf(magicnumber), clazzname);
        }
        catch(Exception x) {
            IOException tmp=new IOException();
            tmp.initCause(x);
            throw tmp;
        }
    }
    

    protected static List<String> parseProtocol(InputStream stream) throws Exception {
        DocumentBuilderFactory factory=DocumentBuilderFactory.newInstance();
        factory.setValidating(false); // for now
        DocumentBuilder builder=factory.newDocumentBuilder();
        Document document=builder.parse(stream);
        NodeList class_list=document.getElementsByTagName("protocol");
        List<String> list=new LinkedList<String>();
        for(int i=0; i < class_list.getLength(); i++) {
            if(class_list.item(i).getNodeType() == Node.ELEMENT_NODE) {
                list.add(parseProtocolData(class_list.item(i)));
            }
        }
        return list;
    }

    protected static String parseProtocolData(Node protocol) throws IOException {
        try {
            protocol.normalize();
            NamedNodeMap attrs=protocol.getAttributes();
            

            String clazzname = attrs.getNamedItem("class").getNodeValue();
            
            return clazzname;
        }
        catch(Exception x) {
            IOException tmp=new IOException();
            tmp.initCause(x);
            throw tmp;
        }
    }


	public static String[] getProtocolClasses() {
		return protocolClasses;
	}


    
    
}
