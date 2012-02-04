package org.ssor.protocol;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ssor.annotation.ProtocolListenerHelper;
import org.ssor.conf.ClassConfigurator;
import org.ssor.exception.ConfigurationException;
import org.ssor.listener.helper.ProtocolHelper;
import org.ssor.util.Group;
import org.ssor.util.Util;

public class ProtocolStack {

	private Protocol[] protocols;

	private int length;
	
	private final Logger logger = LoggerFactory
	.getLogger(ProtocolStack.class);

	public ProtocolStack(String[] classes) {
		try {
			init(classes);
		 
		} catch (Exception e) {
			throw new ConfigurationException("Protocls initialization failed", e);
		}
	}

	
	private void init(String[] classes) throws InstantiationException, 
	IllegalAccessException, ClassNotFoundException {
		protocols = new Protocol[classes.length];
		/*
		protocols = new Protocol[] { new REP(), 
				                     new FT(), 
				                     new AR(),
				                     new MSP() 
		};*/

		length = protocols.length;
		for (int i = 0; i < protocols.length; i++) {
			protocols[i] = (Protocol)Util.loadClass(classes[i], ProtocolStack.class).newInstance();
		}
		
		for (int i = 0; i < protocols.length; i++) {

			if (i != length - 1) {
				protocols[i].setLower(protocols[i + 1]);
				protocols[i + 1].setUpper(protocols[i]);
				
			}

		}
	}

	public Token up(short command, Token value) {
		return protocols[length - 1].up(command, value);
	}

	public Token down(short command, Token value) {
		return protocols[0].down(command, value);
	}
	
	
	@SuppressWarnings("unchecked")
	public void init(Group group) {
		
		final String[] listeners = ClassConfigurator.getProtocolListenerClasses();
		Class<?> clazz = null;
		ProtocolHelper helper = null;
		Object params = null;
		try {
			for (String className : listeners) {
			
					clazz = Util.loadClass(className, ProtocolStack.class);
				    if (logger.isDebugEnabled()) {
				    	logger.debug("Loading for listener: "  + clazz);
				    }
				    
				    if ((ProtocolListenerHelper)clazz.getAnnotation(ProtocolListenerHelper.class) == null) {
				    	throw new ConfigurationException("Class " + clazz + " has no binding helper");
				    }
				    
					helper = (ProtocolHelper)clazz.getAnnotation(ProtocolListenerHelper.class).helperClass().newInstance();
				    params = helper.createParameters(group);
					for (Protocol protocol : protocols){
						if (clazz.isInstance(protocol)) {
						    helper.assignParameters(protocol, group, params);
						    if (logger.isDebugEnabled()) {
						    	logger.debug("Protocl "  + protocol.getClass() + " is listening to listener " + clazz +" with helper " + helper.getClass());
						    }
						}
					}			    
			}
		} catch (ClassNotFoundException e) {
			logger.error("Loading class failed", e);			
		} catch (InstantiationException e) {
			logger.error("Binding listener failed", e);;
		} catch (IllegalAccessException e) {
			logger.error("Binding listener failed", e);
		}
		
		for (Protocol protocol : protocols){
			protocol.setUUID_ADDR(group.getUUID_ADDR());
			protocol.init();
			protocol.setStack(this);
			protocol.setUtil(group.getUtil());
		}
	}
	
	public void finishConsensus() {
		for (Protocol protocol : protocols){
			protocol.finishConsensus();
		}
	}

	public Protocol[] getProtocols() {
		return protocols;
	}

}
