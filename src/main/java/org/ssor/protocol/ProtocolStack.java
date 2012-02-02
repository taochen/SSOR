package org.ssor.protocol;

import java.util.Map;

import org.ssor.CollectionFacade;
import org.ssor.exception.ConfigurationException;
import org.ssor.invocation.InvocationAdaptor;
import org.ssor.invocation.NativeInvocationHandler;
import org.ssor.listener.CommunicationListener;
import org.ssor.listener.InvocationAware;
import org.ssor.listener.RequirementsAware;
import org.ssor.util.Group;
import org.ssor.util.Util;

public class ProtocolStack {

	private Protocol[] protocols;

	private int length;

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
	
	
	public void init(Group group) {
		
		@SuppressWarnings("unchecked")
		final Map<String, Message> sentMessages = CollectionFacade.getConcurrentHashMap();
		final InvocationAdaptor invocationAdaptor = new NativeInvocationHandler(group, group, group.getProxyFactory());
		for (Protocol protocol : protocols){
			
			if(protocol instanceof RequirementsAware)
				((RequirementsAware)protocol).setRequirementsAwareAdaptor(group);
			if(protocol instanceof CommunicationListener)
				((CommunicationListener)protocol).setCommunicationAdaptor(group);
			if(protocol instanceof InvocationAware) {
				((InvocationAware)protocol).setInvocationAdaptor(invocationAdaptor);
				((InvocationAware)protocol).setCallback(group.getCallback());
			} 
			if(protocol instanceof ProtocolSharableInstances){
				((ProtocolSharableInstances)protocol).setCachedSentMessages(sentMessages);
			}
			
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

	

}
