package org.ssor.protocol.replication.abcast;

import org.ssor.AtomicService;
import org.ssor.protocol.Header;

public class DeliveryPacket implements Cloneable{

	private AtomicService service;
	private Object[] arguments;
	private Header iHeader;
	private Object sequence;
	
	// The return result of service, if composite service,
	// then eventually this would be the result of last executed nested service. 
	private Object result;
	
	
	public Object getResult() {
		return result;
	}
	public void setResult(Object result) {
		this.result = result;
	}
	public DeliveryPacket() {
		super();
		// TODO Auto-generated constructor stub
	}
	public DeliveryPacket(AtomicService service, Object[] arguments, Header header,
			Object sequence) {
		super();
		this.service = service;
		this.arguments = arguments;
		this.iHeader = header;
		this.sequence = sequence;
	}
	public AtomicService getService() {
		return service;
	}
	public Object[] getArguments() {
		return arguments;
	}
	public Header getIHeader() {
		return iHeader;
	}
	public Object getSequence() {
		return sequence;
	}
	
	public DeliveryPacket reset(AtomicService service, Object[] arguments, 
			Object sequence){
		this.service = service;
		this.arguments = arguments;
		this.sequence = sequence;
		return this;
	}
	
	public DeliveryPacket clone(){
		return new DeliveryPacket(service, arguments, iHeader, sequence);
	}
	
}
