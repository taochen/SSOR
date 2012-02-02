package org.ssor.invocation;

import org.ssor.AtomicService;

public class InvocationUnit {

	private AtomicService service;
	private Object[] arguments;
	private Object address;
	public InvocationUnit(AtomicService service, Object[] arguments) {
		super();
		this.service = service;
		this.arguments = arguments;
	}
	public AtomicService getService() {
		return service;
	}
	public Object[] getArguments() {
		return arguments;
	}
	public Object getAddress() {
		return address;
	}
	public void setAddress(Object address) {
		this.address = address;
	}
	
	
	
}
