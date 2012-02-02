package org.ssor.protocol;

public abstract class PDU {

	protected Object address;
	
	public PDU( Object address) {
		super();
		this.address = address;
	}

	public Object getAddress() {
		return address;
	}
	
}
