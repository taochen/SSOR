package org.ssor.protocol.election;

import org.ssor.protocol.PDU;

public class ElectionUnit extends PDU{

	private Decision[] decision;

	
	public ElectionUnit(Object address, Decision[] decision) {
		super(address);
		this.decision = decision;
	}
	
	public Decision[] getDecision() {
		return decision;
	}
	
}
