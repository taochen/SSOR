package org.ssor.protocol.replication;

import org.ssor.protocol.PDU;

public class ReplicationUnit extends PDU{

	private String reqId;
	private Object tuple;
	private String service;;
	

	public ReplicationUnit(String reqId, Object tuple, Object address) {
		super(address);
		this.reqId = reqId;
		this.tuple = tuple;
	}

	public ReplicationUnit(String reqId) {
		super(null);
		this.reqId = reqId;
	}
	
	public ReplicationUnit(String reqId, Object timestamp) {
		super(null);
		this.reqId = reqId;
		this.tuple = timestamp;
	}


	public String getReqId() {
		return reqId;
	}

	public Object getTuple() {
		return tuple;
	}

	public String getService() {
		return service;
	}

	
	
	public boolean isSendSeq(){
		return address == null;
	}
	
}
