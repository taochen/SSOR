package org.ssor.protocol;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.UUID;

import org.ssor.Sequence;
import org.ssor.protocol.election.Decision;
import org.ssor.protocol.election.DecisionHeader;
import org.ssor.protocol.replication.ResponseHeader;
import org.ssor.protocol.replication.ResponsePacket;
import org.ssor.protocol.replication.abcast.SequenceCollector;
import org.ssor.util.Streamable;
import org.ssor.util.Util;

public class Message implements Streamable, Comparable<Message>, Cloneable {

	public static final short DELAY_PACKET = 0;
	public static final short DECISION = 2;
	public static final short OTHERS = 1;

	private Header header;
	private Object body;
	private String reqId;
	private Long viewSeqno;

	// Identify if the message has been timestamped
	private transient Boolean isHoldSeq = false;
	// Identify if the message needs to be ordered
	private transient Boolean isRequirePreBroadcasting = true;
	// Identify if the message can be executed on the requester site
	private transient Boolean isExecutable = false;
	// Identify if the message has got the timestamp, normally used to judge if there is
	// need to remove a message from cache in the case of failure
	private transient Boolean isNeedRetransmited = true;
	
	
	private transient Boolean isExecuteInAdvance = false;
	
	private transient Object src;
	
	private transient Short FIFO_Scope;
	
	// Cache and wait until all sequences of nested service is received
	private transient SequenceCollector order;

	public Message() {
	}

	public Boolean isHoldSeq() {
		return isHoldSeq;
	}

	public void setHoldSeq(boolean isTimestamp) {
		this.isHoldSeq = isTimestamp;
	}

	public Message(Header header, Object body, boolean isGen) {
		super();
		this.header = header;
		this.body = body;
		if (isGen)
			reqId = UUID.randomUUID().toString().replaceAll("-", "");
	}

	public Object getBody() {
		return body;
	}

	public void setBody(Object body) {
		this.body = body;
	}

	public Header getHeader() {
		return header;
	}

	public void setHeader(Header header) {
		this.header = header;
	}

	@Override
	public int compareTo(Message another) {
		ResponseHeader anotherHeader = (ResponseHeader) another.header.outer;
		ResponseHeader thisHeader = (ResponseHeader) header.outer;

		return thisHeader.compare(anotherHeader);
	}

	public Boolean isRequirePreBroadcasting() {
		return isRequirePreBroadcasting;
	}

	public void setRequirePreBroadcasting(boolean isRequirePreBroadcasting) {
		this.isRequirePreBroadcasting = isRequirePreBroadcasting;
	}

	public Boolean getIsRequirePreBroadcasting() {
		return isRequirePreBroadcasting;
	}

	public void setIsRequirePreBroadcasting(Boolean isRequirePreBroadcasting) {
		this.isRequirePreBroadcasting = isRequirePreBroadcasting;
	}

	public String getReqId() {
		return reqId;
	}

	public void setReqId(String reqId) {
		this.reqId = reqId;
	}

	public Boolean isExecutable() {
		return isExecutable;
	}

	public void setIsExecutable(Boolean isExecutable) {
		this.isExecutable = isExecutable;
	}

	public Short getFIFO_Scope() {
		return FIFO_Scope;
	}

	public void setFIFO_Scope(Short scope) {
		FIFO_Scope = scope;
	}


	@Override
	public void readFrom(DataInputStream in) throws IOException,
			IllegalAccessException, InstantiationException {
		
		short type = in.readShort();
		switch (type) {
		case DELAY_PACKET: {
		
			ResponsePacket packet = new ResponsePacket();
			packet.readFrom(in);
			body = packet;

			break;

		} case DECISION: {

			int length = in.readInt();
			Decision[] decisions = new Decision[length];

			for (int i = 0; i < length; i++) {
				decisions[i] = new Decision();
				decisions[i].readFrom(in);
			}

			body = decisions;

			break;

		} case OTHERS: {
			try {
				body = Util.readArbitraryObject(in);
			} catch (Exception e) {
				throw new IOException(e);
			}

			break;
		}
		}

		reqId = Util.readString(in);
		long id = 0;
		if((id = in.readLong()) != -1)
		  viewSeqno = id;
		header = Header.externalReadFrom(in);
	}

	@Override
	public void writeTo(DataOutputStream out) throws IOException {

		if (body instanceof ResponsePacket) {
			out.writeShort(DELAY_PACKET);
			((ResponsePacket) body).writeTo(out);
		} else if (body instanceof Decision[]) {
			out.writeShort(DECISION);
			Decision[] decisions = (Decision[]) body;
			int length = decisions.length;
			out.writeInt(length);

			for (int i = 0; i < length; i++)
				decisions[i].writeTo(out);

		} else {
			out.writeShort(OTHERS);
			try {
				Util.writeArbitraryObject(out, body);
			} catch (Exception e) {
				throw new IOException(e);
			}
		}

		// Write request ID
		Util.writeString(reqId, out);
		if(viewSeqno != null)
           out.writeLong(viewSeqno);
		else
		   out.writeLong(-1);
		// Write headers
		Header.externalWriteTo(out, header);
	
	}

	public Boolean isNeedRetransmited() {
		return isNeedRetransmited;
	}

	public void setIsNeedRetransmited(Boolean isNeedRetransmited) {
		this.isNeedRetransmited = isNeedRetransmited;
	}


	public SequenceCollector getSequenceCache(){
		return order;
	}

	public void createSequenceCache(Sequence[] timestamps, int count){
		order = new SequenceCollector(timestamps, count);
	}

	public Boolean isExecuteInAdvance() {
		return isExecuteInAdvance;
	}

	public void setIsExecuteInAdvance(Boolean passOrderingLayer) {
		this.isExecuteInAdvance = passOrderingLayer;
	}

	public Object getSrc() {
		return src;
	}

	public void setSrc(Object src) {
		this.src = src;
	}

	public Long getViewSeqno() {
		return viewSeqno;
	}

	public void setViewSeqno(long viewSeqno) {
		this.viewSeqno = viewSeqno;
	}
	
	public boolean isNeedUnicastConsistency(){	
		return viewSeqno != null && !(header instanceof DecisionHeader);
	}
	
	public String toString(){
		return "(ID: " + reqId + ", first header: " + header.getClass().getName() + "}";
	}

}
