package org.ssor.protocol.replication;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * This header is used when there is need to retransmit nested services of a composed service
 * @author Tao Chen
 *
 */
public class BatchRequestHeader extends RequestHeader {

	
	private Integer[] retransmitedIndices;
	
	public BatchRequestHeader(RequestHeader header, Integer[] retransmitedArray){
		this.service = header.getService();
		this.sessionId = header.getSessionId();
		this.isNonOrdered = header.isNonOrdered();
		this.retransmitedIndices  = retransmitedArray;
	}
	
	@Override
	public void readFrom(DataInputStream in) throws IOException,
			IllegalAccessException, InstantiationException {
		
		int length = in.readInt();
		retransmitedIndices = new Integer[length];
		for(int i = 0; i < length; i++)
			retransmitedIndices[i] = in.readInt();
		super.readFrom(in);
	}



	@Override
	public void writeTo(DataOutputStream out) throws IOException {
		
		int length = retransmitedIndices.length;
		out.writeInt(length);
		for(int i = 0; i < length; i++)
			out.writeInt(retransmitedIndices[i]);
		super.writeTo(out);
	}

	public Integer[] getRetransmitedIndices() {
		return retransmitedIndices;
	}
}
