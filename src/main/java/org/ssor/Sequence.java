package org.ssor;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.ssor.protocol.replication.abcast.SequenceVector;
import org.ssor.util.Streamable;

public class Sequence implements Streamable,Comparable<Sequence> {

	
	private Integer seqno;
	private Integer concurrentno;
	// Only for temp used
	private transient SequenceVector[] vector;
	// Only for recording on cache
	private transient Integer regionNumber;
	private transient String sessionId;
	// Used to identify if a cached executed sequence has been sent, in case of 
	// crash of new sequencer before skipped sequences has been passed
	private transient boolean isProposedForConsensus = false;

	public Sequence(SequenceVector[] vector) {
		super();
		this.vector = vector;
	}
	public Sequence() {
		super();
	}
	public Sequence(Integer timestamp, Integer concurrentSeq) {
		super();
		this.seqno = timestamp;
		this.concurrentno = concurrentSeq;
	}
	
	public Sequence(Integer regionNumber, Integer timestamp, Integer concurrentSeq) {
		super();
		this.regionNumber  = regionNumber;
		this.seqno = timestamp;
		this.concurrentno = concurrentSeq;
	}

	public Sequence(Integer regionNumber, String sessionId, Integer timestamp, Integer concurrentSeq) {
		super();
		this.regionNumber = regionNumber;
		this.seqno = timestamp;
		this.sessionId = sessionId;
		this.concurrentno = concurrentSeq;
	}
	
	public Integer getRegionNumber() {
		return regionNumber;
	}
	
	public void setRegionNumber(Integer regionNumber) {
		this.regionNumber = regionNumber;
	}
	public Integer getSeqno() {
		return seqno;
	}
	public Integer getConcurrentno() {
		return concurrentno;
	}
	@Override
	public void readFrom(DataInputStream in) throws IOException,
			IllegalAccessException, InstantiationException {
		seqno = in.readInt();
		short type = in.readShort();
		if(1 == type)
			concurrentno = in.readInt();
		
	}
	@Override
	public void writeTo(DataOutputStream out) throws IOException {
		// Timestamp would never be null
		out.writeInt(seqno);
		if(concurrentno == null)
			out.writeShort(0);
		else {
			out.writeShort(1);
			out.writeInt(concurrentno);
		}
		
	}
	
	public String toString(){
		return "(seqno# " + seqno + ", concurrentno# " + concurrentno + ")";
	}
	public SequenceVector[] getVector() {
		return vector;
	}
	
	
	/**
	 * They are maintained in the same region
	 */
	
	public boolean isEquals(Sequence another){

		if (seqno.equals(another.seqno) && isSessionEquals(another)){
			
			if(concurrentno == null && another.concurrentno == null)
			    return true;
			
			if(concurrentno != null && concurrentno.equals(another.concurrentno))
				return true;
		}

		return false;
	}
	
	
	/**
	 * They are maintained in the same region, they are identified as equals
	 * even both are null
	 */
	public boolean isSessionEquals(Sequence another){
		
		if(sessionId != null && another.getSessionId() != null)
			return sessionId.equals(another.getSessionId());
		else if(sessionId == null && another.getSessionId() == null)
			return true;
			
		
		
		return false;
	}
	
	
	/**
	 * They are maintained in the same region
	 */
	
	public int isSubsequent(Sequence another, int exceptedConcurrentServiceTimestamp){
//System.out.print(exceptedConcurrentServiceTimestamp+" : "+another.getConcurrentSeq()+"\n");
		if(another.getConcurrentno() == null && seqno.equals(another.getSeqno() - 1))
			return 0;
		else if(another.getConcurrentno() != null && exceptedConcurrentServiceTimestamp == another.getConcurrentno() 
				&& seqno.equals(another.getSeqno() - 1))
			return 0;
		else if(seqno.equals(another.getSeqno()))
			return -1;
		else
			return 1;
	}
	
	public String getSessionId() {
		return sessionId;
	}
	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}
	
	/**
	 * They are maintained in the same region
	 */
	@Override
	public int compareTo(Sequence another) {
		if(seqno > another.seqno)
			return 1;
		else if(seqno < another.seqno)
		    return -1;
		else {
			// It does not matter if both are -1
			if(concurrentno != null && concurrentno == -1)
				return 1;
			else if (another.concurrentno != null && another.concurrentno == -1)
				return -1;
			else 
				return 0;
		}
	}
	public boolean isProposedForConsensus() {
		return isProposedForConsensus;
	}
	public void isProposedForConsensus(boolean isReceiveForConsensus) {
		this.isProposedForConsensus = isReceiveForConsensus;
	}
	
	
}
