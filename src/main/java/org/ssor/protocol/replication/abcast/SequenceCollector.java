package org.ssor.protocol.replication.abcast;

import java.util.LinkedList;
import java.util.List;

import org.ssor.Sequence;




/**
 * This is the cache for request waiting order timestamps of nested services
 * it does not meant to be transmitted
 * @author Tao Chen
 *
 */
public class SequenceCollector {

	// The pending sequence array
	private Sequence[] sequences;
	private int length;
	private int load;
	
	// Record the index of sequences that have not been received
	private List<Integer> waitingIndices;
	
	public SequenceCollector(Sequence[] timestamps, int count) {
		super();
		this.sequences = timestamps;
		this.length = timestamps.length - count;
		// This is mainly used for FT, when sequencer crash
		waitingIndices = new LinkedList<Integer>();
		for(int i = 0; i < length; i++)
			waitingIndices.add(i);
	}
	public boolean add(SequenceVector vector){
		
		synchronized(waitingIndices){
			
			if(sequences[vector.getIndex()] != null)
				return (load == length)? true : false;
			
			sequences[vector.getIndex()] = vector.getSequence();
			load++;
			waitingIndices.remove((Integer)vector.getIndex());
			return (load == length)? true : false;
		}
	}
	
	public void remove(SequenceVector vector){
		synchronized(waitingIndices){
			sequences[vector.getIndex()] = null;
			waitingIndices.add((Integer)vector.getIndex());
			load--;
		}
	}
	
	
	public Sequence[] getSequences() {
		return sequences;
	}
	public int getLength() {
		return length;
	}
	
	public boolean isNeedRetransmission(){
		return waitingIndices.size() != 0;
	}
	public List<Integer> getWaitingIndices() {
		return waitingIndices;
	}
	
	
}
