package org.ssor.protocol.replication.abcast;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.ssor.Sequence;
import org.ssor.util.Streamable;
/**
 * The vector that send back from sequencer, used for nested services that a sequencer may responsible for multiple regions
 * 
 * @author Tao Chen
 *
 */
public class SequenceVector implements Streamable{

	// The index of the service in the nested service stack
	private int index;
	private Sequence sequence;
	
	
	public SequenceVector() {
		super();
	}
	public SequenceVector(int index, Sequence timestamp) {
		super();
		this.index = index;
		this.sequence = timestamp;
	}
	public int getIndex() {
		return index;
	}
	public Sequence getSequence() {
		return sequence;
	}
	@Override
	public void readFrom(DataInputStream in) throws IOException,
			IllegalAccessException, InstantiationException {
		
		index = in.readInt();
		sequence = new Sequence();
		sequence.readFrom(in);
		
	}
	@Override
	public void writeTo(DataOutputStream out) throws IOException {
		
		out.writeInt(index);
        sequence.writeTo(out);
		
	}
	
	public String toString(){
		return "Index: " + index + ", " + sequence.toString();
	}
	
}
