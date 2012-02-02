package org.ssor;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

import org.ssor.util.Streamable;
import org.ssor.util.Util;

/**
 * The state represents the (expected_seqno, expected_concurrentno) that recevied from other node
 * when a new node joining in, it is used to set the (expected_seqno, expected_concurrentno) of current node
 * in comparison with sequences of suspended messages, if there is any.
 * @author Tao Chen
 *
 */
public class State implements Streamable {

	private int region;
	private String sessionId;
	private Sequence sequence;
	
	private transient boolean isFromStateTransfer = false; 
	
	
	public State() {
		super();
	}

	public State(int region, Sequence sequence) {
		super();
		this.region = region;
		this.sequence = sequence;
	}

	public State(int region, String sessionId, Sequence sequence) {
		super();
		this.region = region;
		this.sessionId = sessionId;
		this.sequence = sequence;
	}

	@Override
	public void readFrom(DataInputStream in) throws IOException,
			IllegalAccessException, InstantiationException {
		region = in.readInt();
		sequence = new Sequence();
		sequence.readFrom(in);
		sessionId = Util.readString(in);

	}

	@Override
	public void writeTo(DataOutputStream out) throws IOException {
		out.writeInt(region);
		sequence.writeTo(out);
		Util.writeString(sessionId, out);

	}

	public int getRegion() {
		return region;
	}

	public String getSessionId() {
		return sessionId;
	}

	public Sequence getSequence() {
		return sequence;
	}

	public void setSequence(Sequence sequence) {
		this.sequence = sequence;
		isFromStateTransfer = false;
	}
	
	
    public static class StateList implements Streamable{

    	private List<State> states;
    	private State[] receivedStates;
    	
		public StateList() {
			super();
		}

		public StateList(List<State> states) {
			super();
			this.states = states;
		}

		@Override
		public void readFrom(DataInputStream in) throws IOException,
				IllegalAccessException, InstantiationException {
			receivedStates = new State[in.readInt()];
			State state = null;
			for(int i = 0; i < receivedStates.length; i++){
				state = new State();
				state.readFrom(in);
				receivedStates[i] = state;
			}
			
		}

		@Override
		public void writeTo(DataOutputStream out) throws IOException {
			out.writeInt(states.size());
			for(State state : states)
				state.writeTo(out);
			
		}
		
		public State[] getStates(){
			return receivedStates;
		}
		
	}
    
    public void fromStateTransfer(){
    	isFromStateTransfer = true;
    }
    
	public String toString(){
		return "(Current state is::Region number: " + region + ", sequence: " + sequence + ", sessionId: " + sessionId + ")";
	}

	public boolean isFromStateTransfer() {
		return isFromStateTransfer;
	}

}
