package org.ssor.protocol;

/**
 * The data that passing through the protocol stack, the previous protocol has
 * the right to modify action of the next protocol.
 * 
 * @author Tao Chen
 * 
 */
public class Token implements Cloneable{

	public static final int NO_CHANGE = 0;

	public static final int REPLICATION_REQUEST_REPLICABLE = 1;
	
	public static final int REPLICATION_REQUEST_NON_REPLICABLE = 2;
	
	public static final int REPLICATION_REQUEST_SEQUENCER_NOT_EXIST = 3;
	
	public static final int REPLICATION_COORDINATE_NOT_SEQUENCER = 4;
	
	public static final int REPLICATION_ACQUIRE_SEQUENCE_NOT_COLLECTED = 5;
	
	public static final int REPLICATION_ACQUIRE_SEQUENCE_DISCARDED = 6;
	
	public static final int REPLICATION_ACQUIRE_BORADCAST = 7;
	
	public static final int REPLICATION_ACQUIRE_LAZY_BORADCAST = 8;
		
	public static final int ELECTION_JOIN_DISCARD = 11;
	
	public static final int ELECTION_JOIN_INTERSTS_ONLY= 12;
	
	public static final int ELECTION_JOIN_INTERSTS_AND_REGIONS= 13;
	
	public static final int ELECTION_RECORD_FINAL_BROADCAST= 14;
	
	public static final int ELECTION_RECORD_FINAL_BROADCAST_FOR_THIS= 15;
	
	public static final int ELECTION_AGREEMENT_NO_NEW_REGION= 16;
	
	public static final int FT_AGREEMENT_RETRANSMISSION= 17;
	// This normally represent a decision from the previous protocol
	private int nextAction = NO_CHANGE;

	private Object data;
	private Object dataForNextProtocol;

	public Token(Object data) {
		super();
		this.data = data;
		dataForNextProtocol = data;
	}

	public Token(int nextAction, Object data) {
		super();
		this.nextAction = nextAction;
		this.data = data;
		dataForNextProtocol = data;
	}
	

	public void setNextAction(int nextAction) {
		this.nextAction = nextAction;
	}


	public int getNextAction() {
		return nextAction;
	}

	public Object getData() {
		return data;
	}

	public Object getDataForNextProtocol() {
		return dataForNextProtocol;
	}

	public void setDataForNextProtocol(Object dataForNextProtocol) {
		this.dataForNextProtocol = dataForNextProtocol;
	}
	
	public void reset(){
		data = dataForNextProtocol;
	}
	
	public Token clone(){
		Token token = new Token(nextAction, data);
		token.dataForNextProtocol = dataForNextProtocol;
		return token;
	}
	
	public Token clone(Object dataForNextProtocol, Object data){
		Token token = new Token(nextAction, data);
		token.dataForNextProtocol = dataForNextProtocol;
		return token;
	}

}
