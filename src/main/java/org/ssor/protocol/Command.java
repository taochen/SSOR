package org.ssor.protocol;

public class Command {
	
	public static final short ENTER_SERVICE = 0;

	public static final short LEAEVE_SERVICE = 1;
	
	public static final short ABCAST_FIRST_UNICAST = 2;
	
	public static final short ABCAST_NON_ORDERED_BROADCAST = 3;
	
	public static final short ABCAST_COORDINATE = 4;
	
	public static final short ABCAST_SECOND_UNICAST = 5;
	
	public static final short ABCAST_ACQUIRE_SEQUENCE = 6;
	
	public static final short ABCAST_FINAL_BROADCAST = 7;
	
	public static final short ABCAST_AGREEMENT = 8;
	
	public static final short ABCAST_DELAY_FINAL_BROADCAST = 9;
	
	public static final short ELECTION_FIRST_BROADCAST = 10;

	public static final short ELECTION_UNICAST = 11;
	
	public static final short ELECTION_SECOND_BROADCAST = 12;
	
	public static final short ELECTION_JOIN = 13;
	
	public static final short ELECTION_RECORD = 14;
	
	public static final short ELECTION_AGREEMENT = 15;
	
	public static final short FT_RELEASE_SEQUENCE = 16;
	
	// First broadcast is the ELECTION_SECOND_BROADCAST in election protocol
	public static final short FT_FINAL_BROADCAST = 17;
	
	public static final short FT_UNICAST = 18;
	
	public static final short FT_AGREEMENT = 19;
	
	public static final short FT_COLLECT = 20;
	
	public static final short FT_RETRANSMIT = 21;
	
	public static final short FT_RELEASE_WHEN_JOIN = 22;
	
	public static final short INSTALL_VIEW_ON_LEAVING = 23;
	
	public static final short SUSPECT_NOTIFY = 24;
	
	public static final short FT_AFTER_ABCAST_AGREEMENT = 25;
	/*
	public static final short ENTER_SERVICE = 0;

	public static final short LEAEVE_SERVICE = 1;

	public static final short REPLICATION_REQUEST = 2;

	public static final short ORDERING_REQUEST = 3;

	public static final short ORDERING_COORDINATE = 4;

	public static final short ORDERING_RESPONSE = 5;

	public static final short ORDERING_EXECUTE = 6;

	public static final short ORDERING_AGREEMENT = 7;

	public static final short REPLICATION_COORDINATE = 8;

	public static final short REPLICATION_AGREEMENT = 9;

	public static final short REPLICATION_EXECUTE = 10;

	public static final short ORDERING_NON_ORDERED_AGREEMENT = 11;

	public static final short ORDERING_DELAY_RESPONSE = 12;

	public static final short ELECTING_JOIN = 13;

	public static final short ELECTING_JOIN_RESPONSE = 14;

	public static final short ELECTING_DECIDING = 15;

	public static final short ELECTING_RECORDING = 16;

	public static final short ELECTING_NEW_SEQUENCER = 17;

	public static final short ELECTING_CHANGE_SEQUENCER = 18;

	public static final short REPLICATION_NESTED_EXECUTION = 19;

	public static final short REPLICATION_DELAY_RESPONSE = 20;

	public static final short ORDERING_VALIDATE_IS_BLOCK = 21;

	public static final short REPLICATION_RELEASE_CACHED_SEQUENCE = 22;

	public static final short ORDERING_REQUEST_RETRANSMISSION = 23;

	public static final short FAULT_TOLERANCE_CACHE_AGREED_SEQUENCE = 24;

	public static final short FAULT_TOLERANCE_RELEASE_SEQUENCE = 25;

	public static final short FAULT_TOLERANCE_TOLERATE_CRASH_ON_SEQUENCER = 26;
	
	public static final short FAULT_TOLERANCE_SKIP = 27;
	
	public static final short FAULT_TOLERANCE_CACHE_EXECUTED_SEQUENCE = 28;
	
	public static final short FAULT_TOLERANCE_AGREEMENT_ON_SEQUENCER_CRASH = 29;
	
	public static final short FAULT_TOLERANCE_COLLECTION_ON_SEQUENCER_CRASH = 30;
	
	public static final short FAULT_TOLERANCE_RELEASE_SEQUENCE_AFTER_EXECUTION = 31;
	
	public static final short SUSPECT_NOTIFY = 32;
	
	public static final short FAULT_TOLERANCE_REQUEST = 33;
	
	public static final short INSTALL_VIEW_ON_LEAVING = 34;
	*/
}
