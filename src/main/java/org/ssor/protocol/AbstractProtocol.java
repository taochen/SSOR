package org.ssor.protocol;

import org.slf4j.Logger;
import org.ssor.util.Util;

public abstract class AbstractProtocol implements Protocol{

	
	protected String name;
	
	protected Protocol lower;

	protected Protocol upper;
	
	protected ProtocolStack stack;
	
	protected Util util;
	
	protected int UUID_ADDR;
	
	protected boolean isConsensus = true;
	


	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}

	public void setLower(Protocol lower) {
		this.lower = lower;
	}

	public void setUpper(Protocol upper) {
		this.upper = upper;
	}
	
	
	
	
	protected Token doUp(short command, Token value){		
		return (upper == null)? value : upper.up(command, value);
	}
	
	protected Token doDown(short command, Token value){		
		return (lower == null)? value : lower.down(command,value);
	}
	

	public void setUtil(Util util) {
		this.util = util;
	}

	@Override
	public void init() {
		// TODO Auto-generated method stub

	}
	
	
	protected void trace(Logger logger,String command, String message){	
		logger.trace("Protocol command: (" + command + "); Details: (" + message + ")");
		
	}
	
	public ProtocolStack getStack() {
		return stack;
	}

	public void setStack(ProtocolStack stack) {
		this.stack = stack;
	}

	public void finishConsensus(){
		this.isConsensus = false;
	}
	
	public void setUUID_ADDR(int UUID_ADDR){
		this.UUID_ADDR = UUID_ADDR;
	}
	
	/**
	 * Use within new protocols if one does not sure what should be
	 * pass out after the new protocols. It basically replace the dataForNextProtocol
	 * attribute with a new token which can be used within next new
	 * protocol (the dataForNextProtocol attribute of the new token would still be the
	 * original dataForNextProtocol instance for the whole protocol stack).
	 * 
	 * @param original the original token from previous layer
	 * @param data the new data
	 * @param nextAction the new action
	 */
	protected void formNewToken (Token original, Object data, int nextAction) {
		Object originalData = original.getDataForNextProtocol();
		Token t = new Token (nextAction, originalData);
		t.setDataForNextProtocol(data);
		original.setDataForNextProtocol(t);
	}
	
	/**
	 * Use within new protocols if one does not sure what should be
	 * pass out after the new protocols. Everything should be the same
	 * if the original token is from via formNewToken function.
	 * 
	 * @param original
	 */
	protected void reformToken (Token original) {
		if(!(original.getDataForNextProtocol() instanceof Token)) return;
		
		Token t = (Token)original.getDataForNextProtocol();
		original.setDataForNextProtocol(t.getData());
	}
}
