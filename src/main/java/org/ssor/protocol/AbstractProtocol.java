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
}
