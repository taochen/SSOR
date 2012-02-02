package org.ssor.protocol;

import org.ssor.util.Util;


public interface Protocol {

	public String getName();

	public void setName(String name);

	public void setLower(Protocol lower);

	public void setUpper(Protocol upper);

	public Token up(short command, Token value);

	public Token down(short command, Token value);

	public ProtocolStack getStack();

	public void setStack(ProtocolStack stack);
	
	public void setUtil(Util util);
	
	public void finishConsensus();
	
	public void setUUID_ADDR(int UUID_ADDR);
	
	public void init();

}
