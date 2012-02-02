package org.ssor.protocol.election;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.ssor.util.Streamable;

public class Decision implements Streamable{

	private int region;
	private boolean isChnage;
	
	
	public Decision() {
		super();
	}
	public Decision(int region, boolean isChnage) {
		super();
		this.region = region;
		this.isChnage = isChnage;
	}
	public int getRegion() {
		return region;
	}
	public boolean isChnage() {
		return isChnage;
	}
	@Override
	public void readFrom(DataInputStream in) throws IOException,
			IllegalAccessException, InstantiationException {
		region = in.readInt();
		isChnage = in.readBoolean();
		
	}
	@Override
	public void writeTo(DataOutputStream out) throws IOException {
		out.writeInt(region);
		out.writeBoolean(isChnage);
		
	}
	
}
