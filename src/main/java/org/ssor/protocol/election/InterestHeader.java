package org.ssor.protocol.election;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.ssor.protocol.Header;

public class InterestHeader extends Header {

	
	
	public InterestHeader() {
	}


	@Override
	public void readFrom(DataInputStream in) throws IOException,
			IllegalAccessException, InstantiationException {
		readOuter(in);
		
	}

	@Override
	public void writeTo(DataOutputStream out) throws IOException {
		writeOuter(out);
	}


	
}
