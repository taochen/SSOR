package org.ssor.protocol.tolerance;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.ssor.Sequence;
import org.ssor.util.Streamable;
import org.ssor.util.Util;
public class FaultySequence implements Streamable {

	
	private Sequence sequence;
	
	public FaultySequence() {
		super();
	}

	
	
	public FaultySequence(Sequence sequence) {
		super();
		this.sequence = sequence;
	}



	@Override
	public void readFrom(DataInputStream in) throws IOException,
			IllegalAccessException, InstantiationException {
		
		sequence = new Sequence();
		sequence.setRegionNumber(in.readInt());
		sequence.readFrom(in);
		sequence.setSessionId(Util.readString(in));
	}

	@Override
	public void writeTo(DataOutputStream out) throws IOException {
		out.writeInt(sequence.getRegionNumber());
		sequence.writeTo(out);
		Util.writeString(sequence.getSessionId(), out);

	}



	public Sequence getSequence() {
		return sequence;
	}


}
