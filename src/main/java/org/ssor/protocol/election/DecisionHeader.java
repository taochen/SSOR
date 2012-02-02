package org.ssor.protocol.election;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.ssor.protocol.Header;

public class DecisionHeader extends Header {

	private Integer[] interests;

	public Integer[] getInterests() {
		return interests;
	}

	public DecisionHeader() {
	}

	public DecisionHeader(Integer[] interests) {
		super();
		this.interests = interests;
	}

	@Override
	public void readFrom(DataInputStream in) throws IOException,
			IllegalAccessException, InstantiationException {

		int length = 0;
		if ((length = in.readInt()) != 0) {
			interests = new Integer[length];
			for (int i = 0; i < length; i++)
				interests[i] = in.readInt();
		}
		readOuter(in);
	}

	@Override
	public void writeTo(DataOutputStream out) throws IOException {

		if (interests == null)
			out.writeInt(0);
		else {
			out.writeInt(interests.length);
			for (Integer regionNumber : interests)
				out.writeInt(regionNumber);
		}

		writeOuter(out);
	}
}
