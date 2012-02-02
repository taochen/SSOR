package org.ssor.protocol.replication;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.ssor.util.Streamable;
import org.ssor.util.Util;

/**
 * For delay execution
 * 
 * @author Tao Chen
 * 
 */
public class ResponsePacket implements Streamable {

	private Object[] nestedArguments;
	private Object[] returns;

	public ResponsePacket(int nSize, int rSize) {
		super();
		if (nSize != 0)
			this.nestedArguments = new Object[nSize];
		if (rSize != 0)
			this.returns = new Object[rSize];
	}

	public ResponsePacket() {
	}

	public void setReturns(Object[] returns) {
		this.returns = returns;
	}

	public void setNestedArguments(Object[] nestedArguments) {
		this.nestedArguments = nestedArguments;
	}

	public Object[] getNestedArguments() {
		return nestedArguments;
	}

	public Object[] getReturns() {
		return returns;
	}

	public String toString() {
		return "number of arguments group: "
				+ (nestedArguments == null ? "0" : String
						.valueOf(nestedArguments.length))
				+ ", number of return: "
				+ (returns == null ? "0" : String.valueOf(returns.length));
	}

	@Override
	public void readFrom(DataInputStream in) throws IOException,
			IllegalAccessException, InstantiationException {

		Object object = null;
		try {
			nestedArguments = ((object = Util.readArbitraryObject(in)) == null) ? null
					: (Object[]) object;
			returns = ((object = Util.readArbitraryObject(in)) == null) ? null
					: (Object[]) object;
		} catch (Throwable e) {
			e.printStackTrace();
			throw new IOException(e);
		}

	}

	@Override
	public void writeTo(DataOutputStream out) throws IOException {
		try {
			Util.writeArbitraryObject(out, nestedArguments);
			Util.writeArbitraryObject(out, returns);
		} catch (Throwable e) {
			e.printStackTrace();
			throw new IOException(e);
		}

	}

}
