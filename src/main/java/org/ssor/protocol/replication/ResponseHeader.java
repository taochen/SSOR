package org.ssor.protocol.replication;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.ssor.Sequence;
import org.ssor.protocol.Header;
import org.ssor.protocol.replication.abcast.SequenceVector;

public class ResponseHeader extends Header {

	public static final short NULL = -1;
	public static final short SEQUENCE = 0;
	public static final short SEQUENCE_ARRAY = 1;
	public static final short VECTOR = 2;

	// May be array of OrderVector or simply Sequence or Sequence[]
	private Object timestamp;

	private transient Integer sequencer;
	
	public ResponseHeader() {
		super();
	}

	@SuppressWarnings("unchecked")
	public ResponseHeader(Object timestamp) {
		super();
		this.timestamp = timestamp;
	}


	public Object getTimestamp() {
		return timestamp;
	}

	public int compare(ResponseHeader another) {

		int thisTimestamp = (Integer) timestamp;
		int anotherTimestamp = (Integer) another.timestamp;
		if (thisTimestamp > anotherTimestamp)
			return 1;
		else if (thisTimestamp < anotherTimestamp)
			return -1;
		else
			return 0;
	}

	

	public void setTimestamp(Object timestamp) {		
		this.timestamp = timestamp;
	}

	@Override
	public void readFrom(DataInputStream in) throws IOException,
			IllegalAccessException, InstantiationException {

		short type = in.readShort();
		switch (type) {

		case NULL:{
			timestamp = null;
			break;
		}
		
		case SEQUENCE: {
			timestamp = new Sequence();
			((Sequence)timestamp).readFrom(in);
			break;
		}
		case SEQUENCE_ARRAY: {
			int length = in.readInt();
			Sequence[] array = new Sequence[length];
			for (int i = 0; i < length; i++){
				array[i] = new Sequence();
				array[i].readFrom(in);
			}

			timestamp = array;
			break;
		}
		case VECTOR: {
			int length = in.readInt();
			SequenceVector[] vectors = new SequenceVector[length];

			for (int i = 0; i < length; i++) {
				vectors[i] = new SequenceVector();
				vectors[i].readFrom(in);
			}

			timestamp = vectors;
			break;
		}

		}
		/*
		 * try { timestamp = Util.readArbitraryObject(in); } catch (Exception e) {
		 * throw new IOException(e); }
		 */


		readOuter(in);

	}

	@Override
	public void writeTo(DataOutputStream out) throws IOException {

		/*
		 * try { Util.writeArbitraryObject(out, timestamp); } catch (Exception
		 * e) { throw new IOException(e); }
		 */

		if (timestamp instanceof Sequence) {
			out.writeShort(SEQUENCE);
			((Sequence) timestamp).writeTo(out);
		// For the last broadcasting of composed trigger service
		} else if (timestamp instanceof Sequence[]) {
			out.writeShort(SEQUENCE_ARRAY);
			Sequence[] array = (Sequence[]) timestamp;
			int length = array.length;
			out.writeInt(length);
			for (int i = 0; i < length; i++)
				array[i].writeTo(out);
		} else if (timestamp instanceof SequenceVector[]) {
			out.writeShort(VECTOR);
			SequenceVector[] vector = (SequenceVector[]) timestamp;
			int length = vector.length;
			out.writeInt(vector.length);
			for (int i = 0; i < length; i++)
				vector[i].writeTo(out);		
		} else
			out.writeShort(NULL);

		

		

		writeOuter(out);
	}

	public Integer getSequencer() {
		return sequencer;
	}

	public void setSequencer(Integer sequencer) {
		this.sequencer = sequencer;
	}

}
