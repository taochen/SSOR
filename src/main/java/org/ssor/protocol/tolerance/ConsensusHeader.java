package org.ssor.protocol.tolerance;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.ssor.Region;
import org.ssor.Sequence;
import org.ssor.SessionRegion;
import org.ssor.protocol.Header;
import org.ssor.util.Triple;
import org.ssor.util.Util;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * The header used during decentralized consensus, (normally due to the crash of
 * sequencer, and election of new sequencer needed)
 * 
 * @author Tao Chen
 * @version
 */
public class ConsensusHeader extends Header {

	// Tuple<region number, latest sequence,list of sequence>
	private Triple<Integer, Sequence, Sequence[]>[] array;

	// Tuple<sessiond ID, latest sequence, list of sequence>
	private Triple<String, Sequence, Sequence[]>[] sessionalArray;

	private transient int load = 0;

	public ConsensusHeader() {
	}

	@SuppressWarnings("unchecked")
	public ConsensusHeader(Integer[] regions) {
		
		
		for(Integer i : regions){
			if(i == SessionRegion.SESSION_REGION){
				array = new Triple[regions.length - 1];
				break;
			}
				
		}
		if(array == null)
		 array = new Triple[regions.length];
	}

	@SuppressWarnings("unchecked")
	@Override
	public void readFrom(DataInputStream in) throws IOException,
			IllegalAccessException, InstantiationException {

		int size = in.readInt();
		array = new Triple[size];

		Integer region = null;
		Sequence latest = null;
		int length = 0;
		Sequence[] sequences = null;
		FaultySequence fs = null;
		for (int i = 0; i < size; i++) {

			region = in.readInt();
			latest = new Sequence();
			latest.readFrom(in);
			length = in.readInt();
			sequences = new Sequence[length];
			for (int j = 0; j < length; j++) {
				fs = new FaultySequence();
				fs.readFrom(in);
				sequences[j] = fs.getSequence();
			}

			array[i] = new Triple<Integer, Sequence, Sequence[]>(region, latest,
					sequences);
		}

		if (in.readShort() != -1) {
			size = in.readInt();
			sessionalArray = new Triple[size];
			String session = null;
			for (int i = 0; i < size; i++) {

				session = Util.readString(in);
				latest = new Sequence();
				latest.readFrom(in);
				length = in.readInt();
				sequences = new Sequence[length];
				for (int j = 0; j < length; j++) {
					sequences[j] = new Sequence();
					sequences[j].readFrom(in);
				}

				sessionalArray[i] = new Triple<String, Sequence, Sequence[]>(
						session, latest, sequences);
			}
		}

		readOuter(in);
	}

	@Override
	public void writeTo(DataOutputStream out) throws IOException {
		out.writeInt(array.length);
		Sequence[] sequences = null;
		for (Triple<Integer, Sequence, Sequence[]> triple : array) {

			out.writeInt(triple.getVal1());
			triple.getVal2().writeTo(out);
			sequences = triple.getVal3();
			out.writeInt(sequences.length);
			for (Sequence sequence : sequences) {
				// Write through faulty sequence, so that sessionId and service
				// name can be written
				new FaultySequence(sequence).writeTo(out);
			}

		}

		if (sessionalArray == null)
			out.writeShort(-1);
		else {
			out.writeShort(0);
			out.writeInt(sessionalArray.length);
			for (Triple<String, Sequence, Sequence[]> triple : sessionalArray) {

				Util.writeString(triple.getVal1(), out);
				triple.getVal2().writeTo(out);
				sequences = triple.getVal3();
				out.writeInt(sequences.length);
				for (Sequence sequence : sequences) {
					sequence.writeTo(out);
				}
			}
		}

		writeOuter(out);

	}

	/**
	 * This is not thread safe
	 * 
	 * @param region
	 * @param sequences
	 */
	public void addTripleValue(Region region, Sequence[] sequences) {
		if (region instanceof SessionRegion)
			setSessionalSequences((SessionRegion) region, sequences);
		else {
			array[load] = new Triple<Integer, Sequence, Sequence[]>(region
					.getRegion(), region.getExpectedSequence(), sequences);
			load++;
		}
	}

	public Triple<Integer, Sequence, Sequence[]>[] getTripleArray() {
		return array;
	}
	
	public Triple<String, Sequence, Sequence[]>[] getSessionalTripleArray() {
		return sessionalArray;
	}



	@SuppressWarnings("unchecked")
	private void setSessionalSequences(SessionRegion region,
			Sequence[] sequences) {
		final Map<String, List<Sequence>> map = new HashMap<String, List<Sequence>>();
		for (Sequence seq : sequences) {

			if (!map.containsKey(seq.getSessionId())) {

				map.put(seq.getSessionId(), new LinkedList<Sequence>());
			}

			map.get(seq.getSessionId()).add(seq);
		}

		sessionalArray = new Triple[map.size()];
		int i = 0;
		String session = null;
		for (List<Sequence> list : map.values()) {
			session = list.get(0).getSessionId();
			sessionalArray[i] = new Triple<String, Sequence, Sequence[]>(
					session, region.getExpectedSequence(session), list
							.toArray(new Sequence[list.size()]));
			i++;
		}
	}
}
