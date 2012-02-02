package org.ssor.util;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.ssor.CollectionFacade;
import org.ssor.Sequence;
import org.ssor.protocol.replication.abcast.SequenceVector;

/**
 * A wrapper of sequence queue, simple concurrent tasks, the sequences holding
 * here may or may not belong to the same region
 * 
 * This is mainly used when cache of sequence is needed
 * 
 * @author Tao Chen
 * 
 */
@SuppressWarnings("unchecked")
public class SequenceLinkedList {

	Queue<Sequence> queue = CollectionFacade.getConcurrentQueue();

	/**
	 * Add sequence, the incoming parameter can be array of sequence,
	 * OrderVector or an single sequence.
	 * 
	 * @param object
	 *            the incoming data
	 */
	public void add(Object object) {
		if (object instanceof Sequence) {

			queue.add((Sequence) object);

		} else if (object instanceof Sequence[]) {

			final Sequence[] sequences = (Sequence[]) object;
			for (Sequence seq : sequences)
				queue.add(seq);

		} else {
			final SequenceVector[] vectors = (SequenceVector[]) object;

			for (SequenceVector vector : vectors)
				queue.add(vector.getSequence());

		}
	}

	/**
	 * Remove the corresponding sequence (non-concurrent).
	 * 
	 * @param sequence
	 *            the sequence
	 */
	public void remove(Sequence sequence) {

		/*
		 * We use iteration here, can be optimized
		 */
		Iterator<Sequence> itr = queue.iterator();
		Sequence seq = null;
		while (itr.hasNext()) {
			seq = itr.next();
			if (seq.isEquals(sequence)) {
				queue.remove(seq);
				
				break;
			}

		}

	}

	/**
	 * Remove all the concurrent sequence ahead, when the subsequent sequential
	 * sequence comes in this is needed in order to estimate the last state and
	 * missing sequence in case of sequencer crash.
	 * 
	 * @param sequence
	 *            the next sequential sequence
	 */
	public void removeWithConcurrentSequences(Sequence sequence) {

		/*
		 * We use iteration here, can be optimized
		 */
		Iterator<Sequence> itr = queue.iterator();
		Sequence seq = null;
		while (itr.hasNext()) {
			seq = itr.next();
			if (seq.isEquals(sequence)
					|| (seq.getConcurrentno() != null && (seq
							.getConcurrentno() == -1
							&& seq.getSeqno() + 1 == sequence
									.getSeqno() && sequence
							.isSessionEquals(seq)))) {

				queue.remove(seq);

			}

		}

	}

	public int size() {
		return queue.size();

	}

	public Sequence poll() {
		return queue.poll();
	}

	/**
	 * Extract all sequences when consensus needed, note that the ones that have
	 * been marked as 'received' would not be included.
	 * 
	 * @return the list of sequence that have not been marked previously
	 */
	public Sequence[] getAllForConsensus() {
		final List<Sequence> list = new LinkedList<Sequence>();

		Iterator<Sequence> itr = queue.iterator();
		Sequence seq = null;
		while (itr.hasNext()) {
			seq = itr.next();
			if (!seq.isProposedForConsensus()){
				System.out.print("acquired: " + seq +"\n");
				
				list.add(seq);
			}

		}

		return list.toArray(new Sequence[list.size()]);

	}

	/**
	 * Mark all the current contained sequences as 'proposed' this is correct
	 * since no new sequence are cache before the consensus finish. Note that
	 * this is essential since the new sequencer may crash before the release of
	 * the sequences that proposed for the previous crash, in which case the
	 * sequence may be redundantly proposed.
	 */
	public void markSequence() {

		Iterator<Sequence> itr = queue.iterator();
		Sequence seq = null;
		while (itr.hasNext()) {
			seq = itr.next();
			// if (regionNumber == seq.getRegionNumber()) {
			seq.isProposedForConsensus(true);

			// }

		}
	}

	/**
	 * Remove the sequence that belongs to a specific region, this is only used
	 * in case the contained sequences are from different regions.
	 * 
	 * @param regionNumber
	 *            the unique region number
	 */
	public void removeByRegionNumber(int regionNumber) {

		Iterator<Sequence> itr = queue.iterator();
		Sequence seq = null;
		while (itr.hasNext()) {
			seq = itr.next();
			if (seq.getRegionNumber() == regionNumber) {

				queue.remove(seq);

			}

		}

	}

	public void clear() {
		queue.clear();
	}

}
