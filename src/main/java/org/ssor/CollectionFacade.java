package org.ssor;

import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * This is the facade that implements the underlying hash table used across all
 * the replication component. Alternative implementation can easily realized by
 * re-implementing this class, no extra concurrency controls are added here, it
 * is completely rely on the selected implementation.
 * 
 * @author Tao Chen
 * 
 */
public class CollectionFacade {

	/**
	 * 
	 * @return a concurrent hash map
	 */
	@SuppressWarnings("unchecked")
	public static Map getConcurrentHashMap() {
		return new java.util.concurrent.ConcurrentHashMap();
	}

	/**
	 * 
	 * @return a concurrent hash map with given initial size
	 */
	@SuppressWarnings("unchecked")
	public static Map getConcurrentHashMap(int size) {
		return new java.util.concurrent.ConcurrentHashMap(size);
	}

	/**
	 * 
	 * @return a priority queue
	 */
	@SuppressWarnings("unchecked")
	public static Queue getPriorityQueue() {
		return new PriorityQueue();
	}

	/**
	 * 
	 * @return a concurrent queue
	 */
	@SuppressWarnings("unchecked")
	public static Queue getConcurrentQueue() {
		return new ConcurrentLinkedQueue();
	}
}
