package org.ssor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;

import org.ssor.util.Group;

/**
 * This class manages all nodes, in order to maintain a global view of region distribution
 * 
 * @author Tao Chen
 * 
 */
public class RegionDistributionManager {

	private Map<Integer, Node> registry;
	// For elect new sequencer
	private Queue<Node> queue;

	// This do not change after initialization, this would, service should have
	// uuid
	// even comes from different cloud/group
	private List<Integer> interestedRegions;

	// Used for maintain the queue, during election, make sure the right one on
	// the head
	private Object mutualLock = new Byte[0];

	// The view during the first election, used only once
	@SuppressWarnings("unchecked")
	protected Queue<Integer> view;

	// Used by region during the first REP's consensus
	@SuppressWarnings("unchecked")
	protected Queue<Integer> copiedView;

	private Group group;
	
	public boolean isHasNode(Integer key) {
		return registry.containsKey(key);
	}

	public Integer[] getInterestedRegions() {
		if (interestedRegions == null)
			return null;

		return interestedRegions.toArray(new Integer[interestedRegions.size()]);
	}

	@SuppressWarnings("unchecked")
	public RegionDistributionManager(Group group) {
		this.group = group;
		registry = CollectionFacade.getConcurrentHashMap(50);
		queue = CollectionFacade.getPriorityQueue();
	}

	public void register(Node node, int id) {
		if (id == node.getUUID_ADDR())
			node.setInterestedRegions(interestedRegions);

		if (!registry.containsKey(node.getUUID_ADDR())) {
			registry.put(node.getUUID_ADDR(), node);
			synchronized (mutualLock) {
				queue.add(node);
				// System.out.print(node.getUUID_ADDR()+" :
				// "+Environment.getUUID_ARRR(group) + " Now number " +
				// queue.size() + "\n");
			}
		}

	}

	public Node remove(Integer key) {
		Node node = registry.remove(key);
		//System.out.print("node: " + node + "\n");
		synchronized (mutualLock) {
			queue.remove(node);
		}
		return node;
	}

	public Node get(Integer key) {
		return registry.get(key);
	}

	public void clear() {
		registry.clear();
	}

	public void setInterests(int... regions) {
		if(interestedRegions == null)
		  interestedRegions = new ArrayList<Integer>();
		for (int i : regions)
			interestedRegions.add(i);
	}

	/**
	 * Sync by external on mutualLock
	 */
	public Node getOverheadSequencer(int id) {
		// synchronized(mutalLock){
		
		//System.out.print("Head of queue: " + queue.peek().getUUID_ADDR() + "\n");
		
		if (queue.peek().getUUID_ADDR() != id)
			return null;
		return queue.poll();
		// }
	}

	/**
	 * Sync by external on mutualLock
	 * 
	 * @param node
	 */
	public void putBack(Node node) {
		// synchronized(mutalLock){
		queue.add(node);
		// }
	}

	public void resort(Node node) {
		synchronized (mutualLock) {
			queue.remove(node);
			queue.add(node);

		}
	}

	public Object getMutalLock() {
		return mutualLock;
	}

	public Object getAddress(Integer key) {
		if (registry.get(key) == null)
			return null;
		return registry.get(key).getAddress();
	}

	public int getNumberOfNode() {
		return registry.size();
	}

	public boolean receive(Integer id) {
		return view.contains(id);
	}

	public boolean isCollectionEnough(Integer id) {

		view.remove(id);
		boolean result = view.size() == 0;
		if (result) {
			group.finishConsensus();
			view = null;
		}
		return result;
	}

	@SuppressWarnings("unchecked")
	public int getNextSequencer(Integer regionNumber) {

		// Fof keeping sync with other join operation (leaving has been sync by
		// view synchrony)
		synchronized (mutualLock) {

			Node[] nodes = queue.toArray(new Node[queue.size()]);
			Arrays.sort(nodes, ((PriorityQueue) queue).comparator());

			for (Node n : queue) {
				System.out.print("Order when get: " + n.getUUID_ADDR()
						+ "*********\n");
			}
			for (Node node : nodes) {
				if (node.isInterested(regionNumber)) {
					System.out.print("next: " + node.getUUID_ADDR()
							+ ", region: " + regionNumber
							+ " interested*****************\n");
					return node.getUUID_ADDR();
				}
			}

			System.out.print("next: " + queue.peek().getUUID_ADDR()
					+ ", region: " + regionNumber + " peek*****************\n");
			// If no one interests, then use the one that have minimum workload
			return queue.peek().getUUID_ADDR();
		}
	}

	@SuppressWarnings("unchecked")
	public void setView(Queue<Integer> view) {
		this.view = view;
		copiedView = CollectionFacade.getConcurrentQueue();
		copiedView.addAll(view);
	}

	public Queue<Integer> getView() {
		Queue<Integer> q = copiedView;
		copiedView = null;
		return q;
	}

	public void crashDuringConsensus(Integer id) {
		if (view == null)
			return;

		isCollectionEnough(id);

	}
	
	public boolean hasNode(Integer id){
		return registry.containsKey(id);
	}
}
