package org.ssor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.ssor.protocol.election.Decision;

/**
 * Some FT has been sync by VS, thus not extra sync needed
 * 
 * @author Tao Chen
 * 
 */
public class Node implements Comparable<Node> {

	// Regions ID that it interested, this do not change after initialization
	private List<Integer> interests;

	// Regions ID that it responsible for, this may be changed
	private Set<Integer> regions;

	// Regions ID that it responsible for, but not complete yet
	private Queue<Integer> uncompletedRegionsWhenJoining;
	
	// <node id, list of region> for the crash nodes, and this node was elected as the new sequencer
	// of their regions, but not complete yet
	private Map<Integer, Queue<Integer>> uncompletedNodeAndRegionWhenCrash;
	
	private int UUID_ADDR;

	private Object address;

	@SuppressWarnings("unchecked")
	public Node(Integer UUID_ADDR, Object address) {
		super();
		this.UUID_ADDR = UUID_ADDR;
		this.address = address;
		regions = new HashSet<Integer>();
		uncompletedRegionsWhenJoining = CollectionFacade.getConcurrentQueue();
		uncompletedNodeAndRegionWhenCrash = CollectionFacade.getConcurrentHashMap();
	}

	public synchronized void addRegion(Integer region) {
		regions.add(region);
	}

	public synchronized void removeRegion(Integer region) {
		regions.remove(region);
	}

	public Integer[] getInterestedRegion() {

		if (interests == null)
			return null;

		return interests.toArray(new Integer[interests.size()]);
	}

	public Decision[] getResponsibleRegionsWithDecision() {

		final Decision[] decisions = new Decision[regions.size()];
		final Iterator<Integer> itr = regions.iterator();
		for (int i = 0; i < regions.size(); i++)
			decisions[i] = new Decision(itr.next(), false);
		
		return decisions;
	}

	public boolean isHasResponsibleRegion() {
		return regions.size() != 0;
	}

	public synchronized Decision[] findInterestedRegion(Integer[] anotherInterests) {
		List<Decision> list = new ArrayList<Decision>();
		List<Integer> interestsList = Arrays.asList(anotherInterests);
		for (Integer i : regions) {

			if (!interestsList.contains(i)
					|| (interests != null && interests
							.contains(i)))
				list.add(new Decision(i, false));
			// If interest by new node, and not an interest of current node
			else
				list.add(new Decision(i, true));

		}

		return list.size() == 0 ? null : list.toArray(new Decision[list.size()]);
	}

	public synchronized Decision[] splitRegion() {

		// If only one, then only reply but not change
		if (isResponsibleForOnlyOneSequncer())
			return new Decision[] { new Decision(regions.iterator().next(), false) };

		int length = regions.size() / 2;
		List<Decision> list = new ArrayList<Decision>();
		int assign = 0;
		for (Integer i : regions) {

			// Do not change session sequencer during random sequencer balancing
			if (i == SessionRegion.SESSION_REGION)
				list.add(new Decision(i, false));
			// Do not change sequencer if this is interested by it
			else if (interests != null && interests.contains(i))
				list.add(new Decision(i, false));
			// Change if this is not interested by current node, and it
			// responsible for more than 1 regions
			else {
				// Can not add exceed half
				if (assign < length) {
					list.add(new Decision(i, true));
					assign++;
				} else
					list.add(new Decision(i, false));
			}
		}

		return list.toArray(new Decision[list.size()]);
	}

	@Override
	public int compareTo(Node node) {
		
		if (this.regions.size()  > node.regions.size())
			return -1;
		else if (this.regions.size() < node.regions.size())
			return 1;
		else {
			
			if(this.regions.contains(SessionRegion.SESSION_REGION))
				return 1;
			else if(node.regions.contains(SessionRegion.SESSION_REGION))
				return -1;
			
			return this.UUID_ADDR > node.UUID_ADDR ? -1 : 1;
		}
	}

	public boolean isCurrentNode(int id) {
		return id == UUID_ADDR;
	}

	public Object getAddress() {
		return address;
	}

	public void setInterestedRegions(List<Integer> interestedRegions) {
		this.interests = interestedRegions;
	}

	public void setInterestedRegions(Integer[] interests) {
		if(interests != null)
		  this.interests = Arrays.asList(interests);
	}

	public int getUUID_ADDR() {
		return UUID_ADDR;
	}

	private boolean isResponsibleForOnlyOneSequncer() {
		return regions.size() == 1;
	}

	public boolean isInterested(Integer number) {
		if (interests == null)
			return false;
		return interests.contains(number);
	}

	@SuppressWarnings("unchecked")
	public void addNewResposibleRegion(Integer id, Integer regionNumber) {
		
		if(uncompletedNodeAndRegionWhenCrash.containsKey(id)){
			uncompletedNodeAndRegionWhenCrash.get(id).add(regionNumber);
		}else {
            Queue<Integer> queue = CollectionFacade.getConcurrentQueue();
			queue.add(regionNumber);
			
			uncompletedNodeAndRegionWhenCrash.put(id, queue);
		}
		
		
	}

	public void addNewResposibleRegion(Decision[] decisions) {
		for (Decision decision : decisions) {

			if (decision.isChnage())
				uncompletedRegionsWhenJoining.add(decision.getRegion());
		}
	}

	public void clearUncompletedRegionWhenJoining(Integer regionNumber) {
		uncompletedRegionsWhenJoining.remove(regionNumber);
	
	}
	
	public void clearUncompletedNodeWhenCrash(Integer uuid) {
		uncompletedNodeAndRegionWhenCrash.remove(uuid);
	
	}
	
	public Integer[] getUncompletedRegion(){
			
		return uncompletedRegionsWhenJoining.toArray(new Integer[uncompletedRegionsWhenJoining
			                          					.size()]);	
	}
	
	public boolean isHasUncompletedNodeWhenCrash(){
		return uncompletedNodeAndRegionWhenCrash.size() == 0;	
	}
	
	public void releaseViewsWhenThisNodeCrash(RegionDistributionSynchronyManager manager){
		
		for(Map.Entry<Integer, Queue<Integer>> entry : uncompletedNodeAndRegionWhenCrash.entrySet()){
			// Add to this node, so that they can be reassign when this node crash, since they are
			// considered as this node's regions
			for(Integer regionNumber : entry.getValue())
			  addRegion(regionNumber);
			
			manager.releaseOnCrashDuringConsensus(entry.getKey());
		}
		
		uncompletedNodeAndRegionWhenCrash.clear();
	}

	public boolean isHasUncompletedRegionWhenJoining() {
		return uncompletedRegionsWhenJoining.size() != 0;
	}
	
	public boolean isLocalUncompleteRegion(Integer number) {
		return uncompletedRegionsWhenJoining.contains(number);
	}

	public Integer[] getResponsibleRegions() {
		return regions.toArray(new Integer[regions.size()]);
	}

	public String toString(){
		return "(Node UUID: " + String.valueOf(UUID_ADDR) + ")";
	}
}
