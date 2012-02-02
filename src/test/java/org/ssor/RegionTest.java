package org.ssor;

import java.util.List;
import java.util.Queue;

import junit.framework.TestCase;

public class RegionTest extends TestCase {

	
    public Sequence[] testExtractSkippedSequences(){
    	Region region = new Region(1, Region.CONFLICT_REGION);
    	setView(region);
    	region.addCollectedSequences(null, 1111, new Sequence[]{new Sequence(10, null), new Sequence(14, -1),  new Sequence(14, -1)}, new Sequence(10, null));
    	region.addCollectedSequences(null, 2222, new Sequence[]{new Sequence(14, 4),new Sequence(17, -1)}, new Sequence(12, null));
    	//region.addCollectedSequences(null, 2222, new Sequence[]{new Sequence(15, null)}, new Sequence(14, null));
    	
    	if(region.isSequenceCollectionFinished()){
    		List<Sequence> list = region.extractSkippedSequences();
    		for(Sequence seq : list)
    			System.out.print(seq + "\n");
    		
    		
    		return list.toArray(new Sequence[list.size()]);
    	}
    	
    	
		return null;
	}
	
	public void testFaultTolerance(){
		
		Sequence[] skip = testExtractSkippedSequences();
		/*
		Region region = new Region(1, Region.CONFLICT_REGION);
		region.expectedSeqno = 12;
		region.exceptedConcurrentno = 3;
		Sequence[] s = new Sequence[]{new Sequence(14, 4),new Sequence(14, -1),  new Sequence(14, -1),new Sequence(17, -1)};
		for(Sequence se : skip)
		region.addSkippedSequence(se);
		
		int decision = 0;
		for(Sequence se : s){
			
			if((decision = region.isExecutable(null, se,
					new AtomicService(s.toString()))) <= 0){
						
				
				System.out.print("accept: " + se + "\n");
				
						if(decision == 0)
							region.increaseSeqno(null);
						else
							region.increaseConcurrentno(null);
						
					}
		}*/
	}
	
	private void setView(Region region){
		Queue queue = CollectionFacade.getConcurrentQueue();
		queue.add(1111);
		queue.add(2222);
		region.setConsensusView(queue);
	}
}
