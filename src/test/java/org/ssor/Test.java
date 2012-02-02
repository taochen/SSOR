package org.ssor;

import java.io.File;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;

import org.ssor.protocol.Message;
import org.ssor.protocol.replication.RequestHeader;
import org.ssor.util.Util;


public class Test {

	public static Integer counter = -1;
	public static List<Integer> list = new ArrayList<Integer>();

	public static long time;
	public static int no = 2000;
	static ServiceManager manager ;
	public static void main(String[] args) {
		
		/*
	manager.register(new Service("org.ssor.Service.isSequencer",new Region(1, Region.APPLICATION_SCOPE), null));
		
		Test t = new Test();
		//manager.register("1", new Service("1",new Region(0,2)));
		
		time = System.currentTimeMillis();
	int[][] e = new int[1][2];
		Object[] i = new Integer[2][3] ;
		Object i1 = new Object[2] ;
			try {
				Message m = new Message(new RequestHeader("org.ssor.Service.isSequencer"), new Object[]{1,1}, true);
				System.out.print(Util.objectToByteBuffer(m).length);
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}*/
	
		//SQLParserTest.counter.notifyAll();
		//for (int i = 0; i < no; i++)
			//t.new ThreadTest(i).start();
		/*
		Node n = null;
		Queue q = CollectionFacade.getPriorityQueue();
		n = new Node(2, new Object());
		n.addRegion(2);
		n.addRegion(-1);
		n.addRegion(3);
		q.add(n);
		n = new Node(1, new Object());
		n.addRegion(1);
		q.add(n);
		
		Iterator itr = q.iterator();
		while(itr.hasNext())
			System.out.print(itr.next() + "***\n");*/
		String i = "100";
		System.out.print(i.hashCode() + "***\n");
	}

	
	
	public static class Static{
	
		private int i;
		public Static(int i){
			this.i = i;
		}
		
		public void get(){
			System.out.print(i+"\n");
		}
	}	
	private class ThreadTest extends Thread {
	

		private File file;
		private int i;
		private List<String> list = new ArrayList<String>();

		public ThreadTest(File file, int i) {
			this.file = file;
			this.i = i;
		}

		public ThreadTest(int i) {

			this.i = i;
		}

		public void run() {

			/*
			 * try { BufferedReader in = new BufferedReader(new
			 * InputStreamReader( new FileInputStream(new File(
			 * "D:/course-project/ICW-Backup/commonPassword.txt")))); String
			 * line; while ((line = in.readLine()) != null){ //list.add(line);
			 * if(line.equals("1234qwer")) list.add(line); }
			 * 
			 * System.out.print(list.size() + "****" + i +"\n");
			 * 
			 * in.close(); } catch (Exception e) { // TODO Auto-generated catch
			 * block e.printStackTrace(); }
			 */
			try {
				
				AtomicService service = manager.get("1");
				Object lock = service.getMutualLock("1");
				synchronized (lock) {
				while(service.isExecutable("1",null)>0){
				
						//System.out.print(i + " stopping\n");
						//SQLParserTest.list.add(i);
					lock.wait();
					
					
					
					}
					
				//System.out.print("****" + i + "\n");
				//service.increase("1");
				System.out.print("*counter " + i  + "\n");
			
				//if(SQLParserTest.list.contains(i))
					//SQLParserTest.list.remove(i);
				
				
				//if(SQLParserTest.list.size() != 0){
					//System.out.print("*size " + SQLParserTest.list.size()  + "\n");
				if(i == no-1)
					System.out.print( System.currentTimeMillis() - Test.time   + "\n");
				
				lock.notifyAll();
				}
				 //SQLParserTest.object.notify();
				
				
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}
	
}
