package org.ssor;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.ssor.protocol.Message;
import org.ssor.protocol.replication.RequestHeader;
import org.ssor.protocol.replication.ResponseHeader;

public class ConcurrentTest {

	//public static ConcurrentQueue queue = new ConcurrentQueue(false);
	public static Map map = CollectionFacade.getConcurrentHashMap(100);
	
	public static int no = 1;
	public static Integer i = 0;
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		ConcurrentTest t = new ConcurrentTest();
		TestClass tt = t. new TestClass();
		for (int i = 0; i < 1000; i++){
			if(i%10==0)
				tt.print();
			
			t.new ThreadTest(tt).start();
			}
		
		
		
	}
	private  class TestClass{
		
		public int i = 0;
		
		public void print(){
	
				i++;
			
		
		}
	}
	
	private class ThreadTest extends Thread {

		private File file;
		private int i;
		private List<String> list = new ArrayList<String>();
		TestClass tt;
		public ThreadTest(File file, int i) {
			this.file = file;
			this.i = i;
		}

		public ThreadTest(int i) {

			this.i = i;
		}
		
		public ThreadTest(TestClass tt) {

			this.tt = tt;
		}

		public void run() {
			
			/*
			Message m1 = new Message(null, null);
			RequestHeader rh1 = new RequestHeader(null);
			ResponseHeader rh11 = new ResponseHeader(null, i);
			rh1.setOuter(rh11);
			m1.setHeader(rh1);
			*/
			synchronized(tt){

				System.out.print(tt.i + " print\n");
				
				
				
			}
			
			 /*
			synchronized(ConcurrentTest.i){
				ConcurrentTest.i++;
			if(ConcurrentTest.i == ConcurrentTest.no){
			   
				while(!queue.isEmpty()){
			 	//System.out.print(((ResponseHeader)queue.pop().getHeader().getOuter()).getTimestamp()+"\n");
				  
			   }
				
			}
		}*/
        }

	}
	

}
