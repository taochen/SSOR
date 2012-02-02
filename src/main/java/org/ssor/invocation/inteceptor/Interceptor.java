package org.ssor.invocation.inteceptor;

public class Interceptor {

	private static ThreadLocal<String> localSession = new ThreadLocal<String>();	
	
	public static void set(String sessionId){
		localSession.set(sessionId);
	}
	
	public static String get(){
		
		if(localSession.get() == null)
		    return null;
		String sessionId = localSession.get();
		localSession.remove();
		return sessionId;
	}
}
