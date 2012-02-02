package org.ssor.protocol.replication;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.ssor.Region;
import org.ssor.AtomicService;
import org.ssor.ServiceManager;
import org.ssor.CompositeService;
import org.ssor.invocation.inteceptor.Interceptor;
import org.ssor.protocol.Header;
import org.ssor.util.Util;

public class RequestHeader extends Header {

	
	
	
	protected String sessionId;
	protected String service;
	protected boolean isNonOrdered = false;
	private transient Integer requester;
	
	public RequestHeader(String service, int requester, boolean isSessional) {
		super();
		this.service = service;
		this.requester = requester;
		if(isSessional)
		   sessionId = Interceptor.get();
		else
		   Interceptor.get();
	}



	public RequestHeader() {
		super();
	}



	public void setRequester(Integer requester) {
		this.requester = requester;
	}



	public RequestHeader(String sessionId, String service, int requester) {
		super();
		this.sessionId = sessionId;
		this.service = service;
		this.requester = requester;
	}



	public String getSessionId() {
		return sessionId;
	}



	public String getService() {
		return service;
	}


	public boolean isRequester(int id) {		
		return id == requester;
	}

	public boolean isNonOrdered() {
		return isNonOrdered;
	}



	public void setNonOrdered(boolean isNonOrdered) {
		this.isNonOrdered = isNonOrdered;
	}



	public void setService(String service) {
		this.service = service;
	}



	@Override
	public void readFrom(DataInputStream in) throws IOException,
			IllegalAccessException, InstantiationException {
		
		sessionId = Util.readString(in);
		service = ServiceManager.getServiceName(in.readInt());
		isNonOrdered = in.readBoolean();
		readOuter(in);
	}



	@Override
	public void writeTo(DataOutputStream out) throws IOException {
		
		
		Util.writeString(sessionId, out);
		out.writeInt(ServiceManager.getMagicNumber(service));
		out.writeBoolean(isNonOrdered);
		
		writeOuter(out);
	}



	public Integer getRequester() {
		return requester;
	}

	public Integer[] getNestedBelongingRegion(Set<Region> regions, List<Integer> waitingIndic, AtomicService atomicService){
		
		final CompositeService triggerService = (CompositeService)atomicService;
		final AtomicService[] services = triggerService.getServices();
		
		// The index that needs to be retransmited
		final List<Integer> list = new LinkedList<Integer>();
		for(Integer i : waitingIndic){
			if(regions.contains(services[i].getRegion()))
				list.add(i);
		}
		return list.toArray(new Integer[list.size()]);
	}

}
