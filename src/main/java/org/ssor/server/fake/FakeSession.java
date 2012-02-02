package org.ssor.server.fake;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpSession;
import javax.servlet.http.HttpSessionContext;

/**
 * All actions on this wrapper would be address on the original as well, in order to trigger listeners
 * @author Administrator
 *
 */
@SuppressWarnings("deprecation")
public class FakeSession implements HttpSession {
	
	private final Map<String, Object> attributes = new HashMap<String, Object>();
	private long lastActiveTime;
	private HttpSession original;

	
	
	public FakeSession(HttpSession original) {
		super();
		this.original = original;
	}

	@Override
	public Object getAttribute(String name) {
		return attributes.get(name);
	}

	@Override
	public Enumeration<?> getAttributeNames() {
		return original.getAttributeNames();
	}

	@Override
	public long getCreationTime() {
		return original.getCreationTime();
	}

	@Override
	public String getId() {
		return original.getId();
	}

	@Override
	public long getLastAccessedTime() {
		return original.getLastAccessedTime();
	}

	@Override
	public int getMaxInactiveInterval() {
		return original.getMaxInactiveInterval();
	}

	@Override
	public ServletContext getServletContext() {
		return original.getServletContext();
	}

	@SuppressWarnings("deprecation")
	@Override
	public HttpSessionContext getSessionContext() {
		return original.getSessionContext();
	}

	@Override
	public Object getValue(String name) {
		return attributes.get(name);
	}

	@SuppressWarnings("deprecation")
	@Override
	public String[] getValueNames() {
		return original.getValueNames();
	}

	@Override
	public void invalidate() {
		original.invalidate();

	}

	@Override
	public boolean isNew() {
		return original.isNew();
	}

	@Override
	public void putValue(String name, Object value) {
		attributes.put(name, value);
		//original.putValue(name, value);
	}

	@Override
	public void removeAttribute(String name) {
		attributes.remove(name);
		//original.removeAttribute(name);

	}

	@Override
	public void removeValue(String name) {
		attributes.remove(name);
		//original.removeValue(name);
	}

	@Override
	public void setAttribute(String name, Object value) {
		attributes.put(name, value);
		//original.setAttribute(name, value);

	}

	@Override
	public void setMaxInactiveInterval(int interval) {
		original.setMaxInactiveInterval(interval);

	}

	
	public boolean isCanDeleted(long timeout){
		return System.currentTimeMillis() - lastActiveTime > timeout && original == null;
	}
	
	public void resetActivation(){
		lastActiveTime = System.currentTimeMillis();
	}
}
