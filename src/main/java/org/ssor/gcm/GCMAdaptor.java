package org.ssor.gcm;

import org.ssor.invocation.InvocationCallback;
import org.ssor.listener.ViewChangeListener;
import org.ssor.protocol.Message;
import org.ssor.util.Group;
import org.ssor.util.Util;

public interface GCMAdaptor extends CommunicationAdaptor, ViewChangeListener {

	public void init(Object object) throws Throwable;

	public void processPriorReceive(final Message message, final Object src);

	public void setUtil(Util util);

	public boolean isNeedNewThread(Message message);

	public void blockRelease(Message message, Object address);

	public void setPath(String path);

	public void setUDP(boolean isUDP);

	public void setCallback(InvocationCallback callback);

	public Group getGroup();
	
}
