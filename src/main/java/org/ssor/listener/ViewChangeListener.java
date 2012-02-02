package org.ssor.listener;

import java.util.Collection;

import org.ssor.util.Callback;

public interface ViewChangeListener {


	public void memberLeave(int uuid, Collection<?> cache);
	
	public void currentNodeJoin(Collection<?> cache);
	
	public void initView(Collection<?> addresses, boolean isFirst, Callback callback);
}
