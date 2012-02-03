package org.ssor.protocol;

import java.util.Map;

import org.ssor.annotation.ProtocolListenerHelper;

@ProtocolListenerHelper(helperClass=org.ssor.listener.helper.ProtocolSharableInstancesHelper.class)
public interface ProtocolSharableInstances {

	public void setCachedSentMessages(Map<String, Message> sentMessages);
}
