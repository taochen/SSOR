package org.ssor.listener;

import org.ssor.annotation.ProtocolListenerHelper;
import org.ssor.gcm.CommunicationAdaptor;

@ProtocolListenerHelper(helperClass=org.ssor.listener.helper.CommunicationListenerHelper.class)
public interface CommunicationListener {

	public void setCommunicationAdaptor(CommunicationAdaptor adaptor);
}
