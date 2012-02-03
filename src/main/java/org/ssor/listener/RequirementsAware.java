package org.ssor.listener;

import org.ssor.RequirementsAwareAdaptor;
import org.ssor.annotation.ProtocolListenerHelper;

@ProtocolListenerHelper(helperClass=org.ssor.listener.helper.RequirementsAwareHelper.class)
public interface RequirementsAware {

	public void setRequirementsAwareAdaptor(RequirementsAwareAdaptor adaptor);
}
