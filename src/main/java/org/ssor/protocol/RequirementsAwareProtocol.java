package org.ssor.protocol;

import org.ssor.RegionDistributionManager;
import org.ssor.RequirementsAwareAdaptor;
import org.ssor.ServiceManager;
import org.ssor.RegionDistributionSynchronyManager;
import org.ssor.listener.RequirementsAware;

/**
 * This abstract class provide interfaces for the protocols which require state-level information
 * 
 * @author Tao Chen
 *
 */
public abstract class RequirementsAwareProtocol extends AbstractProtocol implements RequirementsAware{

	protected ServiceManager serviceManager;
	
	protected RegionDistributionManager regionDistributionManager;
	
	protected RegionDistributionSynchronyManager regionDistributionSynchronyManager;

	/**
	 * Verify if a service is associate with sessional conflict region
	 * @param name service name
	 * @return true indicate yes, otherwise false
	 */
	protected boolean isSessional(String name){
		return serviceManager.isSessional(name);
	}
	
	public void setRequirementsAwareAdaptor(RequirementsAwareAdaptor adaptor){
		serviceManager = adaptor.getServiceManager();
		regionDistributionManager = adaptor.getRegionDistributionManager();
		regionDistributionSynchronyManager = adaptor.getRegionDistributionSynchronyManager();
	}
}
