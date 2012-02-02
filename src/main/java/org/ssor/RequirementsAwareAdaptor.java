package org.ssor;


public interface RequirementsAwareAdaptor {

	
	public ServiceManager getServiceManager();

	public RegionDistributionManager getRegionDistributionManager();
	
	public RegionDistributionSynchronyManager getRegionDistributionSynchronyManager();
}
