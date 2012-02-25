package org.ssor;


public interface RequirementsAwareAdaptor extends Adaptor{

	
	public ServiceManager getServiceManager();

	public RegionDistributionManager getRegionDistributionManager();
	
	public RegionDistributionSynchronyManager getRegionDistributionSynchronyManager();
}
