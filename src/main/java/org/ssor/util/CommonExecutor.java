package org.ssor.util;

import org.ssor.protocol.Message;

public class CommonExecutor {

	/**
	 * Used for release process no in different thread
	 * @param cal
	 * @param group
	 * @param msg
	 */
	public static Object releaseForReceiveInAnotherThread (Callback cal, Group group, Message msg) {
		final Object obj = (cal == null) ? null : cal.run();
		group.getRegionDistributionSynchronyManager().releaseMsgOnView(msg);
		return obj;
	}
}
