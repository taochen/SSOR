package org.ssor.util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Implementations of Streamable can add their state directly to the output
 * stream, enabling them to bypass costly serialization
 * 
 * @author Tao Chen
 */
public interface Streamable {

	/**
	 * Write the entire state of the current object (including superclasses) to
	 * outstream. Note that the output stream should not be closed
	 */
	void writeTo(DataOutputStream out) throws IOException;

	/**
	 * Read the state of the current object (including superclasses) from
	 * instream Note that the input stream should not be closed
	 */
	void readFrom(DataInputStream in) throws IOException,
			IllegalAccessException, InstantiationException;
}
