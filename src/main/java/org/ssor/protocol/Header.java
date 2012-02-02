package org.ssor.protocol;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.ssor.conf.ClassConfigurator;
import org.ssor.util.Streamable;

public abstract class Header implements Streamable, Cloneable {

	public static final short EOH = -1;

	protected Header outer;

	public Header() {

	}

	public Header getOuter() {
		return outer;
	}

	public void setOuter(Header outer) {
		this.outer = outer;
	}

	public Object clone() {

		Object object = null;
		try {
			object = super.clone();
		} catch (CloneNotSupportedException e) {
			e.printStackTrace();
		}
		if (outer != null)
			((Header) object).setOuter((Header) outer.clone());

		return object;
	}

	protected void readOuter(DataInputStream in) throws IOException,
			IllegalAccessException, InstantiationException {

		short magic;
		if (EOH != (magic = in.readShort())) {
			outer = (Header) ClassConfigurator.get(magic).newInstance();
			outer.readFrom(in);
		}
	}

	protected void writeOuter(DataOutputStream out) throws IOException {
		externalWriteTo(out, outer);

	}

	/**
	 * Write from an initial Header, which trigger writing of the entire header chain
	 * @param out
	 * @param clazz
	 * @throws IOException
	 */
	public static void externalWriteTo(DataOutputStream out, Header header) throws IOException {

		if(header == null){
			out.writeShort(EOH);
			return;
		}
		out.writeShort(ClassConfigurator.getMagicNumber(header.getClass()));
		header.writeTo(out);
	}
	
	
 
	/**
	 * Read the entire header chain and return the instance of first header
	 * @param in
	 * @return
	 * @throws IOException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	public static Header externalReadFrom(DataInputStream in) throws IOException,
			IllegalAccessException, InstantiationException {

		short magic;
		Header h = null;

		if (EOH != (magic = in.readShort())) {
			h = (Header) ClassConfigurator.get(magic).newInstance();
			h.readFrom(in);
		}
		
		return h;
	}
}
