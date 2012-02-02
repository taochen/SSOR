package org.ssor.invocation;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Method;

import org.ssor.protocol.Header;

/**
 * This does not need to be transmitted
 * @author Tao Chen
 *
 */
@SuppressWarnings("serial")
public class InvocationHeader extends Header {

	private Method method;
	private Object instance;
	private Object[] arguments;
	public InvocationHeader(Method method, Object instance, Object[] arguments) {
		super();
		this.method = method;
		this.instance = instance;
		this.arguments = arguments;
	}
	
	public Object invoke(){
		
		try {
			return method.invoke(instance, arguments);
		
		} catch (Exception e) {
			e.getCause().printStackTrace();
		}
		
		return null;
	}

	@Override
	public void readFrom(DataInputStream in) throws IOException,
			IllegalAccessException, InstantiationException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void writeTo(DataOutputStream out) throws IOException {
		// TODO Auto-generated method stub
		
	}

}
