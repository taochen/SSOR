package org.ssor.util;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.ssor.conf.ClassConfigurator;

/**
 * This is the utility class that enable object serialization/deserialization
 * into/from stream as well as class loading functionality.
 * 
 * @author Tao Chen extends from JGroup
 * 
 */
public abstract class Util {

	private Random random = new Random();

	private static Map<Class<?>, Byte> PRIMITIVE_TYPES = new HashMap<Class<?>, Byte>(
			15);
	private static final byte TYPE_NULL = 0;
	private static final byte TYPE_STREAMABLE = 1;
	private static final byte TYPE_SERIALIZABLE = 2;

	private static final byte TYPE_BOOLEAN = 10;
	private static final byte TYPE_BYTE = 11;
	private static final byte TYPE_CHAR = 12;
	private static final byte TYPE_DOUBLE = 13;
	private static final byte TYPE_FLOAT = 14;
	private static final byte TYPE_INT = 15;
	private static final byte TYPE_LONG = 16;
	private static final byte TYPE_SHORT = 17;
	private static final byte TYPE_STRING = 18;
	private static final byte TYPE_BYTEARRAY = 19;

	// Used for read/write of arbitrary objects (only can be used for
	// 1-demenssional array)
	private static final short OBJECT = 2;
	private static final short OBJECT_ARRAY = 3;
	static {

		PRIMITIVE_TYPES.put(Boolean.class, new Byte(TYPE_BOOLEAN));
		PRIMITIVE_TYPES.put(Byte.class, new Byte(TYPE_BYTE));
		PRIMITIVE_TYPES.put(Character.class, new Byte(TYPE_CHAR));
		PRIMITIVE_TYPES.put(Double.class, new Byte(TYPE_DOUBLE));
		PRIMITIVE_TYPES.put(Float.class, new Byte(TYPE_FLOAT));
		PRIMITIVE_TYPES.put(Integer.class, new Byte(TYPE_INT));
		PRIMITIVE_TYPES.put(Long.class, new Byte(TYPE_LONG));
		PRIMITIVE_TYPES.put(Short.class, new Byte(TYPE_SHORT));
		PRIMITIVE_TYPES.put(String.class, new Byte(TYPE_STRING));
		PRIMITIVE_TYPES.put(byte[].class, new Byte(TYPE_BYTEARRAY));

	}

	public abstract int getUUIDFromAddress(Object object);

	public short getRandomShort() {
		return (short) random.nextInt(Short.MAX_VALUE);
	}

	/**
	 * Tries to load the class from the current thread's context class loader.
	 * If not successful, tries to load the class from the current instance.
	 * 
	 * 
	 * @param classname
	 *            Desired class.
	 * @param clazz
	 *            Class object used to obtain a class loader if no context class
	 *            loader is available.
	 * @return Class, or null on failure.
	 */
	public static Class<?> loadClass(String classname, Class<?> clazz)
			throws ClassNotFoundException {
		ClassLoader loader;

		try {
			loader = Thread.currentThread().getContextClassLoader();
			if (loader != null) {
				return loader.loadClass(classname);
			}
		} catch (Throwable t) {
		}

		if (clazz != null) {
			try {
				loader = clazz.getClassLoader();
				if (loader != null) {
					return loader.loadClass(classname);
				}
			} catch (Throwable t) {
			}
		}

		try {
			loader = ClassLoader.getSystemClassLoader();
			if (loader != null) {
				return loader.loadClass(classname);
			}
		} catch (Throwable t) {
		}

		throw new ClassNotFoundException(classname);
	}

	/**
	 * Load the class from classpath, and return the corresponding stream.
	 * 
	 * @param name
	 *            class name
	 * @param clazz
	 *            class of the target casspath
	 * @return the input stream
	 */
	public static InputStream getResourceAsStream(String name, Class<?> clazz) {
		ClassLoader loader;
		InputStream retval = null;

		try {
			loader = Thread.currentThread().getContextClassLoader();
			if (loader != null) {
				retval = loader.getResourceAsStream(name);
				if (retval != null)
					return retval;
			}
		} catch (Throwable t) {
		}

		if (clazz != null) {
			try {
				loader = clazz.getClassLoader();
				if (loader != null) {
					retval = loader.getResourceAsStream(name);
					if (retval != null)
						return retval;
				}
			} catch (Throwable t) {
			}
		}

		try {
			loader = ClassLoader.getSystemClassLoader();
			if (loader != null) {
				return loader.getResourceAsStream(name);
			}
		} catch (Throwable t) {
		}

		return retval;
	}

	/**
	 * Write string into output stream.
	 * 
	 * @param s
	 *            the string
	 * @param out
	 *            the output stream
	 * @throws IOException
	 */
	public static void writeString(String s, DataOutputStream out)
			throws IOException {
		if (s != null) {
			out.write(1);
			out.writeUTF(s);
		} else {
			out.write(0);
		}
	}

	/**
	 * Read string from input stream.
	 * 
	 * @param in
	 *            the input stream
	 * @return the string
	 * @throws IOException
	 */
	public static String readString(DataInputStream in) throws IOException {
		int b = in.read();
		if (b == 1)
			return in.readUTF();
		return null;
	}

	/**
	 * Convert an object to bytes, by way of either use traditional Serializable
	 * interface or use the light-weight and efficient Streamable interface if
	 * it is possible.
	 * 
	 * @param obj
	 *            the object
	 * @return the byte that needs to be transmitted
	 * @throws Exception
	 */
	public static byte[] objectToByteBuffer(Object obj) throws Exception {
		if (obj == null)
			return ByteBuffer.allocate(Global.BYTE_SIZE).put(TYPE_NULL).array();

		if (obj instanceof Streamable) {
			final ExposedByteArrayOutputStream out_stream = new ExposedByteArrayOutputStream(
					128);
			final ExposedDataOutputStream out = new ExposedDataOutputStream(
					out_stream);
			out_stream.write(TYPE_STREAMABLE);
			writeGenericStreamable((Streamable) obj, out);
			// Close the stream
			Util.close(out);
			return out_stream.toByteArray();
		}

		Byte type = PRIMITIVE_TYPES.get(obj.getClass());
		if (type == null) { // will throw an exception if object is not
			// serializable
			final ExposedByteArrayOutputStream out_stream = new ExposedByteArrayOutputStream(
					128);
			out_stream.write(TYPE_SERIALIZABLE);
			ObjectOutputStream out = new ObjectOutputStream(out_stream);
			out.writeObject(obj);
			out.close();
			return out_stream.toByteArray();
		}

		switch (type.byteValue()) {
		case TYPE_BOOLEAN:
			return ByteBuffer.allocate(Global.BYTE_SIZE * 2).put(TYPE_BOOLEAN)
					.put(((Boolean) obj).booleanValue() ? (byte) 1 : (byte) 0)
					.array();
		case TYPE_BYTE:
			return ByteBuffer.allocate(Global.BYTE_SIZE * 2).put(TYPE_BYTE)
					.put(((Byte) obj).byteValue()).array();
		case TYPE_CHAR:
			return ByteBuffer.allocate(Global.BYTE_SIZE * 3).put(TYPE_CHAR)
					.putChar(((Character) obj).charValue()).array();
		case TYPE_DOUBLE:
			return ByteBuffer.allocate(Global.BYTE_SIZE + Global.DOUBLE_SIZE)
					.put(TYPE_DOUBLE).putDouble(((Double) obj).doubleValue())
					.array();
		case TYPE_FLOAT:
			return ByteBuffer.allocate(Global.BYTE_SIZE + Global.FLOAT_SIZE)
					.put(TYPE_FLOAT).putFloat(((Float) obj).floatValue())
					.array();
		case TYPE_INT:
			return ByteBuffer.allocate(Global.BYTE_SIZE + Global.INT_SIZE).put(
					TYPE_INT).putInt(((Integer) obj).intValue()).array();
		case TYPE_LONG:
			return ByteBuffer.allocate(Global.BYTE_SIZE + Global.LONG_SIZE)
					.put(TYPE_LONG).putLong(((Long) obj).longValue()).array();
		case TYPE_SHORT:
			return ByteBuffer.allocate(Global.BYTE_SIZE + Global.SHORT_SIZE)
					.put(TYPE_SHORT).putShort(((Short) obj).shortValue())
					.array();
		case TYPE_STRING:
			String str = (String) obj;
			byte[] buf = new byte[str.length()];
			for (int i = 0; i < buf.length; i++)
				buf[i] = (byte) str.charAt(i);
			return ByteBuffer.allocate(Global.BYTE_SIZE + buf.length).put(
					TYPE_STRING).put(buf, 0, buf.length).array();
		case TYPE_BYTEARRAY:
			buf = (byte[]) obj;
			return ByteBuffer.allocate(Global.BYTE_SIZE + buf.length).put(
					TYPE_BYTEARRAY).put(buf, 0, buf.length).array();
		default:
			throw new IllegalArgumentException("type " + type + " is invalid");
		}

	}

	/**
	 * Convert bytes to an object, by way of either use traditional Serializable
	 * interface or use the light-weight and efficient Streamable interface if
	 * it is possible.
	 * 
	 * @param buffer
	 *            the bytes
	 * @return the object
	 * @throws Exception
	 */
	public static Object objectFromByteBuffer(byte[] buffer) throws Exception {
		if (buffer == null)
			return null;
		return objectFromByteBuffer(buffer, 0, buffer.length);
	}

	/**
	 * 
	 * Convert bytes to an object, by way of either use traditional Serializable
	 * interface or use the light-weight and efficient Streamable interface if
	 * it is possible.
	 * 
	 * @param buffer
	 *            the byte that has been received
	 * @param offset
	 *            the start index
	 * @param length
	 *            the length
	 * @return the object
	 * @throws Exception
	 */

	public static Object objectFromByteBuffer(byte[] buffer, int offset,
			int length) throws Exception {
		if (buffer == null)
			return null;
		Object retval = null;
		byte type = buffer[offset];

		switch (type) {
		case TYPE_NULL:
			return null;
		case TYPE_STREAMABLE:
			ByteArrayInputStream in_stream = new ExposedByteArrayInputStream(
					buffer, offset + 1, length - 1);
			InputStream in = new DataInputStream(in_stream);
			retval = readGenericStreamable((DataInputStream) in);
			// Close the stream
			Util.close(in);
			break;
		case TYPE_SERIALIZABLE: // the object is Externalizable or Serializable
			in_stream = new ExposedByteArrayInputStream(buffer, offset + 1,
					length - 1);
			in = new ObjectInputStream(in_stream); // changed Nov 29 2004
			// (bela)
			try {
				retval = ((ObjectInputStream) in).readObject();
			} finally {
				Util.close(in);
			}
			break;
		case TYPE_BOOLEAN:
			return ByteBuffer.wrap(buffer, offset + 1, length - 1).get() == 1;
		case TYPE_BYTE:
			return ByteBuffer.wrap(buffer, offset + 1, length - 1).get();
		case TYPE_CHAR:
			return ByteBuffer.wrap(buffer, offset + 1, length - 1).getChar();
		case TYPE_DOUBLE:
			return ByteBuffer.wrap(buffer, offset + 1, length - 1).getDouble();
		case TYPE_FLOAT:
			return ByteBuffer.wrap(buffer, offset + 1, length - 1).getFloat();
		case TYPE_INT:
			return ByteBuffer.wrap(buffer, offset + 1, length - 1).getInt();
		case TYPE_LONG:
			return ByteBuffer.wrap(buffer, offset + 1, length - 1).getLong();
		case TYPE_SHORT:
			return ByteBuffer.wrap(buffer, offset + 1, length - 1).getShort();
		case TYPE_STRING:
			byte[] tmp = new byte[length - 1];
			System.arraycopy(buffer, offset + 1, tmp, 0, length - 1);
			return new String(tmp);
		case TYPE_BYTEARRAY:
			tmp = new byte[length - 1];
			System.arraycopy(buffer, offset + 1, tmp, 0, length - 1);
			return tmp;
		default:
			throw new IllegalArgumentException("type " + type + " is invalid");
		}
		return retval;
	}

	/**
	 * Write the magic number that represent the class signature, if available.
	 * 
	 * @param obj
	 *            the object
	 * @param out
	 *            the output stream
	 * @throws IOException
	 */
	public static void writeGenericStreamable(Streamable obj,
			DataOutputStream out) throws IOException {
		short magic_number;
		String classname;

		if (obj == null) {
			out.write(0);
			return;
		}

		out.write(1);
		magic_number = ClassConfigurator.getMagicNumber(obj.getClass());
		// write the magic number or the class name
		if (magic_number == -1) {
			out.writeBoolean(false);
			classname = obj.getClass().getName();
			out.writeUTF(classname);
		} else {
			out.writeBoolean(true);
			out.writeShort(magic_number);
		}

		// write the contents
		obj.writeTo(out);
	}

	/**
	 * Read the object from the magic number that represent the class signature,
	 * if available.
	 * 
	 * @param in
	 *            input stream
	 * @return the object
	 * @throws IOException
	 */
	public static Streamable readGenericStreamable(DataInputStream in)
			throws IOException {
		Streamable retval = null;
		int b = in.read();
		// 0 means the object is null
		if (b == 0)
			return null;

		boolean use_magic_number = in.readBoolean();
		String classname;
		Class<?> clazz;

		try {
			if (use_magic_number) {
				short magic_number = in.readShort();

				clazz = ClassConfigurator.get(magic_number);
				if (clazz == null) {
					throw new ClassNotFoundException("Class for magic number "
							+ magic_number + " cannot be found.");
				}
			} else {
				classname = in.readUTF();
				clazz = ClassConfigurator.get(classname);
				if (clazz == null) {
					throw new ClassNotFoundException(classname);
				}
			}

			retval = (Streamable) clazz.newInstance();
			retval.readFrom(in);
			return retval;
		} catch (Exception ex) {
			throw new IOException("failed reading object: " + ex.toString());
		}
	}

	/**
	 * Write arbitrary objects into stream, this does not support
	 * muti-demensional array.
	 * 
	 * @param out
	 *            output stream
	 * @param object
	 *            the object
	 * @throws Exception
	 */
	public static void writeArbitraryObject(DataOutputStream out, Object object)
			throws Exception {

		if (object == null) {
			out.write(0);
			return;
		}

		out.write(1);
		if (object instanceof Object[]) {
			out.writeShort(OBJECT_ARRAY);

			// Set name of the array or indicator
			String name = object.getClass().getName().replaceAll("\\[+L?", "")
					.replace(";", "");
			short m = ClassConfigurator.getMagicNumber(name);

			out.writeShort(m);
			if (m == -1)
				writeString(name, out);

			Object[] objects = (Object[]) object;
			int length = objects.length;
			out.writeInt(length);
			for (int i = 0; i < length; i++) {
				writeArbitraryObject(out, objects[i]);
			}

		} else {

			out.writeShort(OBJECT);
			byte[] b = Util.objectToByteBuffer(object);
			out.writeInt(b.length);
			out.write(b, 0, b.length);
		}
	}

	/**
	 * Read arbitrary objects from stream, this does not support
	 * muti-demensional array.
	 * 
	 * @param in
	 *            the input stream
	 * @return the object
	 * @throws Exception
	 */
	public static Object readArbitraryObject(DataInputStream in)
			throws Exception {

		if (in.read() == 0)
			return null;
		short mark = in.readShort();

		if (OBJECT_ARRAY == mark) {

			Object[] objects = null;

			short m = in.readShort();

			objects = (m == -1) ? (Object[]) Array.newInstance(Class
					.forName(readString(in)), in.readInt()) : (Object[]) Array
					.newInstance(ClassConfigurator.getArray(m), in.readInt());

			int length = objects.length;
			for (int i = 0; i < length; i++) {
				objects[i] = readArbitraryObject(in);
			}

			return objects;

		} else {

			int len = in.readInt();
			byte[] buffer = new byte[len];
			in.readFully(buffer, 0, len);

			return Util.objectFromByteBuffer(buffer);
		}
	}

	public static void close(InputStream inp) {
		if (inp != null)
			try {
				inp.close();
			} catch (IOException e) {
			}
	}

	public static void close(OutputStream out) {
		if (out != null) {
			try {
				out.close();
			} catch (IOException e) {
			}
		}
	}

}
