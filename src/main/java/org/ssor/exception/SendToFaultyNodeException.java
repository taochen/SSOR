package org.ssor.exception;

import org.ssor.protocol.Message;

@SuppressWarnings("serial")
public class SendToFaultyNodeException extends RuntimeException {

	
	private Message message;
	public SendToFaultyNodeException(int id, Message message) {
		super("Intend to send message to faulty node " + id);
		// TODO Auto-generated constructor stub
	}
	public Message getFaultyMessage() {
		return message;
	}

	
}
