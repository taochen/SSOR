package org.ssor.protocol.tolerance;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.ssor.protocol.Header;
/**
 * Used during the last broadcast of tolerating protocol
 * @author Tao Chen
 *
 */
public class AgreementHeader extends Header {

	
	private FaultySequence[] faulty;
	private boolean isTriggerRetransmission;
	// For releasing region distribution synchrony
	private Integer viewId = -1;
	
	public AgreementHeader(FaultySequence[] faulty, boolean isTriggerRetransmission) {
		super();
		this.faulty = faulty;
		this.isTriggerRetransmission = isTriggerRetransmission;
	}
	
	
	public AgreementHeader(FaultySequence[] faulty, boolean isTriggerRetransmission, Integer viewId) {
		super();
		this.faulty = faulty;
		this.isTriggerRetransmission = isTriggerRetransmission;
		if(viewId != null)
		  this.viewId = viewId;
	}

	public Integer getViewId() {
		return viewId == -1 ? null : viewId;
	}


	public AgreementHeader() {
		super();
	}

	@Override
	public void readFrom(DataInputStream in) throws IOException,
			IllegalAccessException, InstantiationException {

		int length = in.readInt();
		faulty = new FaultySequence[length];
		for(int i = 0; i < length; i++){
			faulty[i] = new FaultySequence();
			faulty[i].readFrom(in);
		}
		isTriggerRetransmission = in.readBoolean();
		viewId = in.readInt();
		readOuter(in);

	}

	@Override
	public void writeTo(DataOutputStream out) throws IOException {
		
		int length = faulty.length;
		out.writeInt(length);
		for(int i = 0; i < length; i++)
			faulty[i].writeTo(out);
		out.writeBoolean(isTriggerRetransmission);
		out.writeInt(viewId);
		writeOuter(out);

	}

	public FaultySequence[] getFaulty() {
		return faulty;
	}

	public boolean isTriggerRetransmission() {
		return isTriggerRetransmission;
	}

}
