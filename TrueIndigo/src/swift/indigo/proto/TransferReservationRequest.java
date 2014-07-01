package swift.indigo.proto;

import swift.api.CRDTIdentifier;
import sys.net.api.Envelope;
import sys.net.api.Message;
import sys.net.api.MessageHandler;

public class TransferReservationRequest implements Message {

	private String targetSite;
	private CRDTIdentifier counterId;

	public TransferReservationRequest() {
	}

	public TransferReservationRequest(String targetSite, CRDTIdentifier counterId) {
		this.targetSite = targetSite;
		this.counterId = counterId;
	}

	public String getTargetSite() {
		return targetSite;
	}

	public void setTargetSite(String targetSite) {
		this.targetSite = targetSite;
	}

	public CRDTIdentifier getCounterId() {
		return counterId;
	}

	public void setCounterId(CRDTIdentifier counterId) {
		this.counterId = counterId;
	}

	@Override
	public void deliverTo(Envelope src, MessageHandler handler) {
		((IndigoProtocol) handler).onReceive(src, this);
	}
}