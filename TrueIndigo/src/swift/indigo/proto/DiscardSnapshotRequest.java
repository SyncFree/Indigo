package swift.indigo.proto;

import swift.clocks.Timestamp;
import swift.proto.ClientRequest;
import sys.net.api.Envelope;
import sys.net.api.MessageHandler;

/**
 * Request to release the locks associated with a timestamp...
 * 
 * @author smduarte
 */
public class DiscardSnapshotRequest extends ClientRequest {

	protected long serial;
	protected Timestamp cltTimestamp;

	/**
	 * Fake constructor for Kryo serialization.
	 */
	public DiscardSnapshotRequest() {
	}

	public DiscardSnapshotRequest(long serial, String clientId, Timestamp cltTimestamp) {
		super(clientId);
		this.serial = serial;
		this.cltTimestamp = cltTimestamp;
	}

	public long serial() {
		return serial;
	}

	public Timestamp cltTimestamp() {
		return cltTimestamp;
	}

	public String requesterId() {
		return super.getClientId();
	}

	@Override
	public void deliverTo(Envelope src, MessageHandler handler) {
		((IndigoProtocolHandler) handler).onReceive(src, this);
	}

	// Below is meant only for ordering remote pending requests...

	public String toString() {
		return String.format("%s:%s)", requesterId(), cltTimestamp);
	}
}
