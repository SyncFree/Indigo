package swift.indigo.proto;

import java.util.List;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.crdt.core.CRDTObjectUpdatesGroup;
import swift.indigo.ReservationsProtocolHandler;
import sys.net.api.Envelope;
import sys.net.api.MessageHandler;

public class IndigoCommitRequest extends swift.proto.CommitUpdatesRequest {

	long serial;
	boolean withLocks;

	public IndigoCommitRequest() {
		super();
	}

	public IndigoCommitRequest(long serial, String clientId, Timestamp cltTimestamp, CausalityClock dependencyClock,
			List<CRDTObjectUpdatesGroup<?>> objectUpdateGroups, boolean withLocks) {
		super(clientId, cltTimestamp, dependencyClock, objectUpdateGroups);
		this.serial = serial;
		this.withLocks = withLocks;
	}

	public long serial() {
		return serial;
	}

	@Override
	public void deliverTo(Envelope src, MessageHandler handler) {
		((ReservationsProtocolHandler) handler).onReceive(src, this);
	}

	public boolean withLocks() {
		return withLocks;
	}
}
