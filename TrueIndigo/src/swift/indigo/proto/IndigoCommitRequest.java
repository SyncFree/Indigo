package swift.indigo.proto;

import java.util.List;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.crdt.core.CRDTObjectUpdatesGroup;
import sys.net.api.Envelope;
import sys.net.api.MessageHandler;

public class IndigoCommitRequest extends swift.proto.CommitUpdatesRequest {

	long serial;

	public IndigoCommitRequest() {
		super();
	}

	public IndigoCommitRequest(long serial, String clientId, Timestamp cltTimestamp, CausalityClock dependencyClock, List<CRDTObjectUpdatesGroup<?>> objectUpdateGroups) {
		super(clientId, cltTimestamp, dependencyClock, objectUpdateGroups);
		this.serial = serial;
	}

	public long serial() {
		return serial;
	}

	@Override
	public void deliverTo(Envelope src, MessageHandler handler) {
		((IndigoProtocol) handler).onReceive(src, this);
	}
}
