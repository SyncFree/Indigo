package swift.proto;

import sys.net.api.Envelope;
import sys.net.api.MessageHandler;

public class RemoteCommitUpdatesRequest extends CommitUpdatesRequest {

	public RemoteCommitUpdatesRequest() {
	}

	public RemoteCommitUpdatesRequest(CommitUpdatesRequest req) {
		super(req.clientId, req.cltTimestamp, req.dependencyClock, req.objectUpdateGroups);
		super.setTimestamp(req.timestamp);
		super.kStability = -1;
	}

	@Override
	public void deliverTo(Envelope src, MessageHandler handler) {
		((SurrogateProtocol) handler).onReceive(src, this);
	}
}
