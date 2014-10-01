package swift.proto;

import sys.net.api.Envelope;
import sys.net.api.MessageHandler;

/**
 * Client request to get the latest known committed versions at the server.
 * 
 * 
 * @author mzawirski
 */
public class CurrentClockRequest extends ClientRequest {

	public CurrentClockRequest() {
	}

	public CurrentClockRequest(String clientId) {
		super(clientId);
	}

	@Override
	public void deliverTo(Envelope src, MessageHandler handler) {
		if (handler instanceof SequencerProtocol)
			((SequencerProtocol) handler).onReceive(src, this);
		else
			((SurrogateProtocol) handler).onReceive(src, this);

	}
}
