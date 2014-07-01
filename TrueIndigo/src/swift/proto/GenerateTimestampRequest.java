package swift.proto;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import sys.net.api.Envelope;
import sys.net.api.MessageHandler;

/**
 * Server request to generate a timestamp for a transaction.
 * 
 * @author nmp
 */
public class GenerateTimestampRequest extends ClientRequest {
	Timestamp cltTimestamp;
	CausalityClock dependencyClk;

	transient Envelope source;

	public GenerateTimestampRequest() {
	}

	public GenerateTimestampRequest(String clientId, Timestamp cltTimestamp, CausalityClock dependencyClk) {
		super(clientId);
		this.cltTimestamp = cltTimestamp;
		this.dependencyClk = dependencyClk.clone();
		this.dependencyClk.drop(clientId);
	}

	public void setSource(Envelope source) {
		this.source = source;
	}

	public Envelope getSource() {
		return source;
	}

	public Timestamp getCltTimestamp() {
		return cltTimestamp;
	}

	public CausalityClock getDependencyClk() {
		return dependencyClk;
	}

	@Override
	public void deliverTo(final Envelope sender, final MessageHandler handler) {
		((SequencerProtocol) handler).onReceive(sender, this);
	}
}
