package swift.pubsub;

import swift.api.CRDTIdentifier;
import swift.clocks.CausalityClock;
import sys.net.api.Envelope;
import sys.net.api.MessageHandler;
import sys.pubsub.PubSub;
import sys.pubsub.PubSub.Notifyable;
import sys.pubsub.PubSubNotification;

public class SwiftNotification extends PubSubNotification<CRDTIdentifier> {

	long seqN;
	String src;
	CausalityClock dcVersion;

	SwiftNotification() {
	}

	public SwiftNotification(String src, CausalityClock dcVersion, Notifyable<CRDTIdentifier> payload) {
		super(payload);
		this.src = src;
		this.dcVersion = dcVersion;
	}

	public SwiftNotification(Notifyable<CRDTIdentifier> payload) {
		super(payload);
	}

	public SwiftNotification(long seqN, String src, CausalityClock dcVersion, Notifyable<CRDTIdentifier> payload) {
		super(payload);
		this.src = src;
		this.seqN = seqN;
		this.dcVersion = dcVersion;
	}

	SwiftNotification clone(long seqN) {
		return new SwiftNotification(seqN, src, dcVersion, payload());
	}

	public String src() {
		return src;
	}

	public CausalityClock dcVersion() {
		return dcVersion;
	}

	public long seqN() {
		return seqN;
	}

	@Override
	public void notifyTo(PubSub<CRDTIdentifier> pubsub) {
		if (payload().key() != null)
			pubsub.subscribers(payload().key()).forEach(i -> {
				try {
					((SwiftSubscriber) i).onNotification(this);
				} catch (Exception x) {
					x.printStackTrace();
				}
			});
		else
			pubsub.subscribers(payload().keys()).forEach(i -> {
				try {
					((SwiftSubscriber) i).onNotification(this);
				} catch (Exception x) {
					x.printStackTrace();
				}
			});

	}
	@Override
	public void deliverTo(Envelope src, MessageHandler handler) {
		((SwiftSubscriber) handler).onReceive(src, this);
	}
}
