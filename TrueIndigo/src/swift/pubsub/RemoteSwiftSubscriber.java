package swift.pubsub;

import java.util.concurrent.atomic.AtomicLong;

import swift.api.CRDTIdentifier;
import sys.net.api.Endpoint;
import sys.net.api.Service;
import sys.pubsub.PubSub.Notifyable;
import sys.pubsub.PubSub.Subscriber;
import sys.pubsub.RemoteSubscriber;

public class RemoteSwiftSubscriber extends RemoteSubscriber<CRDTIdentifier> implements SwiftSubscriber {
	AtomicLong fifoSeq = new AtomicLong(0L);

	public RemoteSwiftSubscriber(String clientId, Service stub) {
		super(clientId, stub, null);
	}

	public RemoteSwiftSubscriber(String clientId, Service stub, Endpoint remote) {
		super(clientId, stub, remote);
	}

	public void onNotification(Notifyable<CRDTIdentifier> info) {
		super.onNotification(((SwiftNotification) info).clone(fifoSeq.incrementAndGet()));
	}

	public int hashCode() {
		return id().hashCode();
	}

	@SuppressWarnings("unchecked")
	public boolean equals(Object o) {
		Subscriber<Endpoint> other = (Subscriber<Endpoint>) o;
		return id().equals(other.id());
	}

	@Override
	public void onNotification(SwiftNotification event) {
		super.onNotification(event.clone(fifoSeq.incrementAndGet()));
	}
}