package swift.pubsub;

import static sys.Context.Networking;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

import swift.api.CRDTIdentifier;
import swift.clocks.CausalityClock;
import swift.dc.Server;
import sys.net.api.Endpoint;
import sys.net.api.Service;
import sys.pubsub.impl.AbstractPubSub;

public class DataServerPubSubService extends AbstractPubSub<CRDTIdentifier> {

	final Service stub;
	final Executor executor;
	final Server server;

	public DataServerPubSubService(Server server, Executor executor) {
		super(server.serverId);
		this.executor = executor;
		this.server = server;
		this.stub = Networking.stub();
	}

	@Override
	synchronized public void publish(final Notifyable<CRDTIdentifier> info) {
		CausalityClock vrs = server.clocks.clientClockCopy();
		final SwiftNotification evt = new SwiftNotification(server.serverId, vrs, info);

		subscribers(info.key()).forEach(i -> {
			i.onNotification(evt);
		});
	}

	public void subscribe(CRDTIdentifier key, Endpoint remote) {
		RemoteSwiftSubscriber rs = remoteSubscribers.get(remote), nrs;
		if (rs == null) {
			rs = remoteSubscribers.putIfAbsent(remote, nrs = new RemoteSwiftSubscriber("surrogate-" + remote, stub, remote));
			if (rs == null)
				rs = nrs;
		}
		super.subscribe(key, rs);
	}

	// There should be just one subscriber of this kind - the surrogatePubSub.
	synchronized public void subscribe(String clientId, CRDTIdentifier key, Subscriber<CRDTIdentifier> subscriber) {
		if (suPubSubAdapter == null)
			suPubSubAdapter = new SurrogateSubscriberAdapter(clientId);

		super.subscribe(key, suPubSubAdapter);
	}

	synchronized public void unsubscribe(String clientId, CRDTIdentifier key, Subscriber<CRDTIdentifier> subscriber) {
		// not implemented...
	}

	synchronized public void unsubscribe(CRDTIdentifier key, Endpoint remote) {
		// not implemented...

		// RemoteSwiftSubscriber rs = remoteSubscribers.get(remote);
		// if (rs != null)
		// super.unsubscribe(key, rs);
	}

	class SurrogateSubscriberAdapter implements Subscriber<CRDTIdentifier> {

		final String clientId;
		final SwiftSubscriber handler;
		final AtomicLong fifoSeq = new AtomicLong(0L);

		public SurrogateSubscriberAdapter(String clientId) {
			this.clientId = clientId;
			this.handler = server.suPubSub.handler;
		}

		@Override
		public String id() {
			return clientId;
		}

		@Override
		public void onNotification(Notifyable<CRDTIdentifier> info) {
			handler.onReceive(null, ((SwiftNotification) info).clone(fifoSeq.incrementAndGet()));
		}
	}

	SurrogateSubscriberAdapter suPubSubAdapter;
	ConcurrentHashMap<Endpoint, RemoteSwiftSubscriber> remoteSubscribers = new ConcurrentHashMap<Endpoint, RemoteSwiftSubscriber>();
}
