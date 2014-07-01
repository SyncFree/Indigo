package sys.pubsub;

import sys.net.api.Endpoint;
import sys.net.api.Service;
import sys.pubsub.PubSub.Notifyable;
import sys.pubsub.PubSub.Subscriber;

abstract public class RemoteSubscriber<T> implements Subscriber<T> {

	final String id;
	final Service stub;

	protected Endpoint remote;

	public RemoteSubscriber(String id, Service stub, Endpoint remote) {
		this.id = id;
		this.stub = stub;
		this.remote = remote;
	}

	@Override
	public String id() {
		return id;
	}

	protected void setRemote(Endpoint remote) {
		this.remote = remote;
	}

	@Override
	public void onNotification(Notifyable<T> info) {
		try {
			if (remote != null) {
				stub.send(remote, (PubSubNotification<?>) info);
			}
		} catch (Exception x) {
			x.printStackTrace();
		}
	}

	public Endpoint remoteEndpoint() {
		return remote;
	}

	public int hashCode() {
		return id.hashCode();
	}

	public boolean equals(Object other) {
		return other instanceof RemoteSubscriber && id.equals(((RemoteSubscriber<?>) other).id);
	}

}
