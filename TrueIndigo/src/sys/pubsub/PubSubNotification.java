package sys.pubsub;

import static sys.utils.NotImplemented.NotImplemented;

import java.util.Set;

import sys.net.api.Message;
import sys.pubsub.PubSub.Notifyable;

abstract public class PubSubNotification<T> implements Message, Notifyable<T> {

	Notifyable<T> payload;

	protected PubSubNotification() {
	}

	public PubSubNotification(Notifyable<T> payload) {
		this.payload = payload;
	}

	public Notifyable<T> payload() {
		return payload;
	}

	public String toString() {
		return "" + payload;
	}

	@Override
	public Object src() {
		return payload.src();
	}

	@Override
	public T key() {
		return payload.key();
	}

	@Override
	public Set<T> keys() {
		return payload.keys();
	}

	@Override
	public void notifyTo(PubSub<T> pubsub) {
		throw NotImplemented;
	}
}
