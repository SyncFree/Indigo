package swift.pubsub;

import static sys.Context.Networking;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;

import swift.api.CRDTIdentifier;
import swift.proto.UnsubscribeUpdatesReply;
import swift.proto.UnsubscribeUpdatesRequest;
import sys.net.api.Endpoint;
import sys.net.api.Service;
import sys.pubsub.impl.AbstractPubSub;
import sys.utils.FifoQueue;
import sys.utils.Tasks;
abstract public class ScoutPubSubService extends AbstractPubSub<CRDTIdentifier> implements SwiftSubscriber {

	final Endpoint surrogate;
	final Service stub;

	final Set<CRDTIdentifier> unsubscriptions = Collections.synchronizedSet(new HashSet<CRDTIdentifier>());

	final UpdaterTask updater;
	final FifoQueue<SwiftNotification> fifoQueue;

	public ScoutPubSubService(final String clientId, final Endpoint surrogate) {
		super(clientId);

		// process incoming events observing source fifo order...
		this.fifoQueue = new FifoQueue<SwiftNotification>() {
			public void process(SwiftNotification event) {
				event.payload().notifyTo(ScoutPubSubService.this);
			}
		};

		this.surrogate = surrogate;
		this.stub = Networking.stub();

		this.stub.asyncRequest(surrogate, new PubSubHandshake(clientId), (SwiftNotification evt) -> {
			fifoQueue.offer(evt.seqN(), evt);
		}, true);

		updater = new UpdaterTask() {
			public void run() {
				updateSurrogatePubSub();
			}
		}.reSchedule(5.0);
	}

	private void updateSurrogatePubSub() {
		final Set<CRDTIdentifier> uset = new HashSet<CRDTIdentifier>(unsubscriptions);
		final UnsubscribeUpdatesRequest request = new UnsubscribeUpdatesRequest(-1L, super.id(), uset);
		this.stub.asyncRequest(surrogate, request, (UnsubscribeUpdatesReply r) -> {
			unsubscriptions.removeAll(uset);
		});
	}
	// TODO Q: no synchronization required for these two methods??
	public void subscribe(CRDTIdentifier key) {
		unsubscriptions.remove(key);
		super.subscribe(key, this);
	}

	public void unsubscribe(CRDTIdentifier key) {
		if (super.unsubscribe(key, this)) {
			unsubscriptions.add(key);
			if (!updater.isScheduled())
				updater.reSchedule(0.1);
		}
	}

	public SortedSet<CRDTIdentifier> keys() {
		return new TreeSet<CRDTIdentifier>(super.subscribers.keySet());
	}

	class UpdaterTask implements Runnable {
		final AtomicBoolean isScheduled = new AtomicBoolean(false);

		boolean isScheduled() {
			return isScheduled.get();
		}

		UpdaterTask reSchedule(double when) {
			if (!isScheduled.getAndSet(true)) {
				Tasks.exec(when, this);
			}
			return this;
		}

		public void run() {
			updateSurrogatePubSub();
			isScheduled.set(false);
		}
	}
}
