package swift.pubsub;

import static sys.utils.NotImplemented.NotImplemented;
import swift.api.CRDTIdentifier;
import sys.net.api.Envelope;
import sys.net.api.MessageHandler;
import sys.pubsub.PubSub.Subscriber;

public interface SwiftSubscriber extends Subscriber<CRDTIdentifier>, MessageHandler {

	default String id() {
		throw NotImplemented;
	}

	default void onNotification(SwiftNotification event) {
		throw NotImplemented;
	}

	default void onNotification(UpdateNotification update) {
		throw NotImplemented;
	}

	default void onReceive(Envelope src, SwiftNotification notification) {
		throw NotImplemented;
	}
}
